//! Staggered TCP connection scheduling with bounded in-flight attempts.

use std::{collections::VecDeque, future::Future, net::SocketAddr, time::Duration};

use futures_util::{StreamExt, stream::FuturesUnordered};
use tokio::time::{Instant, sleep_until};

/// Delay before starting another address while earlier attempts are pending.
const ATTEMPT_DELAY: Duration = Duration::from_millis(250);
/// Maximum number of sockets being established concurrently.
const MAX_IN_FLIGHT: usize = 4;

/// Interleaves address families without changing the resolver's first preference.
fn interleave(addresses: &[SocketAddr]) -> VecDeque<SocketAddr> {
    let Some(first) = addresses.first() else {
        return VecDeque::new();
    };
    let (mut preferred, mut alternate): (VecDeque<_>, VecDeque<_>) = addresses
        .iter()
        .copied()
        .partition(|address| address.is_ipv6() == first.is_ipv6());
    let mut ordered = VecDeque::with_capacity(addresses.len());
    while !preferred.is_empty() || !alternate.is_empty() {
        ordered.extend(preferred.pop_front());
        ordered.extend(alternate.pop_front());
    }
    ordered
}

/// Tries addresses with staggered starts, returning the first successful socket.
///
/// Each supplied attempt must have a finite deadline so a full set of stalled
/// attempts cannot starve later addresses. Completed addresses are removed from
/// the input; untried and canceled addresses remain available for protocol-level
/// retries. Dropping this future drops every pending dial; no tasks are spawned.
pub(crate) async fn connect<T, E, F>(
    addresses: &mut Vec<SocketAddr>,
    mut dial: impl FnMut(SocketAddr) -> F,
) -> Option<Result<T, E>>
where
    F: Future<Output = Result<T, E>>,
{
    let mut queued = interleave(addresses);
    let mut pending = FuturesUnordered::new();
    let mut next_start = Instant::now();
    let mut last_error = None;

    loop {
        if pending.is_empty() && queued.is_empty() {
            return last_error.map(Err);
        }

        if !queued.is_empty()
            && pending.len() < MAX_IN_FLIGHT
            && (pending.is_empty() || Instant::now() >= next_start)
        {
            let address = queued.pop_front().unwrap();
            let attempt = dial(address);
            pending.push(async move { (address, attempt.await) });
            next_start = Instant::now() + ATTEMPT_DELAY;
        }

        tokio::select! {
            biased;

            Some((address, result)) = pending.next(), if !pending.is_empty() => {
                addresses.retain(|candidate| *candidate != address);
                match result {
                    Ok(socket) => return Some(Ok(socket)),
                    Err(error) => {
                        last_error = Some(error);
                        // A known failure need not consume another stagger interval.
                        next_start = Instant::now();
                    }
                }
            }

            _ = sleep_until(next_start), if !queued.is_empty() && pending.len() < MAX_IN_FLIGHT => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::pending,
        net::{Ipv4Addr, SocketAddr},
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use futures_util::poll;
    use tokio::{
        sync::oneshot,
        time::{Instant, advance, sleep},
    };

    use crate::staggered::{ATTEMPT_DELAY, MAX_IN_FLIGHT, connect, interleave};

    /// Counts owned attempts or successful sockets until they are dropped.
    struct Owned(Arc<AtomicUsize>);

    impl Owned {
        /// Registers ownership at construction.
        fn new(count: &Arc<AtomicUsize>) -> Self {
            count.fetch_add(1, Ordering::SeqCst);
            Self(Arc::clone(count))
        }
    }

    impl Drop for Owned {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Builds distinct local test addresses without opening sockets.
    fn addresses(count: u16) -> Vec<SocketAddr> {
        (1..=count)
            .map(|port| SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
            .collect()
    }

    #[test]
    fn interleaves_families_preserving_preference_and_order() {
        for input in [
            vec!["[::1]:1", "[::1]:2", "127.0.0.1:3", "127.0.0.1:4"],
            vec!["127.0.0.1:1", "127.0.0.1:2", "[::1]:3", "[::1]:4"],
        ] {
            let input: Vec<SocketAddr> = input.into_iter().map(|s| s.parse().unwrap()).collect();
            assert_eq!(
                interleave(&input).into_iter().collect::<Vec<_>>(),
                vec![input[0], input[2], input[1], input[3]]
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn slow_first_attempt_can_win_and_cancels_loser() {
        let active = Arc::new(AtomicUsize::new(0));
        let starts = Arc::new(Mutex::new(Vec::new()));
        let start = Instant::now();
        let mut addresses = addresses(2);
        let result = connect(&mut addresses, |address| {
            let active = Arc::clone(&active);
            let starts = Arc::clone(&starts);
            async move {
                let _owned = Owned::new(&active);
                starts
                    .lock()
                    .unwrap()
                    .push((address.port(), start.elapsed()));
                if address.port() == 1 {
                    sleep(Duration::from_millis(350)).await;
                    Ok::<_, ()>(address)
                } else {
                    pending().await
                }
            }
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result.port(), 1);
        assert_eq!(
            *starts.lock().unwrap(),
            vec![(1, Duration::ZERO), (2, ATTEMPT_DELAY),]
        );
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert_eq!(addresses, self::addresses(2)[1..]);
    }

    #[tokio::test(start_paused = true)]
    async fn immediate_rejection_advances_without_delay() {
        let start = Instant::now();
        let mut addresses = addresses(2);
        let result = connect(&mut addresses, |address| async move {
            if address.port() == 1 {
                Err(1)
            } else {
                Ok(address)
            }
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result.port(), 2);
        assert_eq!(start.elapsed(), Duration::ZERO);
        assert!(addresses.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn bounded_attempts_do_not_starve_later_addresses() {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut addresses = addresses(6);
        let result = connect(&mut addresses, |address| {
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            async move {
                let _owned = Owned::new(&active);
                peak.fetch_max(active.load(Ordering::SeqCst), Ordering::SeqCst);
                if address.port() < 6 {
                    sleep(Duration::from_secs(10)).await;
                    Err(address)
                } else {
                    Ok(address)
                }
            }
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result.port(), 6);
        assert_eq!(peak.load(Ordering::SeqCst), MAX_IN_FLIGHT);
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_drops_every_pending_attempt() {
        let active = Arc::new(AtomicUsize::new(0));
        let mut addresses = addresses(10);
        let mut connection = Box::pin(connect(&mut addresses, |_| {
            let active = Arc::clone(&active);
            async move {
                let _owned = Owned::new(&active);
                pending::<Result<(), ()>>().await
            }
        }));
        for _ in 0..MAX_IN_FLIGHT {
            assert!(poll!(connection.as_mut()).is_pending());
            advance(ATTEMPT_DELAY).await;
        }
        assert!(poll!(connection.as_mut()).is_pending());
        assert_eq!(active.load(Ordering::SeqCst), MAX_IN_FLIGHT);
        drop(connection);
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn simultaneous_success_drops_unselected_socket() {
        let active = Arc::new(AtomicUsize::new(0));
        let (send_first, first) = oneshot::channel();
        let (send_second, second) = oneshot::channel();
        let mut receivers = vec![first, second].into_iter();
        let mut addresses = addresses(2);
        let mut connection = Box::pin(connect(&mut addresses, |_| {
            let receiver = receivers.next().unwrap();
            async move { Ok::<_, ()>(receiver.await.unwrap()) }
        }));
        assert!(poll!(connection.as_mut()).is_pending());
        advance(ATTEMPT_DELAY).await;
        assert!(poll!(connection.as_mut()).is_pending());
        assert!(send_first.send(Owned::new(&active)).is_ok());
        assert!(send_second.send(Owned::new(&active)).is_ok());
        let winner = connection.await.unwrap().unwrap();
        assert_eq!(active.load(Ordering::SeqCst), 1);
        drop(winner);
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn exhausted_and_empty_lists_terminate() {
        let mut addresses = addresses(3);
        assert_eq!(
            connect(&mut addresses, |address| async move {
                Err::<(), _>(address.port())
            })
            .await,
            Some(Err(3))
        );
        assert!(addresses.is_empty());
        assert_eq!(
            connect(&mut addresses, |_| pending::<Result<(), ()>>()).await,
            None
        );
    }
}
