use crate::client::{Addr, SocketConfig};
use crate::config::{Host, LoadBalanceHosts, TargetSessionAttrs};
use crate::connect_raw::connect_raw;
use crate::connect_socket::connect_socket;
use crate::tls::MakeTlsConnect;
use crate::{Client, Config, Connection, Error, SimpleQueryMessage, Socket};
use futures_util::{FutureExt, Stream};
use rand::seq::SliceRandom;
use std::future::{self, Future};
use std::net::SocketAddr;
use std::pin::pin;
use std::task::Poll;
use std::time::Duration;
use std::{cmp, io};
use tokio::net;

pub async fn connect<T>(
    mut tls: T,
    config: &Config,
) -> Result<(Client, Connection<Socket, T::Stream>), Error>
where
    T: MakeTlsConnect<Socket>,
{
    if config.host.is_empty() && config.hostaddr.is_empty() {
        return Err(Error::config("both host and hostaddr are missing".into()));
    }

    if !config.host.is_empty()
        && !config.hostaddr.is_empty()
        && config.host.len() != config.hostaddr.len()
    {
        let msg = format!(
            "number of hosts ({}) is different from number of hostaddrs ({})",
            config.host.len(),
            config.hostaddr.len(),
        );
        return Err(Error::config(msg.into()));
    }

    // At this point, either one of the following two scenarios could happen:
    // (1) either config.host or config.hostaddr must be empty;
    // (2) if both config.host and config.hostaddr are NOT empty; their lengths must be equal.
    let num_hosts = cmp::max(config.host.len(), config.hostaddr.len());

    if config.port.len() > 1 && config.port.len() != num_hosts {
        return Err(Error::config("invalid number of ports".into()));
    }

    let mut indices = (0..num_hosts).collect::<Vec<_>>();
    if config.load_balance_hosts == LoadBalanceHosts::Random {
        indices.shuffle(&mut rand::rng());
    }

    let mut error = None;
    for i in indices {
        let host = config.host.get(i);
        let hostaddr = config.hostaddr.get(i);
        let port = config
            .port
            .get(i)
            .or_else(|| config.port.first())
            .copied()
            .unwrap_or(5432);

        // The value of host is used as the hostname for TLS validation,
        let hostname = match host {
            Some(Host::Tcp(host)) => Some(host.clone()),
            // postgres doesn't support TLS over unix sockets, so the choice here doesn't matter
            #[cfg(unix)]
            Some(Host::Unix(_)) => None,
            None => None,
        };

        // Try to use the value of hostaddr to establish the TCP connection,
        // fallback to host if hostaddr is not present.
        let addr = match hostaddr {
            Some(ipaddr) => Host::Tcp(ipaddr.to_string()),
            None => host.cloned().unwrap(),
        };

        match connect_host(addr, hostname, port, &mut tls, config).await {
            Ok((client, connection)) => return Ok((client, connection)),
            Err(e) => error = Some(e),
        }
    }

    Err(error.unwrap())
}

async fn connect_host<T>(
    host: Host,
    hostname: Option<String>,
    port: u16,
    tls: &mut T,
    config: &Config,
) -> Result<(Client, Connection<Socket, T::Stream>), Error>
where
    T: MakeTlsConnect<Socket>,
{
    match host {
        Host::Tcp(host) => {
            let mut addrs = net::lookup_host((&*host, port))
                .await
                .map_err(Error::connect)?
                .collect::<Vec<_>>();

            if config.load_balance_hosts == LoadBalanceHosts::Random {
                addrs.shuffle(&mut rand::rng());
            }

            connect_tcp(addrs, hostname.as_deref(), tls, config).await
        }
        #[cfg(unix)]
        Host::Unix(path) => {
            connect_once(Addr::Unix(path), hostname.as_deref(), port, tls, config).await
        }
    }
}

/// Connects to resolved addresses, retrying after TLS or PostgreSQL startup fails.
async fn connect_tcp<T>(
    mut addrs: Vec<SocketAddr>,
    hostname: Option<&str>,
    tls: &mut T,
    config: &Config,
) -> Result<(Client, Connection<Socket, T::Stream>), Error>
where
    T: MakeTlsConnect<Socket>,
{
    let mut last_err = None;
    while let Some(result) = crate::staggered::connect(&mut addrs, |addr| async move {
        let socket = connect_socket(
            &Addr::Tcp(addr.ip()),
            addr.port(),
            Some(config.connect_timeout.unwrap_or(Duration::from_secs(10))),
            config.tcp_user_timeout,
            config.keepalives.then_some(&config.keepalive_config),
        )
        .await?;
        Ok((addr, socket))
    })
    .await
    {
        let (addr, socket) = match result {
            Ok(connected) => connected,
            Err(error) => {
                last_err = Some(error);
                break;
            }
        };
        match connect_on_socket(
            socket,
            Addr::Tcp(addr.ip()),
            hostname,
            addr.port(),
            tls,
            config,
        )
        .await
        {
            Ok(stream) => return Ok(stream),
            Err(e) => {
                last_err = Some(e);
                continue;
            }
        };
    }

    Err(last_err.unwrap_or_else(|| {
        Error::connect(io::Error::new(
            io::ErrorKind::InvalidInput,
            "could not resolve any addresses",
        ))
    }))
}

#[cfg(unix)]
async fn connect_once<T>(
    addr: Addr,
    hostname: Option<&str>,
    port: u16,
    tls: &mut T,
    config: &Config,
) -> Result<(Client, Connection<Socket, T::Stream>), Error>
where
    T: MakeTlsConnect<Socket>,
{
    let socket = connect_socket(
        &addr,
        port,
        config.connect_timeout,
        config.tcp_user_timeout,
        if config.keepalives {
            Some(&config.keepalive_config)
        } else {
            None
        },
    )
    .await?;

    connect_on_socket(socket, addr, hostname, port, tls, config).await
}

/// Completes TLS and PostgreSQL startup on the winning TCP or Unix socket.
async fn connect_on_socket<T>(
    socket: Socket,
    addr: Addr,
    hostname: Option<&str>,
    port: u16,
    tls: &mut T,
    config: &Config,
) -> Result<(Client, Connection<Socket, T::Stream>), Error>
where
    T: MakeTlsConnect<Socket>,
{
    let tls = tls
        .make_tls_connect(hostname.unwrap_or(""))
        .map_err(|e| Error::tls(e.into()))?;
    let has_hostname = hostname.is_some();
    let (mut client, mut connection) = connect_raw(socket, tls, has_hostname, config).await?;

    if config.target_session_attrs != TargetSessionAttrs::Any {
        let mut rows = pin!(client.simple_query_raw("SHOW transaction_read_only"));

        let mut rows = pin!(
            future::poll_fn(|cx| {
                if connection.poll_unpin(cx)?.is_ready() {
                    return Poll::Ready(Err(Error::closed()));
                }

                rows.as_mut().poll(cx)
            })
            .await?
        );

        loop {
            let next = future::poll_fn(|cx| {
                if connection.poll_unpin(cx)?.is_ready() {
                    return Poll::Ready(Some(Err(Error::closed())));
                }

                rows.as_mut().poll_next(cx)
            });

            match next.await.transpose()? {
                Some(SimpleQueryMessage::Row(row)) => {
                    let read_only_result = row.try_get(0)?;
                    if read_only_result == Some("on")
                        && config.target_session_attrs == TargetSessionAttrs::ReadWrite
                    {
                        return Err(Error::connect(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "database does not allow writes",
                        )));
                    } else if read_only_result == Some("off")
                        && config.target_session_attrs == TargetSessionAttrs::ReadOnly
                    {
                        return Err(Error::connect(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "database is not read only",
                        )));
                    } else {
                        break;
                    }
                }
                Some(_) => {}
                None => return Err(Error::unexpected_message()),
            }
        }
    }

    client.set_socket_config(SocketConfig {
        addr,
        hostname: hostname.map(|s| s.to_string()),
        port,
        connect_timeout: config.connect_timeout,
        tcp_user_timeout: config.tcp_user_timeout,
        keepalive: if config.keepalives {
            Some(config.keepalive_config.clone())
        } else {
            None
        },
    });

    Ok((client, connection))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::timeout;

    use crate::config::SslMode;
    use crate::connect::connect_tcp;
    use crate::{Config, NoTls};

    /// Accepts a real TCP connection and reads its PostgreSQL startup message.
    async fn accept_startup(listener: TcpListener) -> TcpStream {
        let (mut socket, _) = listener.accept().await.unwrap();
        let length = socket.read_u32().await.unwrap();
        assert!((8..4096).contains(&length));
        let mut startup = vec![0; usize::try_from(length - 4).unwrap()];
        socket.read_exact(&mut startup).await.unwrap();
        assert_eq!(&startup[..4], &[0, 3, 0, 0]);
        socket
    }

    #[tokio::test]
    async fn startup_failure_retries_remaining_address() {
        let rejected = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let healthy = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addresses = vec![
            rejected.local_addr().unwrap(),
            healthy.local_addr().unwrap(),
        ];
        let mut config = Config::new();
        config.user("test_user").ssl_mode(SslMode::Disable);

        let reject_startup = async {
            // TCP succeeds; closing during startup must still permit another address.
            drop(accept_startup(rejected).await);
        };
        let complete_startup = async {
            let mut socket = accept_startup(healthy).await;
            // AuthenticationOk followed by ReadyForQuery completes this test server's startup.
            socket
                .write_all(b"R\0\0\0\x08\0\0\0\0Z\0\0\0\x05I")
                .await
                .unwrap();
            let mut byte = [0];
            assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
        };
        let connect = async {
            let (client, connection) =
                connect_tcp(addresses, Some("localhost"), &mut NoTls, &config)
                    .await
                    .unwrap();
            drop(client);
            drop(connection);
        };

        timeout(Duration::from_secs(10), async {
            tokio::join!(reject_startup, complete_startup, connect);
        })
        .await
        .unwrap();
    }
}
