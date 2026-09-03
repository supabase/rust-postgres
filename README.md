# Supabase fork of Rust-Postgres

This repository is Supabase's fork of the
[Materialize rust-postgres fork], which in turn tracks the upstream
[rust-postgres] project.

## Why this fork exists

Supabase maintains this fork to carry targeted changes required by
[Supabase ETL] when the behavior we need is not yet available from Materialize
or upstream. This currently includes workload-specific PostgreSQL `COPY OUT`
response buffering so ETL can apply backpressure promptly and avoid retaining
unnecessary response data while copying large tables.

The fork should remain narrowly focused: custom changes should be documented
and tested, and suitable fixes should still be contributed to the closest
upstream project whenever practical.

The repository lineage is:

```text
sfackler/rust-postgres
        ↓
MaterializeInc/rust-postgres
        ↓
supabase/rust-postgres
```

Consumers should pin an exact commit from this repository rather than depend
on a moving branch.

## Adding a Supabase patch

Open changes against this repository when they are required by Supabase ETL
and cannot be consumed from Materialize or upstream. Keep patches focused and
include tests for behavioral changes.

When a change is generally useful, open or forward the corresponding change to
the appropriate upstream repository so it can eventually be removed from this
fork.

## Integrating Materialize changes

Materialize is the direct upstream for this fork. To incorporate its latest
changes:

```shell
git clone https://github.com/supabase/rust-postgres.git
cd rust-postgres
git remote add materialize https://github.com/MaterializeInc/rust-postgres.git
git fetch materialize
git checkout master
git checkout -b integrate-materialize
git merge materialize/master
# Resolve any conflicts, then open a PR against supabase/rust-postgres.
```

[rust-postgres]: https://github.com/sfackler/rust-postgres
[Materialize rust-postgres fork]: https://github.com/MaterializeInc/rust-postgres
[Supabase ETL]: https://github.com/supabase/etl
