<!-- Keep a Changelog repeats the same section headings under every release, so
     duplicate-heading detection is scoped to siblings for this file only. -->
<!-- markdownlint-configure-file { "MD024": { "siblings_only": true } } -->

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [4.2.2] - 2026-09-07

### Fixed

- PostgreSQL upserts sent a JSON `null` as the literal text `"nothing"`.
  Tiingo returns `volume: null` for some rows (FLMI in the 2026-09-06 weekly
  reconcile); JSON3 decodes that as `nothing`, `get_ticker_data` returns the
  raw frame, and LibPQ stringifies `nothing` rather than mapping it to SQL
  NULL, so the staging insert failed with
  `invalid input syntax for type bigint: "nothing"`. `select_upsert_columns`,
  the single path every PostgreSQL upsert takes, now replaces `nothing` with
  `missing`, so the value reaches the server as NULL and the existing
  `COALESCE` defaults apply (#681).

## [4.2.1] - 2026-08-22

### Fixed

- The tag release gate in `docker-publish.yml` and `Docs.yml` matched the
  preflight run on `.path == ".github/workflows/release-preflight.yml@main"`.
  The Actions API reports `path` without an `@ref` suffix — that form belongs
  to `workflow_ref` — so the condition could never hold and both workflows
  failed on every release tag, v4.1.0 and v4.2.0 included. Image publication
  and documentation deployment from a tag have therefore never run; the `main`
  paths were unaffected because the gate only applies to tags.

  The branch is now pinned separately with `.head_branch == "main"`, so
  dropping the suffix does not weaken the check that the attesting run came
  from `main`.

  The contract test asserted the shape of this condition, not whether its
  constant matched reality, which is why it passed throughout. It now also
  asserts the `@main` form is absent.

## [4.2.0] - 2026-08-22

### Fixed

- `close_duckdb` no longer reduces every close failure to a warning. A failed
  close is now raised when the caller is on a normal return path, and demoted
  to a logged error only while an exception is already unwinding, so a
  `finally` still surfaces the original failure rather than the cleanup one.

  Swallowing unconditionally made a failed close invisible in the one place it
  mattered: the scheduler closes and then immediately prints a success line,
  and its shell wrapper reads the Julia exit code as the verdict, so the run
  reported success either way.

  This is behavioural for callers that close outside a `finally` — a close
  failure that previously produced a warning and exit 0 now propagates.

### Changed

- Minimum supported Julia is now 1.10, the current long-term-support release.
  The floor was 1.9, which is no longer an LTS and which CI could only test by
  deleting the manifest and resolving fresh. That resolution selected DuckDB.jl
  1.3.2 against DuckDB_jll 1.5.5 — a wrapper two minor versions behind its
  native library, a pairing no other CI lane and no production deployment uses.
  Julia 1.10 resolves DuckDB.jl 1.5.2 against the same 1.5.5 native library,
  which is also the version the `historical_data` migration workaround in
  `src/db/schema.jl` was written against.

  This is a breaking change for callers on Julia 1.9.

### Added

- `api_request_count` and `reset_api_request_count!`, reporting Tiingo HTTP
  requests issued since the last reset. Tiingo meters hourly *requests*, not
  tickers, and the two diverge exactly where nobody could see: a date range
  longer than the chunk size becomes several requests, and a retryable failure
  becomes several more. A phase that fetched 8,013 tickers may have spent well
  over 8,013 requests against a 10,000/hour ceiling, and the only estimate in
  the system was an unmeasured `tickers * 5` upper bound. Counted where the
  request is issued rather than at a collection loop, so chunking and retries
  are included without a caller having to remember them.

## [4.1.0] - 2026-08-20

### Added

- `is_quota_failure` and a `category` field on `SyncFailure`, carrying one of
  `:quota`, `:no_data`, `:transient`, or `:permanent`. A provider rejecting a
  request because the hourly quota is spent is now distinguishable from a
  request that failed on its own merits, so a caller can treat the gap a later
  cycle will collect differently from something worth alerting on. Only a
  status the thrower committed to earns a category; message wording never does,
  matching the reasoning behind `is_no_data_error`. Four-argument
  `SyncFailure` construction still works and infers `:transient` or
  `:permanent` from `retryable`.
- `postgres_migration_readiness`, a read-only report of whether a database is
  ready to migrate. It takes no lock, opens no transaction, and creates
  nothing, so it is safe against a live database at any time — including from a
  monitoring check. `migrate_postgres!` answered the same question only by
  attempting the work, which meant discovering a problem inside a maintenance
  window rather than while planning one. The report carries `state`
  (`:ready`, `:migration_required`, `:drift`, `:newer_schema`,
  `:unknown_layout`, or `:invalid_ledger`), `ready`, `migratable`, the ledger
  and target versions, and structured `drift`. Note that `postgres_schema_version`
  cannot distinguish a fresh database from an unrecognised pre-ledger one —
  both report `0` — while the readiness report separates them.

### Fixed

- A relative `.env` is now looked for in the caller's working directory before
  the package's own directory. The package directory was the only root, which
  is correct for a repository checkout and wrong for a `Pkg`-installed package,
  where it resolves to `~/.julia/packages/<name>/<hash>/` — a directory `Pkg`
  owns, re-hashes on every version change, and may garbage-collect. The missing
  key error named that path as the place to put a credential; it now names a
  location the operator controls, and mentions `env_path=`. Absolute paths are
  unaffected, and a checkout that keeps its `.env` beside the package still
  resolves through the retained fallback.
- `PRODUCTION.md` no longer claims that every consumer foreign key blocks a
  universe replacement. `replace_ticker_universe` supports exactly one by name,
  `filtered_stocks_ticker_fkey`, recreating it with its `ON DELETE CASCADE`
  inside the replacement transaction — so the document forbade a clause the
  library itself writes. Other consumer foreign keys still fail closed.
- A migration refused because the schema does not match the canonical manifest
  now names each relation, column, and index at fault instead of only saying
  that something differs. The message previously ended with an instruction to
  repair by hand while withholding what to repair, so an operator had to
  reconstruct it against undocumented internals. `_manifest_matches` is now
  defined as the drift report being empty, so the predicate and the message
  cannot disagree, and a long list is summarised rather than dumped.
- The DuckDB `historical_data.fetched_at` migration now also repairs a column
  that carries `NOT NULL` but no default. The guard was keyed on the two states
  the original bug produced — absent, or present and nullable — so a database
  repaired by hand, which is how the constraint gets applied without the
  default, was skipped forever. It is now keyed on the invariant the canonical
  DDL declares. The two repairs are issued independently: re-applying
  `SET NOT NULL` to a column that already carries it aborts the DuckDB process
  rather than raising.

### Changed

- `migrate_postgres!` defaults `statement_timeout_seconds` to 3600 and
  `lock_timeout_seconds` to 30, both now named constants rather than literals
  repeated across call sites. The former 300s statement timeout could not
  complete migration 2, which rewrites every row of `historical_data` in one
  transaction and measured 19m49s against 20,622,888 rows on the production
  data plane. The lock timeout is unchanged in value and stays small: it bounds
  contention with another writer, not the work itself.
- An unrecognised `TIINGO_LOGGER` value now falls back to the console logger
  and warns, naming the rejected value and the supported modes. It previously
  installed a `NullLogger`, discarding every diagnostic the package emits —
  silently, since the logger that would have carried the warning was the one
  being discarded. Silence is still available, but only by asking for it with
  the new `none` mode.

## [4.0.0] - 2026-08-19

### Added

- `NoDataError` and the public predicate `is_no_data_error`, plus an export of
  the existing `ApiStatusError`. `is_no_data_error` is true only for
  `NoDataError` and for an `ApiStatusError` carrying 404 or 410, so callers can
  separate "the security had nothing to give" from "the request failed" without
  matching English substrings in an error message.

### Changed

- **Breaking:** `fetch_api_data` now throws `NoDataError` instead of
  `ErrorException` when a 200 response carries no rows and `allow_empty=false`.
  Message text, redaction, retry policy, and `allow_empty=true` behavior are
  unchanged. Callers that catch `ErrorException` to detect a no-data response
  must catch `NoDataError` or use `is_no_data_error`.

### Removed

- **Breaking:** Removed the seven deprecated DuckDB-first full-history entry
  points: `export_to_postgres`, `update_historical`,
  `update_historical_parallel`, `update_historical_sequential`,
  `download_tickers_duckdb`, `add_historical_data`, and
  `update_split_ticker`. Use the sink-neutral collectors and explicit
  PostgreSQL, Parquet, or DuckDB persistence primitives instead.

### Fixed

- The DuckDB `historical_data.fetched_at` migration no longer fails on any
  database carrying secondary indexes. `ALTER COLUMN ... SET NOT NULL` rewrites
  the column's storage, and DuckDB refuses to alter a table while other catalog
  entries depend on it, so the migration now drops and recreates the secondary
  indexes around the alter in one transaction. Index DDL is read back from
  `duckdb_indexes()` rather than hardcoded, so an index this build does not
  know about is still restored exactly. The `UNIQUE (ticker, date)` constraint
  does not block the alter and the table is not rebuilt.
- The same migration is now keyed on `fetched_at` being nullable rather than
  absent, so a database left half-migrated by the failure above repairs itself
  on the next connect instead of never being retried.
- `replace_ticker_universe` no longer rejects duplicate canonical ticker keys.
  Ticker is not a key of either universe table: the supported-tickers feed
  lists the same symbol under several exchanges and asset types, and the
  canonical PostgreSQL schema indexes ticker without a unique constraint.
  Uniqueness is only required when the optional `filtered_stocks` foreign key
  is deployed, and PostgreSQL enforces that itself through the unique bridge
  index that foreign key depends on.

## [3.0.0] - 2026-08-11

### Changed

- **Breaking:** renamed the package and module from `Tiingo` to
  `QuansiftMarketData`, and moved the repository to
  `Quansift/QuansiftMarketData.jl`. Consumers must replace `using Tiingo` with
  `using QuansiftMarketData`. The UUID and exported API remain unchanged.
- Renamed package, documentation, release, and container identities while
  preserving Tiingo provider terminology and stable production paths,
  environment variables, and PostgreSQL identifiers.

## [2.0.0] - 2026-08-11

### Added

- Sink-neutral collection API: `collect_ticker_universe`, `collect_historical`, and
  `collect_fundamentals`, with the matching normalizers `normalize_eod_prices`,
  `normalize_security_observations`, and `normalize_fundamental_daily_metrics`.
- Typed collection results `HistoricalCollectionResult`, `FundamentalCollectionResult`,
  and `SyncFailure`, plus `SyncIncompleteError` for strict callers that must convert a
  partial failure into a non-successful job result.
- PostgreSQL persistence primitives: `create_tables(pg_conn)`, `replace_ticker_universe`,
  `upsert_security_observations`, `upsert_fundamental_daily_metrics`, and PostgreSQL
  overloads of `upsert_stock_data` and `upsert_stock_data_bulk`.
- Verified atomic local Parquet output via `write_parquet`, with DataFrame and
  PostgreSQL-table overloads returning `ParquetWriteResult`.
- Fundamentals surface: `get_fundamental_meta`, `get_fundamental_watermarks`,
  `sync_fundamentals!`, and a resumable market-cap backfill script.
- `scripts/refresh_postgres_via_duckdb.jl` for refreshing a local PostgreSQL
  `historical_data` table through a throwaway DuckDB working file.
- Opt-in PostgreSQL 17 integration lane in CI, Parquet sink contract tests, and
  secret-redaction tests.
- Forward-only PostgreSQL schema migrations with
  `POSTGRES_SCHEMA_VERSION == 1`, a checksummed public ledger, advisory-lock
  serialization, finite legacy-layout recognition, and rollback-safe
  PostgreSQL 17 integration coverage.
- Deterministic `micro`, `load`, and `soak` benchmark modes in an independent
  project, with bounded synthetic fixtures, metadata-only JSON results, one
  Linux/Julia 1.12 PR smoke, and scheduled PostgreSQL/local evidence jobs.
- An advisory, allowlisted live Tiingo canary that observes normalized frames
  in memory and performs zero application persistence.
- Development, exact-main-SHA release-preflight, and stable post-tag hygiene
  modes covering Project, changelog, citation, config, secrets, migrations,
  hermetic tests, PostgreSQL 17, docs/links, benchmark smoke, and image build.

### Changed

- **Breaking:** renamed the package and module from `TiingoJulia` to `Tiingo`,
  and moved the repository to `Quansift/Tiingo.jl`. Consumers must replace
  `using TiingoJulia` with `using Tiingo`.
- Documented the storage responsibility boundary: system PostgreSQL is the authoritative
  relational store and Parquet is the full-history interchange/archive format. DuckDB is
  optional local analysis state and is no longer a production full-history source of
  record. This repository imposes no DuckDB retention period.
- Cross-system orchestration — DigitalOcean Spaces publication, rolling three-year
  DigitalOcean Managed PostgreSQL publication, and QuantScreener/TATSU sequencing — is
  owned by `quansift_scheduler`, not Tiingo.
- Prefer the `OHLCV_DUCKDB_PATH` and `OHLCV_PG_CONNECTION` environment variables.
- DuckDB historical upsert is now set-based.
- PostgreSQL `create_tables` now delegates to the versioned migration runner;
  unknown, corrupt, or newer schemas fail closed instead of being guessed or
  overwritten.
- CI, documentation, lint, and image release behavior now accepts only exact
  stable `vX.Y.Z` tags. Live-canary status and performance timing/RSS data are
  not release gates.

### Deprecated

- The DuckDB-first full-history path is deprecated and targeted for removal in a
  future major release.
  System PostgreSQL is the source of record and Parquet is the full-history archive, so
  routing full history through DuckDB duplicates both. Affected entry points:
  `export_to_postgres`, `update_historical`, `update_historical_parallel`,
  `update_historical_sequential`, `download_tickers_duckdb`, `add_historical_data`, and
  `update_split_ticker`.
- Replacements are already available: use `collect_historical` with a caller-supplied
  writer instead of `update_historical`, and `collect_ticker_universe` with
  `replace_ticker_universe` instead of `download_tickers_duckdb`. Write full history to
  PostgreSQL through the `upsert_*` overloads and archive it with `write_parquet` instead
  of bridging DuckDB to PostgreSQL through `export_to_postgres`.
- DuckDB itself is not deprecated. Connection, schema, upsert, and query primitives
  (`connect_duckdb`, `close_duckdb`, `create_tables`, `create_indexes`,
  `optimize_database`, the DuckDB `upsert_*` overloads, and `get_tickers_*`) remain
  supported for optional local analysis.

### Fixed

- Fundamentals export completes within a failure tolerance instead of failing
  all-or-nothing, and tolerates empty payloads and multi-candidate probes.
- PostgreSQL export refreshes FK-referenced tables in place via upsert rather than
  drop-and-replace.
- The `historical_data` staging table retains its `(ticker, date)` key.

## [1.0.0] - 2026-02-13

### Added

- `config.toml` and `config.example.toml` for readable, comment-friendly configuration.
- `.env.example` template for secure environment setup.
- CI release hygiene checks for tracked `.env*` files and required config keys.

### Changed

- Configuration loading now prefers `config.toml` and falls back to legacy `config.json`.
- Configuration defaults are overrideable via `TIINGO_*` environment variables.
- API/network tests run only when `TIINGO_TEST_LIVE_API=true`, keeping CI deterministic by default.

### Fixed

- `download_latest_tickers` ZIP extraction now uses compatible `ZipFile.Reader` open/close handling.
- Sequential historical update now correctly handles no-data responses.
- Parallel update no longer assumes a `ticker` column exists in API-returned DataFrames.
- `get_api_key` error output no longer leaks environment variable names.

[unreleased]: https://github.com/Quansift/QuansiftMarketData.jl/compare/v4.2.2...HEAD
[4.2.2]: https://github.com/Quansift/QuansiftMarketData.jl/releases/tag/v4.2.2
[4.2.1]: https://github.com/Quansift/QuansiftMarketData.jl/releases/tag/v4.2.1
[4.2.0]: https://github.com/Quansift/QuansiftMarketData.jl/releases/tag/v4.2.0
[4.1.0]: https://github.com/Quansift/QuansiftMarketData.jl/releases/tag/v4.1.0
[4.0.0]: https://github.com/Quansift/QuansiftMarketData.jl/releases/tag/v4.0.0
[3.0.0]: https://github.com/Quansift/QuansiftMarketData.jl/releases/tag/v3.0.0
[2.0.0]: https://github.com/Quansift/QuansiftMarketData.jl/releases/tag/v2.0.0
[1.0.0]: https://github.com/Quansift/QuansiftMarketData.jl/releases/tag/v1.0.0
