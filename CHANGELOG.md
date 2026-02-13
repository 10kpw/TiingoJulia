# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[unreleased]: https://github.com/10kpw/TiingoJulia/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/10kpw/TiingoJulia/releases/tag/v1.0.0
