# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Apache-2.0 `LICENSE` and `NOTICE`.
- `pyproject.toml` packaging with per-provider optional extras
  (`bedrock` / `anthropic` / `gateway` / `all` / `dev`) and a `cv-assessor`
  console entry point.
- Command-line flags (`--provider`, `--limit`, `--since`, `--criteria-file`,
  `--output-csv`, `--dry-run`, `--version`) that override `.env` values.
- `--dry-run` mode: lists which emails would be scored (PDF present / skipped /
  already processed) without calling the model, sending mail, or writing the CSV.
- Network-free `pytest` suite covering JSON parsing/coercion, email PDF
  extraction, acknowledgement rendering, and CLI argument handling.
- GitHub Actions CI running `ruff` + `pytest` on Python 3.9–3.12.
- `CONTRIBUTING.md`, `CHANGELOG.md`, and GitHub issue / pull-request templates.
- README: "Responsible & ethical use" disclaimer, privacy/PII note, supported-
  providers matrix, example output, and a troubleshooting section.

### Changed
- `requirements.txt` now installs the core deps plus all provider SDKs and points
  users at the lighter `pyproject.toml` extras.

## [0.1.0]

### Added
- Initial release: fault-tolerant batch CV scorer over IMAP with native PDF
  ingestion, pluggable Bedrock / Anthropic / OpenAI-compatible-gateway providers,
  crash-safe CSV output, resumable runs, and opt-in neutral acknowledgement
  emails with a rolling-window send rate limiter.
