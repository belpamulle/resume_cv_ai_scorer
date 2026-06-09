# Contributing to CV Assessor

Thanks for your interest in improving CV Assessor. This is a small, focused tool;
contributions that keep it simple, robust, and crash-safe are very welcome.

## Ground rules

Before contributing, please read the **[Responsible & ethical use](README.md#responsible--ethical-use)**
section of the README. This is an automated hiring-support tool, and we will not
accept changes that:

- enable fully automated rejection/acceptance of candidates,
- email scores, red flags, or any assessment data to applicants, or
- weaken the human-in-the-loop posture of the project.

## Development setup

```bash
git clone <your-fork-url>
cd cv-assessor
python -m venv .venv && source .venv/bin/activate
pip install -e ".[all,dev]"      # all providers + pytest + ruff
```

You can install a single provider extra instead of `all` if you only work on one
backend, e.g. `pip install -e ".[gateway,dev]"`.

## Before opening a pull request

```bash
ruff check .          # lint
ruff format .         # (optional) auto-format
pytest -q             # run the test suite
```

CI runs `ruff` + `pytest` on Python 3.9–3.12; please make sure both pass locally.

## Guidelines

- **Keep it dependency-light.** Provider SDKs are optional extras and imported
  lazily inside each provider class — don't add top-level imports of `boto3`,
  `anthropic`, or `openai`.
- **Never abort the batch.** Per-email failures must be caught and recorded
  (`status=API_ERROR`), never raised out of the main loop.
- **Add tests** for any new pure/parsing logic. The existing suite in `tests/`
  is network-free and is the model to follow.
- **Don't commit candidate data.** `candidates.csv` and any `*.csv` are
  git-ignored on purpose; double-check you aren't adding real PII.

## Reporting security issues

Please do not file public issues for security-sensitive reports (e.g. credential
handling). Instead contact the maintainers privately. When sharing reproductions,
**redact all real candidate data and credentials.**
