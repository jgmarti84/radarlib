Thank you for contributing to radarlib!

Guidelines
- Fork the repo and create a feature branch for your changes.
- Run tests locally before opening a PR: pytest -q
- Follow PEP 8. We use Black and Flake8; run them locally or rely on CI.
- Write tests for new features and bug fixes. Put tests under tests/unit or tests/integration and use fixtures from tests/conftest.py.

Pull Request Checklist
- [ ] Tests added/updated
- [ ] Linting checks pass (black --check, flake8)
- [ ] CI is green

If your change needs a dataset or large binary, don't commit it to the repository — use an external data server or add it to tests/data and mark the tests as integration.

## Running tests (local)

We don't require `tox` to run tests locally. Use the following commands to set up your development environment and run the test suite.

1. Create and activate a virtual environment (recommended):

```bash
python -m venv .venv
source .venv/bin/activate
```

2. Install the package in editable mode and install development dependencies:

```bash
pip install --upgrade pip
pip install -e .
pip install -r requirements-dev.txt
```

3. Run unit tests only (fast):

```bash
pytest -q -m "not integration"
```

4. Run integration tests (requires real BUFR files and/or the dynamic library):

Place real `.BUFR` files under `tests/data/bufr/` and any required resources under `tests/data/bufr_resources/`, then:

```bash
pytest -q -m integration
```

If no files are present under `tests/data/bufr/`, the integration tests will be skipped.

5. Run linters and formatter checks (used by CI):

```bash
black --check src tests --exclude src/radarlib/io/bufr/bufr_resources
flake8 src tests --exclude src/radarlib/io/bufr/bufr_resources
```

### Pre-commit hooks

We recommend installing `pre-commit` so formatting and linting run automatically before commits.

Install and enable hooks:

```bash
pip install pre-commit
pre-commit install
pre-commit run --all-files  # run once for existing files
```

This installs hooks defined in `.pre-commit-config.yaml` (Black, isort, flake8, and basic checks).

## Adding tests

- Put unit tests under `tests/unit/` and name them `test_*.py`.
- Put integration tests under `tests/integration/` and mark them with `@pytest.mark.integration`.
- Use fixtures from `tests/conftest.py` for common test data and mocking helpers.
- For functions that touch the C library or real BUFR files, prefer monkeypatching the low-level I/O functions so unit tests remain fast and deterministic.

When adding large test data files, do not commit them to the repository directly. Instead, use Git LFS or an external host and document how to fetch the files in this CONTRIBUTING guide.
