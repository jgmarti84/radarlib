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
