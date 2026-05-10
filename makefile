.PHONY: help setup install-precommit install-hooks check clean

PYTHON ?= python3

help:
	@echo "Available targets:"
	@echo "  make setup    Install pre-commit (if missing) and register git hooks"
	@echo "  make check    Run all pre-commit checks against the whole repo"
	@echo "  make clean    Remove pre-commit caches"

setup: install-precommit install-hooks
	@echo ""
	@echo "[done] Pre-commit will now run on every 'git commit'."
	@echo "       Run 'make check' to scan the whole repo right now."

install-precommit:
	@if command -v pre-commit >/dev/null 2>&1; then \
		echo "[ok] pre-commit already installed: $$(pre-commit --version)"; \
	else \
		echo "[..] pre-commit not found, installing via pip..."; \
		$(PYTHON) -m pip install --user pre-commit; \
	fi

install-hooks:
	@echo "[..] Registering git hooks..."
	@pre-commit install
	@echo "[..] Pre-fetching hook environments (first time only)..."
	@pre-commit install-hooks

check:
	@pre-commit run --all-files

clean:
	@pre-commit clean
	@pre-commit gc