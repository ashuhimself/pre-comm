# Pre-commit setup

This repo uses [pre-commit](https://pre-commit.com) to enforce code quality and Airflow DAG standards on every commit.

## Quick start (one-time setup)

```bash
# 1. Install pre-commit (skip if already installed)
pip install --user pre-commit

# 2. From the repo root, register the git hooks
pre-commit install

# 3. (Optional) Run all hooks against every file in the repo
pre-commit run --all-files
```

That's it. From now on, every `git commit` will automatically run the checks on your staged files.

## What runs on every commit

| Hook | What it does |
|------|--------------|
| `trailing-whitespace` | Strips trailing whitespace |
| `end-of-file-fixer` | Ensures files end with a newline |
| `check-yaml` | Validates YAML syntax |
| `check-added-large-files` | Blocks accidentally committed large files |
| `black` | Formats Python code (line length 100) |
| `ruff` | Lint + import sort + pyupgrade (auto-fixes) |
| `mypy` | Static type checking on `dags/` and `tests/` |
| `check-dag-standards` | Custom Airflow DAG checks (C1–C12) |

### Custom DAG checks

For any `.py` file under `dags/` or `tests/`:

- **C2** — No unused imports or variables _(always runs)_

For files defining a DAG (via `@dag` or `DAG()`):

- **C1** — `catchup=False` must be set
- **C3** — Docstring required and must describe the actual DAG
- **C4** — `is_paused_upon_creation=True` must be set
- **C5** — `schedule` or `schedule_interval` must be defined
- **C6** — `task_id` must be `snake_case`
- **C8** — DAG function must be invoked exactly once at module level
- **C9** — No hardcoded schema names (use `Variable.get()` etc.)
- **C10** — `default_args` must include `owner`, `retries`, `retry_delay`, `on_failure_callback`
- **C11** — No heavy module-level calls (`Variable.get()`, DB, HTTP) — they run on every scheduler parse
- **C12** — DAG must define `tags=[...]`

## Common commands

```bash
# Run all hooks on the entire repo
pre-commit run --all-files

# Run all hooks on specific files
pre-commit run --files dags/my_dag.py

# Run a single hook
pre-commit run ruff --all-files
pre-commit run check-dag-standards --all-files

# Update hook versions to the latest
pre-commit autoupdate
```

## Skipping checks (use sparingly)

```bash
# Skip ALL hooks for one commit
git commit -m "msg" --no-verify

# Skip a specific hook
SKIP=mypy git commit -m "msg"
```

## When a check fails

- **black / ruff** — usually auto-fix the file. Just `git add` the changes and re-commit.
- **mypy** — fix the type error manually.
- **check-dag-standards** — read the `[Cx]` code in the error message; the rule is documented above.

## Troubleshooting

**`pre-commit: command not found`**
Add `~/.local/bin` to your `PATH`, or use `python -m pre_commit` instead.

**Hooks are slow on first run**
Pre-commit downloads each hook's environment the first time. Subsequent runs use the cache.

**Need to clean caches**
```bash
pre-commit clean
pre-commit gc
```

**Hook keeps failing on a file you can't fix right now**
Use `SKIP=<hook-id>` for that commit, then open a ticket to fix it properly.