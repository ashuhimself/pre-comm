REQUIRED_DEFAULT_ARGS = {"owner", "retries", "retry_delay", "on_failure_callback"}

# Heavy module-level calls that run on every scheduler parse
_HEAVY_OBJECTS = {"Variable", "Connection"}
_HEAVY_HTTP = {"requests", "httpx", "urllib"}
_HEAVY_METHODS = {"execute", "fetchall", "fetchone", "fetchmany", "get_records"}

"C10": "DAG must define default_args with owner, retries, retry_delay, on_failure_callback",
"C11": "No heavy calls at module level (Variable.get, DB, HTTP) — runs on every parse",
"C12": "DAG must have tags=[...] for ownership/domain",


def check_default_args(path: Path) -> List[str]:
    """
    [C10] DAG must define default_args containing owner, retries,
    retry_delay, and on_failure_callback.
    """
    tree = _parse(path)
    if tree is None:
        return []

    def _extract_keys(value: ast.AST) -> Optional[Set[str]]:
        if not isinstance(value, ast.Dict):
            return None
        return {
            k.value for k in value.keys
            if isinstance(k, ast.Constant) and isinstance(k.value, str)
        }

    found_keys: Optional[Set[str]] = None
    for node in ast.walk(tree):
        # default_args = {...}
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == "default_args"
        ):
            keys = _extract_keys(node.value)
            if keys is not None:
                found_keys = keys

        # @dag(default_args={...}) or DAG(default_args={...})
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if kw.arg == "default_args":
                    keys = _extract_keys(kw.value)
                    if keys is not None:
                        found_keys = keys

    errors: List[str] = []
    if found_keys is None:
        errors.append(
            "[C10] default_args dict not found — "
            f"must define {sorted(REQUIRED_DEFAULT_ARGS)}"
        )
    else:
        missing = REQUIRED_DEFAULT_ARGS - found_keys
        if missing:
            errors.append(
                f"[C10] default_args is missing required keys: {sorted(missing)}"
            )
    return errors


def check_no_heavy_top_level(path: Path) -> List[str]:
    """
    [C11] No heavy calls at module level. Top-level code runs on every
    scheduler DAG parse — move into tasks.
    """
    tree = _parse(path)
    if tree is None:
        return []

    errors: List[str] = []

    def _flag(call: ast.Call) -> Optional[str]:
        if not isinstance(call.func, ast.Attribute):
            return None
        attr = call.func.attr
        if not isinstance(call.func.value, ast.Name):
            return None
        obj = call.func.value.id

        if obj in _HEAVY_OBJECTS:
            return f"{obj}.{attr}()"
        if obj in _HEAVY_HTTP and attr in {"get", "post", "put", "delete", "request"}:
            return f"{obj}.{attr}()"
        if attr in _HEAVY_METHODS:
            return f"{obj}.{attr}()"
        return None

    # Only inspect module-level statements — skip function/class bodies
    for stmt in tree.body:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        for child in ast.walk(stmt):
            if isinstance(child, ast.Call):
                bad = _flag(child)
                if bad:
                    errors.append(
                        f"[C11] Heavy call '{bad}' at module level "
                        f"(line {child.lineno}) — move inside a task. "
                        f"This runs on every scheduler parse."
                    )
    return errors

def check_dag_has_tags(path: Path) -> List[str]:
    """[C14] DAG must set tags=[...] for ownership and discoverability."""
    tree = _parse(path)
    if tree is None:
        return []
    errors: List[str] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, (ast.Name, ast.Attribute)):
            func_name = (
                node.func.id if isinstance(node.func, ast.Name)
                else getattr(node.func, "attr", None)
            )
            if func_name not in {"dag", "DAG"}:
                continue
            tags_kw = next((kw for kw in node.keywords if kw.arg == "tags"), None)
            if tags_kw is None:
                errors.append(
                    f"[C14] DAG at line {node.lineno} is missing 'tags=[...]'."
                )
            elif isinstance(tags_kw.value, ast.List) and not tags_kw.value.elts:
                errors.append(
                    f"[C14] DAG at line {node.lineno} has empty 'tags=[]'."
                )
    return errors


    # issues.extend(check_default_args(path))
    # issues.extend(check_no_heavy_top_level(path))