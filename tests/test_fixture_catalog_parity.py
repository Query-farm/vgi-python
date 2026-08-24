# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""The two fixture-worker entry points must serve the same catalogs.

``vgi-fixture-worker`` (subprocess/launcher transport) and ``vgi-fixture-http``
(HTTP transport) each assemble their own list of worker classes. The SAME
``.test`` files run against both, so a catalog present in one list and absent
from the other fails those files on one transport only — which reads as a
transport-specific bug and is not one.

That is not hypothetical. ``NarrowBindWorker`` was added to the subprocess entry
point and not the HTTP one, so ``narrow_bind_mismatch.test`` failed every HTTP
run with ``No worker handles catalog 'narrow_bind'``. That test guards a client
SIGSEGV — a worker whose bind ``output_schema`` is narrower than the table's
advertised columns used to walk off the end of the batch in
``ArrowTableFunction::ArrowToDuckDB`` — so it was the worst one to silently lose
on a transport.

Both lists are built inside function bodies with local imports (deliberately:
the fixture extras are optional, and a module-level import would make the whole
package require them). So this reads the source rather than calling the
functions, which would start a server.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pytest

# Workers each entry point may legitimately add on its own. The writable
# catalog is conditional on an optional extra in BOTH, and is appended outside
# the literal list, so it never appears in either parsed set.
_ALLOWED_ASYMMETRY: dict[str, str] = {}


def _worker_class_names(module_path: Path, func_name: str) -> set[str]:
    """Names in the `worker_classes = [...]` / `workers = [...]` literal.

    Parsed rather than executed: calling either function starts a worker.
    """
    tree = ast.parse(module_path.read_text())

    target: ast.FunctionDef | None = None
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == func_name:
            target = node
            break
    assert target is not None, f"{module_path.name} has no function {func_name}()"

    for node in ast.walk(target):
        if not isinstance(node, ast.AnnAssign | ast.Assign):
            continue
        targets = [node.target] if isinstance(node, ast.AnnAssign) else node.targets
        names = {t.id for t in targets if isinstance(t, ast.Name)}
        if not names & {"worker_classes", "workers"}:
            continue
        if not isinstance(node.value, ast.List):
            continue
        found = {e.id for e in node.value.elts if isinstance(e, ast.Name)}
        if found:
            return found

    pytest.fail(
        f"{module_path.name}:{func_name}() no longer assigns a list literal of worker "
        "classes to `worker_classes` or `workers`. This guard now matches nothing — "
        "update the parser rather than deleting the test."
    )


@pytest.fixture(scope="module")
def _lists() -> tuple[set[str], set[str]]:
    import vgi._test_fixtures.http_server as http_server
    import vgi._test_fixtures.worker as subprocess_worker

    sub = _worker_class_names(Path(inspect.getfile(subprocess_worker)), "main")
    http = _worker_class_names(Path(inspect.getfile(http_server)), "main")
    return sub, http


def test_both_lists_are_non_empty(_lists: tuple[set[str], set[str]]) -> None:
    """A parser that silently matches nothing would make this whole file vacuous."""
    sub, http = _lists
    assert len(sub) >= 2, f"subprocess entry point parsed as {sub} — parser is broken"
    assert len(http) >= 2, f"HTTP entry point parsed as {http} — parser is broken"


def test_entry_points_serve_the_same_catalogs(_lists: tuple[set[str], set[str]]) -> None:
    """Neither entry point may serve a catalog the other does not."""
    sub, http = _lists
    only_sub = sorted(sub - http - set(_ALLOWED_ASYMMETRY))
    only_http = sorted(http - sub - set(_ALLOWED_ASYMMETRY))
    assert not only_sub and not only_http, (
        "the fixture entry points serve different catalogs, so the shared .test files "
        "will fail on one transport only:\n"
        f"  only in vgi-fixture-worker (subprocess): {only_sub}\n"
        f"  only in vgi-fixture-http:                {only_http}\n"
        "Add the missing worker to the other list, or record it in _ALLOWED_ASYMMETRY "
        "with the reason it is genuinely one-transport."
    )


def test_allowed_asymmetry_stays_honest(_lists: tuple[set[str], set[str]]) -> None:
    """An excuse for a worker neither list mentions reads as coverage that isn't there."""
    sub, http = _lists
    for name, reason in _ALLOWED_ASYMMETRY.items():
        assert name in sub or name in http, (
            f"_ALLOWED_ASYMMETRY excuses {name!r} ({reason}), but neither entry point mentions it — drop the entry."
        )
