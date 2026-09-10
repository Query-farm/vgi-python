# Copyright 2026 Query Farm LLC - https://query.farm

"""Run portable Filter v2 corpus cases against any VGI example worker."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pyarrow as pa
from vgi_rpc.rpc import RpcError

from vgi.arguments import Arguments
from vgi.client import Client, ClientError
from vgi.filter_v2_builder import deserialize_filter_batch

ROOT = Path(__file__).resolve().parent


def read_batch(relative: str) -> pa.RecordBatch:
    """Read one corpus IPC stream."""
    return deserialize_filter_batch((ROOT / relative).read_bytes())


def run_case(worker: str, case: dict[str, Any]) -> str | None:
    """Run one worker-compatible case, returning a failure description."""
    worker_case = case["worker"]
    expected = read_batch(case["worker_expected"]) if "worker_expected" in case else None
    join_keys = [read_batch(path) for path in case.get("join_keys", [])]
    try:
        with Client(worker, pool=None) as client:
            output = list(
                client.table_function(
                    function_name=worker_case["function"],
                    schema_path=worker_case["schema_path"],
                    arguments=Arguments(positional=tuple(pa.scalar(value) for value in worker_case["arguments"])),
                    projection_ids=worker_case["projection_ids"],
                    pushdown_filters=(ROOT / case["filter"]).read_bytes(),
                    join_keys=join_keys,
                )
            )
    except (ClientError, RpcError) as exc:
        if "error" in case:
            return None
        return f"unexpected worker error: {exc}"
    if "error" in case:
        return f"expected worker error containing {case['error']!r}"
    actual = pa.Table.from_batches(output) if output else pa.Table.from_batches([expected.slice(0, 0)])
    expected_table = pa.Table.from_batches([expected])
    actual_shape = [(field.name, field.type) for field in actual.schema]
    expected_shape = [(field.name, field.type) for field in expected_table.schema]
    if actual_shape != expected_shape or actual.num_rows != expected_table.num_rows:
        return f"rows differ: actual={actual.to_pylist()} expected={expected_table.to_pylist()}"
    if actual.num_rows and actual.to_pylist() != expected_table.to_pylist():
        return f"rows differ: actual={actual.to_pylist()} expected={expected_table.to_pylist()}"
    return None


def main() -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", required=True, help="Worker command or executable path")
    parser.add_argument("--case", action="append", dest="case_ids", help="Run only this case ID (repeatable)")
    args = parser.parse_args()

    manifest = json.loads((ROOT / "runtime-manifest.json").read_text())
    selected = [case for case in manifest["cases"] if case.get("worker")]
    if args.case_ids:
        wanted = set(args.case_ids)
        selected = [case for case in selected if case["id"] in wanted]
    failures: list[str] = []
    for case in selected:
        failure = run_case(args.worker, case)
        marker = "PASS" if failure is None else "FAIL"
        print(f"{marker} {case['id']}")
        if failure is not None:
            failures.append(f"{case['id']}: {failure}")
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print(f"{len(selected)} worker corpus cases passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
