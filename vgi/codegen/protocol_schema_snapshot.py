# Copyright 2025, 2026 Query Farm LLC - https://query.farm

r"""Per-version protocol schema snapshots and the compatibility check (``PROTOCOL_VERSIONS_V1`` §8).

A snapshot records, for one protocol at one ``major.minor``, every method's
parameter / result / header Arrow schema and the Arrow schema of every record
type (``ArrowSerializableDataclass``) defined in the protocol's module. Snapshots
live at ``schemas/<protocol>/<major.minor>/snapshot.json`` and are written at
release time::

    uv run python -m vgi.codegen.protocol_schema_snapshot write

CI runs the check, which compares the current types to the latest snapshot of
each protocol::

    uv run python -m vgi.codegen.protocol_schema_snapshot check

**Passes** (additive): new methods, new record types, new *nullable* fields
appended after the existing ones, a field becoming nullable, new capability
headers (not schema at all). **Fails** unless the protocol's ``major.minor`` was
bumped past the snapshot's: a removed or renamed method, record or field, a type
change, a new non-nullable field, a field becoming non-nullable, or existing
fields moving position (order matters: Arrow parameter and record batches are
positional for some readers). Struct fields are compared recursively by the same
rules; list and map element types must match.

When the current version is newer than the latest snapshot, the check passes and
reminds you to ``write`` the new snapshot; when it is older, it fails.
"""

from __future__ import annotations

import argparse
import base64
import dataclasses
import importlib
import json
import sys
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow as pa
from pyarrow import ipc

#: Protocols with snapshots: (module, class name).  The extension speaks these.
SNAPSHOT_PROTOCOLS: tuple[tuple[str, str], ...] = (
    ("vgi.protocol", "VgiProtocol"),
    ("vgi.secret_protocol", "VgiSecretProtocol"),
    ("vgi.attach_ticket", "AttachTickets"),
)

#: Where snapshots live, relative to the repository root.
SCHEMAS_DIR = Path(__file__).resolve().parents[2] / "schemas"

SNAPSHOT_FORMAT = 1


def _schema_b64(schema: pa.Schema | None) -> str:
    return "" if schema is None else base64.b64encode(schema.serialize().to_pybytes()).decode()


def _schema_text(schema: pa.Schema | None) -> list[str]:
    if schema is None:
        return []
    return [f"{f.name}: {f.type}{'' if f.nullable else ' not null'}" for f in schema]


def _read_schema(b64: str) -> pa.Schema | None:
    return None if not b64 else ipc.read_schema(pa.py_buffer(base64.b64decode(b64)))


def _protocol_class(module: str, name: str) -> type:
    cls = getattr(importlib.import_module(module), name)
    assert isinstance(cls, type)
    return cls


def build_snapshot(protocol: type) -> dict[str, Any]:
    """Build the snapshot document for *protocol* from the current types.

    Args:
        protocol: A Protocol class declaring ``protocol_name`` and ``protocol_version``.

    Returns:
        The JSON-ready snapshot.

    """
    from vgi_rpc.rpc import rpc_methods
    from vgi_rpc.rpc._types import _protocol_wire_name
    from vgi_rpc.utils import ArrowSerializableDataclass

    methods: dict[str, Any] = {}
    for name, info in sorted(rpc_methods(protocol).items()):
        header = info.header_type.ARROW_SCHEMA if info.header_type is not None else None
        result = info.result_schema if info.has_return else None
        methods[name] = {
            "method_type": info.method_type.value,
            "params": _schema_b64(info.params_schema),
            "result": _schema_b64(result),
            "header": _schema_b64(header),
            "params_text": _schema_text(info.params_schema),
            "result_text": _schema_text(result),
            "header_text": _schema_text(header),
        }
    module = sys.modules[protocol.__module__]
    records: dict[str, Any] = {}
    for attr, value in sorted(vars(module).items()):
        if not (
            isinstance(value, type)
            and issubclass(value, ArrowSerializableDataclass)
            and dataclasses.is_dataclass(value)
        ):
            continue
        schema = getattr(value, "ARROW_SCHEMA", None)
        if isinstance(schema, pa.Schema):
            records[attr] = {"schema": _schema_b64(schema), "schema_text": _schema_text(schema)}
    version = vars(protocol)["protocol_version"]
    return {
        "format": SNAPSHOT_FORMAT,
        "protocol": _protocol_wire_name(protocol),
        "protocol_version": version,
        "methods": methods,
        "records": records,
    }


def _major_minor(version: str) -> tuple[int, int]:
    major, minor, *_ = (int(p) for p in version.split("."))
    return major, minor


def snapshot_path(protocol_name: str, version: str, root: Path = SCHEMAS_DIR) -> Path:
    """Return ``schemas/<protocol>/<major.minor>/snapshot.json``.

    Args:
        protocol_name: Wire name.
        version: Semver.
        root: The schemas directory.

    Returns:
        The path.

    """
    major, minor = _major_minor(version)
    return root / protocol_name / f"{major}.{minor}" / "snapshot.json"


def latest_snapshot(protocol_name: str, root: Path = SCHEMAS_DIR) -> tuple[tuple[int, int], dict[str, Any]] | None:
    """Return the newest snapshot for *protocol_name*, or ``None``.

    Args:
        protocol_name: Wire name.
        root: The schemas directory.

    Returns:
        ``((major, minor), document)``.

    """
    base = root / protocol_name
    if not base.is_dir():
        return None
    found: list[tuple[tuple[int, int], Path]] = []
    for child in base.iterdir():
        target = child / "snapshot.json"
        if target.is_file():
            try:
                found.append((_major_minor(child.name + ".0"), target))
            except ValueError:
                continue
    if not found:
        return None
    version, path = max(found)
    return version, json.loads(path.read_text())


# ---------------------------------------------------------------------------
# Compatibility
# ---------------------------------------------------------------------------


@dataclass
class Report:
    """What the comparison found.

    Attributes:
        breaking: Changes that require a version bump.
        additive: Changes that are allowed without one.

    """

    breaking: list[str] = field(default_factory=list)
    additive: list[str] = field(default_factory=list)


def _compare_type(old: pa.DataType, new: pa.DataType, where: str, report: Report) -> None:
    if pa.types.is_struct(old) and pa.types.is_struct(new):
        _compare_fields(
            [old.field(i) for i in range(old.num_fields)], [new.field(i) for i in range(new.num_fields)], where, report
        )
        return
    if (pa.types.is_list(old) and pa.types.is_list(new)) or (
        pa.types.is_large_list(old) and pa.types.is_large_list(new)
    ):
        _compare_fields([old.value_field], [new.value_field], f"{where}[]", report)
        return
    if old != new:
        report.breaking.append(f"{where}: type changed {old} -> {new}")


def _compare_fields(old: list[pa.Field[Any]], new: list[pa.Field[Any]], where: str, report: Report) -> None:
    new_by_name = {f.name: (i, f) for i, f in enumerate(new)}
    for i, of in enumerate(old):
        path = f"{where}.{of.name}"
        hit = new_by_name.get(of.name)
        if hit is None:
            report.breaking.append(f"{path}: field removed or renamed")
            continue
        j, nf = hit
        if j != i:
            report.breaking.append(f"{path}: moved from position {i} to {j}")
        if of.nullable and not nf.nullable:
            report.breaking.append(f"{path}: became non-nullable")
        elif not of.nullable and nf.nullable:
            report.additive.append(f"{path}: became nullable")
        _compare_type(of.type, nf.type, path, report)
    old_names = {f.name for f in old}
    for f in new:
        if f.name in old_names:
            continue
        path = f"{where}.{f.name}"
        if f.nullable:
            report.additive.append(f"{path}: new nullable field")
        else:
            report.breaking.append(f"{path}: new non-nullable (required) field")


def _compare_schema(old_b64: str, new_b64: str, where: str, report: Report) -> None:
    old, new = _read_schema(old_b64), _read_schema(new_b64)
    if old is None and new is None:
        return
    if old is None or new is None:
        report.breaking.append(f"{where}: {'appeared' if old is None else 'disappeared'}")
        return
    _compare_fields(list(old), list(new), where, report)


def compare(old: Mapping[str, Any], new: Mapping[str, Any]) -> Report:
    """Compare two snapshot documents of one protocol.

    Args:
        old: The latest checked-in snapshot.
        new: The snapshot of the current types.

    Returns:
        The breaking and additive differences.

    """
    report = Report()
    for name, om in old["methods"].items():
        nm = new["methods"].get(name)
        if nm is None:
            report.breaking.append(f"method {name}: removed or renamed")
            continue
        if om["method_type"] != nm["method_type"]:
            report.breaking.append(f"method {name}: {om['method_type']} -> {nm['method_type']}")
        for part in ("params", "result", "header"):
            _compare_schema(om[part], nm[part], f"method {name} {part}", report)
    for name in new["methods"]:
        if name not in old["methods"]:
            report.additive.append(f"method {name}: new")
    for name, orec in old["records"].items():
        nrec = new["records"].get(name)
        if nrec is None:
            report.breaking.append(f"record {name}: removed or renamed")
            continue
        _compare_schema(orec["schema"], nrec["schema"], f"record {name}", report)
    for name in new["records"]:
        if name not in old["records"]:
            report.additive.append(f"record {name}: new")
    return report


def check(protocols: Iterable[type], root: Path = SCHEMAS_DIR, out: Any = sys.stdout) -> bool:
    """Run the compatibility check for *protocols*; print findings to *out*.

    Args:
        protocols: Protocol classes.
        root: The schemas directory.
        out: Where to write the report.

    Returns:
        ``True`` when every protocol passes.

    """
    ok = True
    for proto in protocols:
        current = build_snapshot(proto)
        name, version = current["protocol"], current["protocol_version"]
        current_mm = _major_minor(version)
        latest = latest_snapshot(name, root)
        if latest is None:
            print(f"FAIL {name}: no snapshot under {root / name}; run `write` and commit it.", file=out)
            ok = False
            continue
        snap_mm, snapshot = latest
        if current_mm < snap_mm:
            print(
                f"FAIL {name}: current {version} is older than the latest snapshot {snap_mm[0]}.{snap_mm[1]}", file=out
            )
            ok = False
            continue
        if current_mm > snap_mm:
            print(
                f"ok   {name} {version}: version bumped past snapshot {snap_mm[0]}.{snap_mm[1]}; "
                f"run `write` at release to snapshot {current_mm[0]}.{current_mm[1]}",
                file=out,
            )
            continue
        report = compare(snapshot, current)
        for line in report.additive:
            print(f"     {name} additive: {line}", file=out)
        if report.breaking:
            ok = False
            print(
                f"FAIL {name} {version}: {len(report.breaking)} breaking change(s) without a protocol version bump:",
                file=out,
            )
            for line in report.breaking:
                print(f"       {line}", file=out)
        else:
            print(f"ok   {name} {version}: compatible with its snapshot", file=out)
    return ok


def write(protocols: Iterable[type], root: Path = SCHEMAS_DIR) -> list[Path]:
    """Write the current snapshot of each protocol.

    Args:
        protocols: Protocol classes.
        root: The schemas directory.

    Returns:
        The files written.

    """
    written = []
    for proto in protocols:
        doc = build_snapshot(proto)
        path = snapshot_path(doc["protocol"], doc["protocol_version"], root)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(doc, indent=1, sort_keys=True) + "\n")
        written.append(path)
    return written


def default_protocols() -> list[type]:
    """Return the protocol classes in :data:`SNAPSHOT_PROTOCOLS`."""
    return [_protocol_class(m, n) for m, n in SNAPSHOT_PROTOCOLS]


def main(argv: list[str] | None = None) -> int:
    """Console-script entry point.

    Args:
        argv: Arguments (default ``sys.argv[1:]``).

    Returns:
        Process exit status.

    """
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["check", "write"])
    parser.add_argument("--root", type=Path, default=SCHEMAS_DIR, help="schemas directory")
    args = parser.parse_args(argv)
    protocols = default_protocols()
    if args.command == "write":
        for path in write(protocols, args.root):
            print(path)
        return 0
    return 0 if check(protocols, args.root) else 1


if __name__ == "__main__":
    sys.exit(main())
