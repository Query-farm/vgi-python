# Copyright 2025, 2026 Query Farm LLC - https://query.farm

r"""Emit ``vgi_protocol_names.hpp`` — the C++ side's view of the wire routing keys.

Third sibling of :mod:`vgi.codegen.cpp_constants` (byte keys from
``vgi_rpc.metadata``) and :mod:`vgi.codegen.cpp_protocol_version` (the surface
versions). This one emits the ``vgi_rpc.protocol`` *values*: the names a client
must send to reach each protocol this repo declares.

Why this generator exists
-------------------------

It was added after a real break, and the break is the specification. Neither
Protocol declared a ``protocol_name``, so every implementation's wire name
defaulted to whatever its local class happened to be called — six
implementations, four different answers. That was invisible until the
transports made ``vgi_rpc.protocol`` a required routing key, at which point
there was no single string the DuckDB extension could send.

The names were then decided (``vgi.v2``, ``vgi.secret.v1``) and hand-written on
both sides. That is the same shape of hazard one step removed: the version is
generated *and* drift-guarded, so a version bump that misses the C++ tree is a
red build, while a rename that missed it was a silent misroute. A wire contract
that two repos spell independently will eventually be spelled differently.

Both names live in ONE header, unlike the versions, which have one file each.
They are not two independent facts: they are the wire-name contract, decided
together and — as the commit that introduced them shows — changed together. One
artifact means one regen entry and one drift test, and makes it impossible to
update one name while leaving the other stale.

The name is read through vgi-rpc's own ``_protocol_wire_name`` rather than
reimplementing its rule here. The rule has a subtlety worth not duplicating:
it reads ``vars(protocol)`` rather than ``getattr``, so a Protocol that
subclasses another and forgets to redeclare does NOT silently inherit its
parent's routing key. Reimplementing that with ``getattr`` would make this
generator disagree with the dispatcher it exists to serve — which is precisely
the class of drift the generator is for.

Workflow:

    uv run --project ~/Development/vgi-python python scripts/regen_generated.py

``tests/test_generated_cpp_protocol_name.py`` enforces drift detection at PR time.
"""

from __future__ import annotations

import argparse
import io
import sys
from typing import TYPE_CHECKING

from vgi_rpc.rpc._types import (
    _protocol_wire_name,
    validate_protocol_name,
)

from vgi.codegen._common import (
    DEFAULT_CPP_NAMESPACE,
    GeneratorError,
    close_namespace,
    open_namespace,
    parse_cpp_namespace,
    provenance_comment,
)
from vgi.protocol import VgiProtocol
from vgi.secret_protocol import VgiSecretProtocol

if TYPE_CHECKING:
    from typing import TextIO


GENERATOR_VERSION = "1"


def current_protocol_name() -> str:
    """Return the wire routing key for :class:`vgi.protocol.VgiProtocol`."""
    return _wire_name(VgiProtocol)


def current_secret_protocol_name() -> str:
    """Return the wire routing key for :class:`vgi.secret_protocol.VgiSecretProtocol`."""
    return _wire_name(VgiSecretProtocol)


def _wire_name(protocol: type) -> str:
    """Resolve and validate one Protocol's wire name.

    Resolution is delegated to vgi-rpc so this never disagrees with the
    dispatcher. Validation is vgi-rpc's too, and runs here so a name that the
    server would refuse to route fails at generation rather than becoming a
    string literal that every client dutifully sends and every server rejects.

    ``allow_reserved`` is deliberately NOT set: the ``vgi_rpc.`` prefix belongs
    to protocols the framework itself defines, and nothing this repo declares
    may claim it.
    """
    name = _protocol_wire_name(protocol)
    try:
        validate_protocol_name(name)
    except ValueError as exc:
        raise GeneratorError(f"{protocol.__name__} has an unroutable wire name: {exc}") from exc

    # The name is emitted into a C++ string literal AND into an HTTP URL path
    # segment. validate_protocol_name's charset already excludes everything that
    # would need escaping in either; this asserts that rather than assuming it,
    # so a future loosening upstream surfaces here instead of as a malformed
    # header or a percent-encoding mismatch at some proxy.
    if not all(0x20 <= ord(c) < 0x7F for c in name):
        raise GeneratorError(f"non-printable byte in protocol_name {name!r}; this is a bug")
    return name


def emit(out: TextIO, namespace: list[str] | None = None) -> None:
    """Emit ``vgi_protocol_names.hpp`` to *out*."""
    if namespace is None:
        namespace = parse_cpp_namespace(DEFAULT_CPP_NAMESPACE)

    entries = [
        (
            "VGI_PROTOCOL_NAME",
            current_protocol_name(),
            "VgiProtocol",
            "the worker/catalog protocol: bind, init, catalog_*, aggregates, table_buffering_*",
        ),
        (
            "VGI_SECRET_PROTOCOL_NAME",
            current_secret_protocol_name(),
            "VgiSecretProtocol",
            "the standalone secret service: the single unary secret_lookup",
        ),
    ]

    body = io.StringIO()
    body.write("#pragma once\n\n")
    body.write("#include <string_view>\n\n")
    body.write(open_namespace(namespace))
    body.write("\n")
    body.write("// Wire routing keys. A vgi-rpc server dispatches on the pair\n")
    body.write("// (protocol, method): one server may co-host several protocols and their\n")
    body.write("// method names may collide, so every request must name the protocol it\n")
    body.write("// addresses under `vgi_rpc.protocol`. On the raw transports that metadata\n")
    body.write("// key is the only carrier; over HTTP the same value is also the protocol\n")
    body.write("// path segment, and the server rejects a request whose two disagree.\n")
    body.write("//\n")
    body.write("// The major version is part of the name, so an incompatible major is a\n")
    body.write("// DIFFERENT protocol and an unroutable request 404s -- an answer any proxy\n")
    body.write("// understands without an Arrow parser.\n")
    for constant_name, value, source, purpose in entries:
        body.write("\n")
        body.write(f"// {source} -- {purpose}.\n")
        body.write(f"// Sourced from {source}.protocol_name (vgi-python).\n")
        body.write(f'inline constexpr std::string_view {constant_name} = "{value}";\n')
    body.write("\n")
    body.write(close_namespace(namespace))

    out.write("// ============================================================================\n")
    out.write(
        provenance_comment(
            generator_module="vgi.codegen.cpp_protocol_name",
            generator_command="python -m vgi.codegen.cpp_protocol_name",
            generator_version=GENERATOR_VERSION,
            regen_command_lines=[
                "uv run --project ~/Development/vgi-python python scripts/regen_generated.py",
            ],
            body=body.getvalue(),
        )
    )
    out.write("// ============================================================================\n")
    out.write("\n")
    out.write(body.getvalue())


def main() -> None:
    """Console-script entry point."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--namespace",
        default=DEFAULT_CPP_NAMESPACE,
        help=(
            "C++ namespace to emit into, `::`-separated "
            f"(default: {DEFAULT_CPP_NAMESPACE}). VGI is not DuckDB-only: a "
            "standalone worker SDK wants something like `vgi::generated`."
        ),
    )
    args = parser.parse_args()
    try:
        namespace = parse_cpp_namespace(args.namespace)
    except GeneratorError as e:
        print(f"\nerror: {e}\n", file=sys.stderr)
        sys.exit(2)
    emit(sys.stdout, namespace)


if __name__ == "__main__":
    main()
