# Copyright 2025, 2026 Query Farm LLC - https://query.farm

r"""Emit RecordBatch-builders for the VGI **secret** protocol (Orchard).

Sibling of :mod:`vgi.codegen.cpp_request_builders`: same machinery, but it walks
:class:`vgi.secret_protocol.VgiSecretProtocol` and includes the secret schema
header (``vgi_secret_protocol_schemas.hpp``) instead of the catalog one.

### Multirepo workflow

    uv run --project ~/Development/vgi-python vgi-gen-cpp-secret-request-builders \
        > ~/Development/vgi/src/generated/vgi_secret_request_builders.hpp

``tests/test_generated_cpp_secret_request_builders.py`` enforces drift.
"""

from __future__ import annotations

import argparse
import sys
from typing import TYPE_CHECKING

from vgi.codegen._common import DEFAULT_CPP_NAMESPACE, GeneratorError, collect_schemas, parse_cpp_namespace
from vgi.codegen.cpp_request_builders import emit_builders
from vgi.secret_protocol import VgiSecretProtocol

if TYPE_CHECKING:
    from typing import TextIO


def emit(out: TextIO, namespace: list[str] | None = None) -> None:
    """Emit the generated C++ secret request-builder header to *out*."""
    schemas = collect_schemas(
        VgiSecretProtocol,
        info_types=(),
        extra_response_types=(),
        check_info_subclasses=False,
    )
    emit_builders(
        out,
        schemas,
        generator_module="vgi.codegen.cpp_secret_request_builders",
        generator_command="vgi-gen-cpp-secret-request-builders",
        regen_command_lines=[
            "uv run --project ~/Development/vgi-python vgi-gen-cpp-secret-request-builders \\",
            "  > ~/Development/vgi/src/generated/vgi_secret_request_builders.hpp",
        ],
        schemas_include="vgi_secret_protocol_schemas.hpp",
    )


def main() -> None:
    """Console-script entrypoint — write the generated header to stdout."""
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
    try:
        emit(sys.stdout, namespace)
    except GeneratorError as e:
        print(f"\nerror: {e}\n", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
