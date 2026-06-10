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

import sys
from typing import TYPE_CHECKING

from vgi.codegen._common import GeneratorError, collect_schemas
from vgi.codegen.cpp_request_builders import emit_builders
from vgi.secret_protocol import VgiSecretProtocol

if TYPE_CHECKING:
    from typing import TextIO


def emit(out: TextIO) -> None:
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
    try:
        emit(sys.stdout)
    except GeneratorError as e:
        print(f"\nerror: {e}\n", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
