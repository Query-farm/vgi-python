# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Generate the C++ worker SDK's public headers with its own namespace."""

from __future__ import annotations

import importlib
import io
import re
from typing import TextIO

_GENERATORS = {
    "schemas": "cpp_schemas",
    "constants": "cpp_constants",
    "version": "cpp_protocol_version",
    "names": "cpp_protocol_name",
}


def emit(out: TextIO, *, record: str) -> None:
    """Emit a public header through the coordinated, atomic generation driver."""
    module = importlib.import_module(f"vgi.codegen.{_GENERATORS[record]}")
    rendered = io.StringIO()
    module.emit(rendered, namespace=["vgi", "generated"])
    # Keep the SDK's regeneration command and avoid backslash-continued C++
    # comments, which GCC diagnoses even when Clang accepts them.
    out.write(
        re.sub(
            r"// To regenerate:\n(//   [^\n]*\n)+",
            "// To regenerate:\n//   scripts/regenerate_protocol.sh\n",
            rendered.getvalue(),
            count=1,
        )
    )
