# Copyright 2026 Query Farm LLC - https://query.farm

"""Private native scan probe: terminate a real worker before producer EOS.

The first generator tick goes through the ordinary filtering, collection and
Arrow writer. The next tick exits the process without closing that writer, so
the client receives a physical transport EOF after a valid empty or nonempty
batch. This must fail, never return a successful partial query result.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from unittest.mock import patch

from vgi._test_fixtures.table.sequence import NestedSequenceFunction
from vgi._test_fixtures.table.splits import SplitPaginatedFunction
from vgi._test_fixtures.worker import main as fixture_main

_nested_process = NestedSequenceFunction.process
_split_process = SplitPaginatedFunction.process


def _process(cls: type[Any], params: Any, state: Any, out: Any) -> None:
    del cls
    if os.environ.get("VGI_SCAN_TRUNCATION_MODE") == "drop" and state.current_index >= 100:
        Path(os.environ["VGI_SCAN_TRUNCATION_EVENT"]).write_text(
            f"pid={os.getpid()} next_index={state.current_index}\n", encoding="utf-8"
        )
        os._exit(75)
    _nested_process(params, state, out)


def _process_split(cls: type[Any], params: Any, state: Any, out: Any) -> None:
    del cls
    if os.environ.get("VGI_SCAN_TRUNCATION_MODE") == "drop_split" and state.cur >= state.hi[state.idx]:
        Path(os.environ["VGI_SCAN_TRUNCATION_EVENT"]).write_text(
            f"pid={os.getpid()} split_end={state.cur}\n", encoding="utf-8"
        )
        os._exit(75)
    _split_process(params, state, out)


def main() -> None:
    """Apply failure injection only inside this separately launched test worker."""
    with (
        patch.object(NestedSequenceFunction, "process", classmethod(_process)),
        patch.object(SplitPaginatedFunction, "process", classmethod(_process_split)),
    ):
        fixture_main()


if __name__ == "__main__":
    main()
