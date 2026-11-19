# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Print the storage-profile summary from ``VGI_STORAGE_PROFILE_FILE`` dumps.

Usage: ``python -m vgi._storage_profile_report profile-*.jsonl``. A separate
module from `vgi._storage_profile`, which the vgi package imports, so ``-m``
does not execute an already-imported module a second time.
"""

import sys

from vgi._storage_profile import report_from_jsonl

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("usage: python -m vgi._storage_profile_report PROFILE.jsonl [...]")
    print(report_from_jsonl(sys.argv[1:]))
