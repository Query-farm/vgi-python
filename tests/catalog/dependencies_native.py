# Copyright 2025, 2026 Query Farm LLC - https://query.farm
"""Run the durable metadata dependency reference for native SQL tests."""

import os

from .dependencies_reference import DependencyCatalog, DependencyWorker

if __name__ == "__main__":
    DependencyCatalog.database_path = os.environ["VGI_DEPENDENCY_REFERENCE_DB"]
    DependencyWorker.main()
