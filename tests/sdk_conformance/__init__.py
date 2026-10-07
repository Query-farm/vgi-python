# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Cross-SDK conformance groups, run against any SDK's fixture worker over HTTP.

Each module skips entirely unless ``VGI_SDK_HTTP_URL`` names a running worker,
so a plain ``pytest`` run stays green. See the module docstrings for the
environment each group reads.
"""
