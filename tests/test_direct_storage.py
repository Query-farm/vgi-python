"""vgi-serve storage selection: VGI_STORAGE_BACKEND and the direct S3 / GCS backends."""

from __future__ import annotations

import logging
from typing import Any

import pytest

pytest.importorskip("boto3")

from vgi_rpc.s3 import S3Storage, StorageConfigError  # noqa: E402
from vgi_rpc.storage_env import ObjectStoreBlobClient  # noqa: E402

from vgi.serve import resolve_blob_vendor_http_config, start_storage  # noqa: E402

_S3 = {
    "VGI_STORAGE_S3_BUCKET": "bkt",
    "VGI_STORAGE_S3_ENDPOINT": "https://s3.example.com",
    "VGI_RPC_MAX_UPLOAD_BYTES": "1048576",
}


def test_none_without_configuration() -> None:
    """Nothing configured: everything inline."""
    cfg = resolve_blob_vendor_http_config({})
    assert (cfg.backend, cfg.external_location, cfg.upload_url_provider) == ("none", None, None)


def test_s3_backend_wires_every_role() -> None:
    """S3: results, inbound pointers, objects (external_location.storage) and upload grants."""
    cfg = resolve_blob_vendor_http_config(_S3)
    assert cfg.backend == "s3"
    storage = cfg.external_location.storage
    assert isinstance(storage, S3Storage)
    assert cfg.upload_url_provider is storage
    assert cfg.max_upload_bytes == 1048576
    assert storage.endpoint_url == "https://s3.example.com" and storage.addressing_style == "path"


def test_checksum_none_advertises_no_grants() -> None:
    """checksum=none: no upload-URL provider, so neither upload URLs nor argument refs are advertised."""
    from vgi.arguments_ref import install_arguments_ref_header

    cfg = resolve_blob_vendor_http_config({**_S3, "VGI_STORAGE_S3_CHECKSUM": "none"})
    assert cfg.upload_url_provider is None and cfg.max_upload_bytes is None
    assert cfg.external_location is not None
    assert (
        install_arguments_ref_header(object(), upload_url_provider=None, external_location=cfg.external_location)
        is None
    )


def test_conflict_refuses_to_start() -> None:
    """Vendor and S3 both configured without VGI_STORAGE_BACKEND."""
    with pytest.raises(ValueError, match="Conflicting storage configuration"):
        resolve_blob_vendor_http_config({**_S3, "VGI_BLOB_VENDOR_URL": "https://vendor.example"})


def test_explicit_backend_resolves_conflict() -> None:
    """VGI_STORAGE_BACKEND picks one."""
    cfg = resolve_blob_vendor_http_config({**_S3, "VGI_BLOB_VENDOR_URL": "https://v", "VGI_STORAGE_BACKEND": "s3"})
    assert cfg.backend == "s3"


def test_vendor_backend_needs_url() -> None:
    """An explicit vendor backend without its URL is an error."""
    with pytest.raises(ValueError, match="needs VGI_BLOB_VENDOR_URL"):
        resolve_blob_vendor_http_config({"VGI_STORAGE_BACKEND": "vendor"})


def test_plain_http_needs_opt_in() -> None:
    """An http:// endpoint refuses to start without VGI_STORAGE_ALLOW_PLAIN_HTTP=1."""
    env = {**_S3, "VGI_STORAGE_S3_ENDPOINT": "http://127.0.0.1:9000"}
    with pytest.raises(ValueError, match="VGI_STORAGE_ALLOW_PLAIN_HTTP"):
        resolve_blob_vendor_http_config(env)
    assert resolve_blob_vendor_http_config({**env, "VGI_STORAGE_ALLOW_PLAIN_HTTP": "1"}).backend == "s3"


def test_start_storage_logs_and_verifies(caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch) -> None:
    """Hosted mode and checksum=none are logged; VGI_STORAGE_S3_VERIFY=1 checks the bucket before serving."""
    cfg = resolve_blob_vendor_http_config({**_S3, "VGI_STORAGE_S3_CHECKSUM": "none", "VGI_STORAGE_S3_VERIFY": "1"})
    calls: list[int] = []

    def fail() -> None:
        calls.append(1)
        raise StorageConfigError("S3 bucket 'bkt' does not exist at endpoint https://s3.example.com")

    monkeypatch.setattr(S3Storage, "verify", lambda self: fail())
    with caplog.at_level(logging.INFO, logger="vgi.serve"), pytest.raises(StorageConfigError, match="does not exist"):
        start_storage(cfg, hosted=True)
    text = caplog.text
    assert "Direct object storage: s3 bucket=bkt" in text
    assert "Orchard hosted mode" in text
    assert "VGI_STORAGE_S3_CHECKSUM=none" in text
    assert calls == [1]


def test_function_storage_uses_the_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    """VGI_STORAGE_EXTERNALIZE=1 with an S3 backend externalizes function-storage values to the bucket."""
    from vgi.function_storage_blobs import blob_client_from_env

    for key, value in _S3.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("VGI_BLOB_VENDOR_URL", raising=False)
    client: Any = blob_client_from_env()
    assert isinstance(client, ObjectStoreBlobClient)
