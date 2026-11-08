# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Secret ATTACH options travel encrypted in attach_opaque_data (SPEC-secret-options)."""

from __future__ import annotations

import hashlib
import hmac
import multiprocessing
import os
import stat
import sys
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from vgi import attach_secret_options as aso
from vgi._test_fixtures.writable_tx import TxfixWorker
from vgi.catalog import attach_options
from vgi.exceptions import OpaqueDataNotRecognizedError
from vgi.protocol import CatalogAttachRequest

posix_only = pytest.mark.skipif(os.name != "posix", reason="POSIX permission semantics")


@pytest.fixture(autouse=True)
def _private_key_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the host-local key at a per-test file."""
    path = tmp_path / "keys" / "attach-secret-options.key"
    monkeypatch.setenv("VGI_ATTACH_SECRET_KEY_FILE", str(path))
    aso._host_local_cache.clear()
    aso.set_attach_secret_options_key(None)
    return path


def _attach(worker: TxfixWorker, **options: Any) -> bytes:
    batch = pa.RecordBatch.from_pylist([options]) if options else None
    result = worker.catalog_attach(
        CatalogAttachRequest(name="txfix", options=batch, data_version_spec=None, implementation_version=None)
    )
    assert result.attach_opaque_data is not None
    return bytes(result.attach_opaque_data)


def _hkdf_reference(root: bytes) -> bytes:
    prk = hmac.new(b"\x00" * 32, root, hashlib.sha256).digest()  # empty salt = HashLen zeros
    return hmac.new(prk, b"vgi.attach_secret_options.v1" + b"\x01", hashlib.sha256).digest()


def test_hkdf_subkey_not_root() -> None:
    """The AEAD key is HKDF-SHA256(root, info="vgi.attach_secret_options.v1"), never the root."""
    root = os.urandom(32)
    key = aso.derive_key(root)
    assert key == _hkdf_reference(root)
    assert key != root


def test_stdio_secret_absent_and_readable() -> None:
    """Without a signing key the raw attach bytes never contain the secret; the accessor returns it."""
    worker = TxfixWorker(quiet=True)
    raw = _attach(worker, token="tok-stdio-secret", transactional=True)
    assert b"tok-stdio-secret" not in raw
    view = worker._unwrap_attach(raw)
    assert b"tok-stdio-secret" not in bytes(view)
    assert attach_options(view) == {"token": "tok-stdio-secret", "transactional": True}
    # The envelope says the host-local root was used.
    _, section, _ = aso.split(raw)
    assert section is not None and section[5 + int.from_bytes(section[1:5], "big")] == aso._V_HOST_LOCAL


def test_http_sealed_secret_absent_and_readable(_private_key_file: Path) -> None:
    """With a signing key the opened inner bytes never contain the secret, and no host key file is used."""
    worker = TxfixWorker(quiet=True)
    worker._signing_key = os.urandom(32)
    sealed = _attach(worker, token="tok-http-secret")
    assert b"tok-http-secret" not in sealed
    inner = worker._unwrap_attach_full(sealed)
    assert inner is not None and b"tok-http-secret" not in inner
    assert attach_options(worker._unwrap_attach(sealed))["token"] == "tok-http-secret"
    assert not _private_key_file.exists()


def test_blob_bound_to_its_attach() -> None:
    """A secret section moved into another attach fails with the uniform error."""
    worker = TxfixWorker(quiet=True)
    a = _attach(worker, token="tok-a")
    b = _attach(worker, token="tok-b")
    uuid_a, section_a, catalog_a = aso.split(a)
    uuid_b, _, _ = aso.split(b)
    assert section_a is not None
    moved = uuid_b + aso._SECTION_MAGIC + len(section_a).to_bytes(4, "big") + section_a + catalog_a
    with pytest.raises(OpaqueDataNotRecognizedError, match="attach_opaque_data not recognized"):
        attach_options(worker._unwrap_attach(moved))
    tampered = bytearray(a)
    tampered[-len(catalog_a) - 1] ^= 0x01
    with pytest.raises(OpaqueDataNotRecognizedError):
        attach_options(worker._unwrap_attach(bytes(tampered)))


def test_no_secret_needs_no_key(_private_key_file: Path) -> None:
    """Non-secret options travel plain and never touch the key file."""
    worker = TxfixWorker(quiet=True)
    raw = _attach(worker, transactional=True)
    assert attach_options(worker._unwrap_attach(raw)) == {"transactional": True}
    assert not _private_key_file.exists()
    assert attach_options(worker._unwrap_attach(_attach(worker))) == {}


def test_options_never_travel_with_copies() -> None:
    """Copying or pickling the catalog bytes drops the options (they are never part of the value)."""
    import copy
    import pickle

    worker = TxfixWorker(quiet=True)
    view = worker._unwrap_attach(_attach(worker, token="tok-copy"))
    assert attach_options(pickle.loads(pickle.dumps(view))) == {}
    assert attach_options(copy.copy(view)) == {}
    assert attach_options(bytes(view)) == {}


def test_explicit_key_preferred(_private_key_file: Path) -> None:
    """An explicitly configured key wins over the host-local file."""
    aso.set_attach_secret_options_key(os.urandom(32))
    worker = TxfixWorker(quiet=True)
    raw = _attach(worker, token="tok-explicit")
    assert attach_options(worker._unwrap_attach(raw))["token"] == "tok-explicit"
    assert not _private_key_file.exists()
    aso.set_attach_secret_options_key(None)
    with pytest.raises(ValueError, match="32 bytes"):
        aso.set_attach_secret_options_key(b"short")


# ---------------------------------------------------------------------------
# Host-local key file
# ---------------------------------------------------------------------------


@posix_only
def test_default_path_and_modes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """XDG_RUNTIME_DIR/vgi/attach-secret-options.key: dir 0700, file 0600, 32 bytes, reused."""
    monkeypatch.delenv("VGI_ATTACH_SECRET_KEY_FILE")
    runtime = tmp_path / "run"
    runtime.mkdir()
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(runtime))
    path = aso.host_local_key_path()
    assert path == runtime / "vgi" / "attach-secret-options.key"
    key = aso._load_host_local_key()
    assert len(key) == 32
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    aso._host_local_cache.clear()
    assert aso._load_host_local_key() == key == path.read_bytes()


def test_temp_dir_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without the env vars: <temp>/vgi-<user>/attach-secret-options.key."""
    import getpass
    import tempfile

    monkeypatch.delenv("VGI_ATTACH_SECRET_KEY_FILE")
    monkeypatch.delenv("XDG_RUNTIME_DIR", raising=False)
    assert aso.host_local_key_path() == Path(tempfile.gettempdir()) / f"vgi-{getpass.getuser()}" / (
        "attach-secret-options.key"
    )


def _load_in_child(path: str, queue: Any) -> None:
    os.environ["VGI_ATTACH_SECRET_KEY_FILE"] = path
    queue.put(aso._load_host_local_key())


@pytest.mark.skipif(sys.platform == "win32", reason="fork-based concurrency")
def test_concurrent_creation_agrees(_private_key_file: Path) -> None:
    """Concurrent first starts all end up with the same key."""
    ctx = multiprocessing.get_context("fork")
    queue = ctx.Queue()
    procs = [ctx.Process(target=_load_in_child, args=(str(_private_key_file), queue)) for _ in range(8)]
    for proc in procs:
        proc.start()
    keys = {queue.get(timeout=30) for _ in procs}
    for proc in procs:
        proc.join(timeout=30)
    assert len(keys) == 1
    assert keys == {_private_key_file.read_bytes()}


@posix_only
def test_bad_permissions_refused(_private_key_file: Path) -> None:
    """A group/other-readable key file is refused, naming the file and the alternatives."""
    aso._load_host_local_key()
    aso._host_local_cache.clear()
    _private_key_file.chmod(0o644)
    with pytest.raises(aso.AttachSecretKeyError, match="VGI_ATTACH_SECRET_KEY_FILE") as exc:
        aso._load_host_local_key()
    assert str(_private_key_file) in str(exc.value)


@posix_only
def test_bad_directory_permissions_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A group/other-accessible key directory is refused."""
    monkeypatch.delenv("VGI_ATTACH_SECRET_KEY_FILE")
    runtime = tmp_path / "run"
    (runtime / "vgi").mkdir(parents=True)
    (runtime / "vgi").chmod(0o755)
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(runtime))
    with pytest.raises(aso.AttachSecretKeyError, match="directory"):
        aso._load_host_local_key()


def test_wrong_size_refused(_private_key_file: Path) -> None:
    """A key file that is not exactly 32 bytes is refused."""
    _private_key_file.parent.mkdir(mode=0o700, parents=True)
    fd = os.open(_private_key_file, os.O_WRONLY | os.O_CREAT, 0o600)
    os.write(fd, b"x" * 31)
    os.close(fd)
    with pytest.raises(aso.AttachSecretKeyError, match="32 bytes"):
        aso._load_host_local_key()


def test_attach_fails_clearly_without_key(_private_key_file: Path) -> None:
    """A secret with a value but no obtainable key fails the ATTACH with a clear message."""
    _private_key_file.parent.mkdir(mode=0o700, parents=True)
    _private_key_file.write_bytes(b"bad")
    worker = TxfixWorker(quiet=True)
    with pytest.raises(aso.AttachSecretKeyError, match="host-local key file"):
        _attach(worker, token="tok-nokey")
