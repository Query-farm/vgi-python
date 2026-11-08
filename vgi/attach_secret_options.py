# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Framework delivery of ATTACH options, with secret options encrypted.

The worker carries the options of every ``ATTACH`` inside the framework part
of ``attach_opaque_data`` so any function, in any worker process, and the
catalog's transaction hooks can read them back with
[`attach_options`][vgi.catalog.attach_option.attach_options]. Options declared
``secret=True`` are encrypted there (vgi ``SPEC-secret-options``; sealing rule
5: no secret in plaintext in any opaque value, on any transport):

* ``K = HKDF-SHA256(root, salt=b"", info=b"vgi.attach_secret_options.v1", 32)``
  where ``root`` is, in order, an explicitly configured key
  ([`set_attach_secret_options_key`][vgi.attach_secret_options.set_attach_secret_options_key]),
  the worker's signing key when it seals (HTTP), or the host-local key file
  shared by every VGI worker of the user on the host (stdio, unix sockets,
  launcher pools).
* XChaCha20-Poly1305 with a random 24-byte nonce; envelope
  ``version || nonce || ciphertext || tag``. The version byte names the
  algorithm and which root was used.
* AAD ``"vgi.attach_secret_options.v1" 0x00 || attach_uuid``: a blob moved
  into another attach fails to open, with the uniform
  ``attach_opaque_data not recognized`` error.

Framework attach plaintext layout: ``uuid(16) || [section] || catalog_bytes``,
where the optional section is ``_SECTION_MAGIC || u32 length || payload`` and
the payload is ``u8 format || u32 len(plain) || plain_ipc || secret_blob``.
Catalog code and functions never see the section in ``attach_opaque_data``:
[`catalog_view`][vgi.attach_secret_options.catalog_view] strips it and keeps
it on the returned bytes object for the accessor.
"""

from __future__ import annotations

import contextlib
import getpass
import os
import stat
import tempfile
import threading
from pathlib import Path
from typing import Any

import pyarrow as pa

__all__ = [
    "AttachSecretKeyError",
    "host_local_key_path",
    "set_attach_secret_options_key",
]

INFO = b"vgi.attach_secret_options.v1"
_AAD_PREFIX = INFO + b"\x00"
_SECTION_MAGIC = b"VGI\x00AOPT\x01"
_SECTION_FORMAT = 1
_MAX_SECTION = 1 << 20
_UUID_LEN = 16
_NONCE_LEN = 24
_TAG_LEN = 16
_KEY_LEN = 32

# Envelope version byte: algorithm XChaCha20-Poly1305 with the named root.
_V_EXPLICIT = 0x11
_V_SIGNING = 0x12
_V_HOST_LOCAL = 0x13

_lock = threading.Lock()
_explicit_root: bytes | None = None
_signing_root: bytes | None = None
_host_local_cache: dict[str, bytes] = {}


class AttachSecretKeyError(RuntimeError):
    """No key could be obtained to encrypt (or open) secret ATTACH options."""


def set_attach_secret_options_key(key: bytes | None) -> None:
    """Configure the root key for secret ATTACH options explicitly (or clear it).

    Optional. Without it the worker uses its signing key when it seals (HTTP)
    and the host-local key file otherwise. Every worker process that serves
    one deployment must use the same key. The key itself is never used
    directly: an HKDF-SHA256 subkey is.

    Args:
        key: At least 32 bytes of key material, or None to clear.

    Raises:
        ValueError: If the key is shorter than 32 bytes.

    """
    global _explicit_root
    if key is not None and len(key) < _KEY_LEN:
        raise ValueError("the secret-options key must be at least 32 bytes")
    with _lock:
        _explicit_root = bytes(key) if key is not None else None


def _note_signing_key(key: bytes | None) -> None:
    """Remember the worker's sealing key (called by the worker; never logged)."""
    global _signing_root
    if key is not None and key != _signing_root:
        with _lock:
            _signing_root = bytes(key)


def derive_key(root: bytes) -> bytes:
    """HKDF-SHA256 subkey of ``root`` for secret ATTACH options."""
    from Crypto.Hash import SHA256
    from Crypto.Protocol.KDF import HKDF

    key = HKDF(root, _KEY_LEN, b"", SHA256, context=INFO)
    assert isinstance(key, bytes)
    return key


# ---------------------------------------------------------------------------
# Host-local key file (shared path and format across SDKs)
# ---------------------------------------------------------------------------


def host_local_key_path() -> Path:
    """Where the host-local key lives.

    ``$VGI_ATTACH_SECRET_KEY_FILE`` when set and non-empty; else
    ``$XDG_RUNTIME_DIR/vgi/attach-secret-options.key`` when ``XDG_RUNTIME_DIR``
    is a directory; else ``<per-user temp dir>/vgi-<username>/attach-secret-options.key``.

    Returns:
        The key file path (it may not exist yet).

    """
    explicit = os.environ.get("VGI_ATTACH_SECRET_KEY_FILE")
    if explicit:
        return Path(explicit)
    xdg = os.environ.get("XDG_RUNTIME_DIR")
    if xdg and os.path.isdir(xdg):
        return Path(xdg) / "vgi" / "attach-secret-options.key"
    return Path(tempfile.gettempdir()) / f"vgi-{getpass.getuser()}" / "attach-secret-options.key"


def _key_file_error(path: Path, problem: str) -> AttachSecretKeyError:
    return AttachSecretKeyError(
        f"secret ATTACH options need the host-local key file {path}, but {problem}. "
        "Fix or remove it, or configure VGI_SIGNING_KEY or VGI_ATTACH_SECRET_KEY_FILE."
    )


def _check_private(path: Path, st: os.stat_result, what: str) -> None:
    if os.name != "posix":
        return
    if st.st_uid != os.getuid():
        raise _key_file_error(path, f"the {what} is not owned by the current user")
    if st.st_mode & 0o077:
        raise _key_file_error(path, f"the {what} is accessible to group or other users")


def _load_host_local_key() -> bytes:
    path = host_local_key_path()
    cache_key = str(path)
    cached = _host_local_cache.get(cache_key)
    if cached is not None:
        return cached
    directory = path.parent
    try:
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    except OSError as exc:
        raise _key_file_error(path, f"its directory cannot be created ({exc.strerror})") from exc
    if not os.environ.get("VGI_ATTACH_SECRET_KEY_FILE"):
        _check_private(path, directory.stat(), "directory")
    if not path.exists():
        _create_key_file(path)
    try:
        st = path.lstat()
    except OSError as exc:
        raise _key_file_error(path, f"it cannot be read ({exc.strerror})") from exc
    if not stat.S_ISREG(st.st_mode):
        raise _key_file_error(path, "it is not a regular file")
    _check_private(path, st, "file")
    data = path.read_bytes()
    if len(data) != _KEY_LEN:
        raise _key_file_error(path, f"it does not hold exactly {_KEY_LEN} bytes")
    _host_local_cache[cache_key] = data
    return data


def _create_key_file(path: Path) -> None:
    """Create the key atomically: a private temp file hard-linked into place, never overwriting."""
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{os.urandom(6).hex()}.tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(os.urandom(_KEY_LEN))
            f.flush()
            os.fsync(f.fileno())
        try:
            os.link(tmp, path)
        except FileExistsError:
            pass  # another process won the race: use its key
        except OSError:
            # No hard links (some filesystems): exclusive create of the target.
            try:
                fd2 = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            except FileExistsError:
                pass
            else:
                with os.fdopen(fd2, "wb") as f2:
                    f2.write(tmp.read_bytes())
    finally:
        with contextlib.suppress(OSError):
            tmp.unlink()


# ---------------------------------------------------------------------------
# Encryption
# ---------------------------------------------------------------------------


def _root_for_encrypt(signing_key: bytes | None) -> tuple[int, bytes]:
    if _explicit_root is not None:
        return _V_EXPLICIT, _explicit_root
    if signing_key is not None:
        _note_signing_key(signing_key)
        return _V_SIGNING, signing_key
    return _V_HOST_LOCAL, _load_host_local_key()


def _root_for_open(version: int) -> bytes | None:
    if version == _V_EXPLICIT:
        return _explicit_root
    if version == _V_SIGNING:
        return _signing_root
    if version == _V_HOST_LOCAL:
        try:
            return _load_host_local_key()
        except AttachSecretKeyError:
            return None
    return None


def _aad(attach_uuid: bytes) -> bytes:
    return _AAD_PREFIX + attach_uuid


def encrypt(plaintext: bytes, attach_uuid: bytes, *, signing_key: bytes | None) -> bytes:
    """Encrypt secret-option bytes bound to ``attach_uuid`` (``AttachSecretKeyError`` without a key)."""
    from Crypto.Cipher import ChaCha20_Poly1305

    version, root = _root_for_encrypt(signing_key)
    nonce = os.urandom(_NONCE_LEN)
    cipher = ChaCha20_Poly1305.new(key=derive_key(root), nonce=nonce)
    cipher.update(_aad(attach_uuid))
    ciphertext, tag = cipher.encrypt_and_digest(plaintext)
    return bytes([version]) + nonce + ciphertext + tag


def decrypt(blob: bytes, attach_uuid: bytes) -> bytes:
    """Open a blob from :func:`encrypt`; any failure is the uniform ``attach_opaque_data not recognized``."""
    from Crypto.Cipher import ChaCha20_Poly1305

    from vgi.exceptions import OpaqueDataNotRecognizedError

    if len(blob) < 1 + _NONCE_LEN + _TAG_LEN:
        raise OpaqueDataNotRecognizedError("attach_opaque_data")
    root = _root_for_open(blob[0])
    if root is None:
        raise OpaqueDataNotRecognizedError("attach_opaque_data")
    nonce, body = blob[1 : 1 + _NONCE_LEN], blob[1 + _NONCE_LEN :]
    cipher = ChaCha20_Poly1305.new(key=derive_key(root), nonce=nonce)
    cipher.update(_aad(attach_uuid))
    try:
        return bytes(cipher.decrypt_and_verify(body[:-_TAG_LEN], body[-_TAG_LEN:]))
    except ValueError as exc:
        raise OpaqueDataNotRecognizedError("attach_opaque_data") from exc


# ---------------------------------------------------------------------------
# Framework section
# ---------------------------------------------------------------------------


def _ipc(batch: pa.RecordBatch) -> bytes:
    from vgi_rpc.utils import serialize_record_batch_bytes

    return serialize_record_batch_bytes(batch)


def build_section(
    options: pa.RecordBatch | None,
    secret_names: set[str],
    *,
    attach_uuid: bytes,
    signing_key: bytes | None,
) -> bytes:
    """Return the options section for one attach (``b""`` when there are no options).

    Columns whose lower-cased name is in ``secret_names`` are encrypted; the
    rest travel as plain Arrow IPC. Secret columns with a NULL value are left
    out, so an attach that supplies no secret needs no key.
    """
    if options is None or options.num_rows == 0 or options.num_columns == 0:
        return b""
    plain_idx: list[int] = []
    secret_idx: list[int] = []
    for i, name in enumerate(options.schema.names):
        if name.lower() in secret_names:
            if options.column(i)[0].is_valid:
                secret_idx.append(i)
        else:
            plain_idx.append(i)
    plain = _ipc(options.select(plain_idx)) if plain_idx else b""
    secret = encrypt(_ipc(options.select(secret_idx)), attach_uuid, signing_key=signing_key) if secret_idx else b""
    payload = bytes([_SECTION_FORMAT]) + len(plain).to_bytes(4, "big") + plain + secret
    if len(payload) > _MAX_SECTION:
        raise ValueError("ATTACH options too large to carry in attach_opaque_data")
    return _SECTION_MAGIC + len(payload).to_bytes(4, "big") + payload


def split(plaintext: bytes) -> tuple[bytes, bytes | None, bytes]:
    """Split a framework attach plaintext into ``(uuid, section payload or None, catalog bytes)``."""
    uuid, rest = plaintext[:_UUID_LEN], plaintext[_UUID_LEN:]
    if not rest.startswith(_SECTION_MAGIC):
        return uuid, None, rest
    from vgi.exceptions import OpaqueDataNotRecognizedError

    at = len(_SECTION_MAGIC)
    if len(rest) < at + 4:
        raise OpaqueDataNotRecognizedError("attach_opaque_data")
    size = int.from_bytes(rest[at : at + 4], "big")
    at += 4
    if not 5 <= size <= _MAX_SECTION or len(rest) < at + size:
        raise OpaqueDataNotRecognizedError("attach_opaque_data")
    return uuid, rest[at : at + size], rest[at + size :]


class CatalogAttachBytes(bytes):
    """The catalog's ``attach_opaque_data`` bytes, plus the framework's options for the accessor.

    Compares, hashes and serializes exactly as the catalog's own bytes; the
    options section rides along only in memory and is never part of the value.
    """

    _vgi_uuid: bytes
    _vgi_section: bytes

    def __new__(cls, catalog: bytes, uuid: bytes, section: bytes) -> CatalogAttachBytes:  # noqa: D102
        obj = super().__new__(cls, catalog)
        obj._vgi_uuid = uuid
        obj._vgi_section = section
        return obj

    def __reduce__(self) -> Any:  # pickles (and copies) as plain bytes: options never travel
        return (bytes, (bytes(self),))


def catalog_view(plaintext: bytes | None) -> bytes | None:
    """The catalog bytes of a framework plaintext, carrying its options section (if any)."""
    if plaintext is None:
        return None
    uuid, section, catalog = split(plaintext)
    if section is None:
        return catalog
    return CatalogAttachBytes(catalog, uuid, section)


def options_of(attach_opaque_data: bytes | None) -> dict[str, Any]:
    """Decode the framework-carried options (secret ones decrypted); ``{}`` when none."""
    if not isinstance(attach_opaque_data, CatalogAttachBytes):
        return {}
    from vgi_rpc.utils import deserialize_record_batch

    from vgi.exceptions import OpaqueDataNotRecognizedError

    payload = attach_opaque_data._vgi_section
    try:
        if payload[0] != _SECTION_FORMAT:
            raise ValueError("unknown section format")
        plain_len = int.from_bytes(payload[1:5], "big")
        plain = payload[5 : 5 + plain_len]
        secret = payload[5 + plain_len :]
        values: dict[str, Any] = {}
        if plain:
            batch, _ = deserialize_record_batch(plain)
            values.update({k.lower(): v for k, v in batch.to_pylist()[0].items()})
    except (ValueError, IndexError, pa.ArrowException) as exc:
        raise OpaqueDataNotRecognizedError("attach_opaque_data") from exc
    if secret:
        opened = decrypt(secret, attach_opaque_data._vgi_uuid)
        try:
            batch, _ = deserialize_record_batch(opened)
        except (ValueError, pa.ArrowException) as exc:
            raise OpaqueDataNotRecognizedError("attach_opaque_data") from exc
        values.update({k.lower(): v for k, v in batch.to_pylist()[0].items()})
    return values
