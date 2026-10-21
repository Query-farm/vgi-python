"""Opt-in publication of immutable catalog snapshots through vgi-rpc.

Repositories hold trusted discovery manifests and immutable RPC result objects.
Scope decisions remain with the catalog; every worker call authenticates before
consulting this manager. A manifest is never served before HTTP readback has
validated both the RPC envelope and all nested catalog collections.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
import weakref
from collections.abc import Callable
from dataclasses import asdict, dataclass, field, replace
from typing import Any, Literal, Protocol

import pyarrow as pa
from vgi_rpc.external import (
    ClientExternalConfig,
    Compression,
    ExternalRef,
    ExternalStorage,
    publish_external,
)
from vgi_rpc.external_fetch import FetchConfig, fetch_url, redact_url
from vgi_rpc.metadata import LOCATION_KEY, LOG_LEVEL_KEY
from vgi_rpc.rpc import rpc_methods
from vgi_rpc.utils import new_ipc_stream

from vgi.catalog.contents_codec import decode_collection, decode_inventory
from vgi.catalog.contract import catalog_contract_hash
from vgi.protocol import CatalogContentsResponse, VgiProtocol

_MANIFEST_VERSION = 1
_CODEC_REVISION = "catalog-collections-1"


@dataclass(frozen=True, kw_only=True)
class SnapshotIdentity:
    """Cheap metadata generation identity; null scope defaults to this attachment.

    source_revision must change whenever metadata changes. Explicitly shared
    visibility_scope is a declaration that callers see identical metadata.
    Never put credentials into either field.
    """

    catalog_namespace: str
    source_revision: str
    catalog_version: int
    visibility_scope: str | None = None


@dataclass(frozen=True, kw_only=True)
class SnapshotRequest:
    """Authorized request passed to the catalog's identity and validator hooks."""

    catalog: Any
    attach_opaque_data: bytes = field(repr=False)
    attachment_scope: str
    catalog_version: int


@dataclass(frozen=True, kw_only=True)
class PublicationLocation:
    """Lifetime guarantees supplied by an uploader, not inferred from its URL."""

    object_id: str | None = None
    expires_at: float | None = None
    retain_until: float | None = None
    stable: bool = False


@dataclass(frozen=True, kw_only=True)
class CatalogPublicationManifest:
    """Trusted discovery record; signed URLs are credentials and are not logged."""

    cache_key: str
    manifest_id: str
    protocol_name: str
    wire_revision: str
    contract_hash: str
    codec_revision: str
    catalog_version: int
    etag: str | None
    sha256: str
    raw_size: int
    stored_size: int
    url: str = field(repr=False)
    object_id: str | None = None
    expires_at: float | None = None
    retain_until: float | None = None
    stable: bool = False
    format_version: int = _MANIFEST_VERSION
    validator_revision: int = _MANIFEST_VERSION

    @property
    def reference(self) -> ExternalRef:
        """Construct the vgi-rpc reference with its required integrity digest."""
        return ExternalRef(self.url, self.sha256)


class CatalogPublicationRepository(Protocol):
    """Thread-safe discovery, immutable upload and conditional manifest writes."""

    storage: ExternalStorage
    profile: str

    def lookup(self, key: str) -> CatalogPublicationManifest | None:
        """Read the current discovery manifest, or return None."""
        ...

    def commit_if_absent(self, manifest: CatalogPublicationManifest) -> CatalogPublicationManifest:
        """Atomically install a manifest or return the winning publication."""
        ...

    def invalidate(self, key: str, manifest_id: str) -> bool:
        """Invalidate only the specified manifest generation."""
        ...

    def location(self, reference: ExternalRef) -> PublicationLocation:
        """Describe the uploaded object and its declared lifetime."""
        ...

    def renew(self, manifest: CatalogPublicationManifest) -> CatalogPublicationManifest | None:
        """Renew a retrieval reference without rebuilding metadata when supported."""
        ...


class MemoryCatalogPublicationRepository:
    """In-process manifests with a supplied HTTP-accessible RPC storage adapter.

    URL lifetime or stable-location guarantees must be explicit. Durable
    recovery requires a durable repository, such as the S3 adapter below.
    """

    def __init__(
        self,
        storage: ExternalStorage,
        *,
        profile: str,
        stable_urls: bool = False,
        url_ttl_seconds: float | None = None,
        retention_seconds: float | None = None,
    ):
        """Configure the repository or publication manager."""
        if not stable_urls and (url_ttl_seconds is None or url_ttl_seconds <= 0):
            raise ValueError("declare stable URLs or a positive URL lifetime")
        self.storage = storage
        self.profile = profile
        self.stable_urls = stable_urls
        self.url_ttl_seconds = url_ttl_seconds
        self.retention_seconds = retention_seconds
        self._manifests: dict[str, CatalogPublicationManifest] = {}
        self._lock = threading.Lock()

    def lookup(self, key: str) -> CatalogPublicationManifest | None:
        """Read the current discovery manifest, or return None."""
        with self._lock:
            return self._manifests.get(key)

    def commit_if_absent(self, manifest: CatalogPublicationManifest) -> CatalogPublicationManifest:
        """Atomically install a manifest or return the winning publication."""
        with self._lock:
            return self._manifests.setdefault(manifest.cache_key, manifest)

    def invalidate(self, key: str, manifest_id: str) -> bool:
        """Invalidate only the specified manifest generation."""
        with self._lock:
            old = self._manifests.get(key)
            if old is None or old.manifest_id != manifest_id:
                return False
            del self._manifests[key]
            return True

    def location(self, reference: ExternalRef) -> PublicationLocation:
        """Describe the uploaded object and its declared lifetime."""
        del reference
        now = time.time()
        return PublicationLocation(
            stable=self.stable_urls,
            expires_at=now + self.url_ttl_seconds if self.url_ttl_seconds else None,
            retain_until=now + self.retention_seconds if self.retention_seconds else None,
        )

    def renew(self, manifest: CatalogPublicationManifest) -> CatalogPublicationManifest | None:
        """Renew a retrieval reference without rebuilding metadata when supported."""
        del manifest
        return None


class S3CatalogPublicationRepository:
    """Durable S3/R2 adapter using an injected boto3-compatible S3 client.

    The client must support conditional PutObject (IfNoneMatch and IfMatch).
    Configure bucket lifecycle to retain objects at least retention_seconds;
    manifests use a separate prefix and are conditionally tombstoned. No
    credentials or signed URLs occur in object keys.
    """

    def __init__(
        self,
        client: Any,
        *,
        bucket: str,
        prefix: str = "vgi-catalog/",
        profile: str,
        url_ttl_seconds: int = 3600,
        retention_seconds: int = 86400,
    ):
        """Configure the repository or publication manager."""
        if url_ttl_seconds <= 0 or retention_seconds <= url_ttl_seconds:
            raise ValueError("retention must exceed a positive URL lifetime")
        self.client, self.bucket, self.prefix = client, bucket, prefix.rstrip("/") + "/"
        self.profile, self.url_ttl_seconds, self.retention_seconds = profile, url_ttl_seconds, retention_seconds
        self.storage = self
        self._locations: dict[str, PublicationLocation] = {}
        self._lock = threading.Lock()

    def _key(self, key: str) -> str:
        return self.prefix + "manifests/" + key + ".json"

    @staticmethod
    def _status(error: Exception) -> str:
        return str(getattr(error, "response", {}).get("Error", {}).get("Code", ""))

    def _load(self, key: str) -> tuple[CatalogPublicationManifest | None, str | None]:
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self._key(key))
        except Exception as error:
            if self._status(error) in ("NoSuchKey", "404", "NotFound"):
                return None, None
            raise
        body = response["Body"]
        try:
            data = body.read(1024 * 1024 + 1)
        finally:
            body.close()
        if len(data) > 1024 * 1024:
            raise ValueError("catalog publication manifest exceeds size limit")
        obj = json.loads(data)
        return (None if obj.get("tombstone") else CatalogPublicationManifest(**obj), response["ETag"])

    def lookup(self, key: str) -> CatalogPublicationManifest | None:
        """Read the current discovery manifest, or return None."""
        return self._load(key)[0]

    def commit_if_absent(self, manifest: CatalogPublicationManifest) -> CatalogPublicationManifest:
        """Atomically install a manifest or return the winning publication."""
        for _ in range(8):
            current, etag = self._load(manifest.cache_key)
            if current is not None:
                return current
            condition = {"IfMatch": etag} if etag else {"IfNoneMatch": "*"}
            try:
                self.client.put_object(
                    Bucket=self.bucket,
                    Key=self._key(manifest.cache_key),
                    Body=json.dumps(asdict(manifest), sort_keys=True, separators=(",", ":")).encode(),
                    ContentType="application/json",
                    **condition,
                )
                return manifest
            except Exception as error:
                if self._status(error) not in ("PreconditionFailed", "412", "ConditionalRequestConflict", "409"):
                    raise
        raise RuntimeError("catalog publication manifest contention")

    def invalidate(self, key: str, manifest_id: str) -> bool:
        """Invalidate only the specified manifest generation."""
        current, etag = self._load(key)
        if current is None or current.manifest_id != manifest_id:
            return False
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=self._key(key),
                Body=b'{"tombstone":true}',
                ContentType="application/json",
                IfMatch=etag,
            )
            return True
        except Exception as error:
            if self._status(error) in ("PreconditionFailed", "412", "ConditionalRequestConflict", "409"):
                return False
            raise

    def upload(self, data: bytes, schema: pa.Schema, *, content_encoding: str | None = None) -> str:
        """Upload an immutable RPC object and preserve its content encoding."""
        del schema
        key = self.prefix + "objects/" + uuid.uuid4().hex + ".arrow"
        args = {"ContentEncoding": content_encoding} if content_encoding else {}
        self.client.put_object(
            Bucket=self.bucket, Key=key, Body=data, ContentType="application/vnd.apache.arrow.stream", **args
        )
        url = self.client.generate_presigned_url(
            "get_object", Params={"Bucket": self.bucket, "Key": key}, ExpiresIn=self.url_ttl_seconds
        )
        now = time.time()
        with self._lock:
            self._locations[url] = PublicationLocation(
                object_id=key, expires_at=now + self.url_ttl_seconds, retain_until=now + self.retention_seconds
            )
        return str(url)

    def location(self, reference: ExternalRef) -> PublicationLocation:
        """Describe the uploaded object and its declared lifetime."""
        with self._lock:
            return self._locations.pop(reference.url)

    def renew(self, manifest: CatalogPublicationManifest) -> CatalogPublicationManifest | None:
        """Conditionally persist a renewed URL for the same immutable publication."""
        for _ in range(8):
            current, etag = self._load(manifest.cache_key)
            if current is None or current.manifest_id != manifest.manifest_id:
                return None
            if current.object_id is None or current.retain_until is None:
                return None
            now = time.time()
            lifetime = min(self.url_ttl_seconds, int(current.retain_until - now))
            if lifetime <= 60:
                return None
            url = self.client.generate_presigned_url(
                "get_object", Params={"Bucket": self.bucket, "Key": current.object_id}, ExpiresIn=lifetime
            )
            renewed = replace(current, url=url, expires_at=now + lifetime)
            try:
                self.client.put_object(
                    Bucket=self.bucket,
                    Key=self._key(current.cache_key),
                    Body=json.dumps(asdict(renewed), sort_keys=True, separators=(",", ":")).encode(),
                    ContentType="application/json",
                    IfMatch=etag,
                )
                return renewed
            except Exception as error:
                if self._status(error) not in ("PreconditionFailed", "412", "ConditionalRequestConflict", "409"):
                    raise
        raise RuntimeError("catalog publication reference renewal contention")


@dataclass(frozen=True, kw_only=True)
class CatalogSnapshotPolicy:
    """Framework delivery configuration; ordinary delivery remains the default."""

    delivery: Literal["ordinary", "published"] = "ordinary"
    repository: CatalogPublicationRepository | None = None
    identity: Callable[[SnapshotRequest], SnapshotIdentity] | None = None
    validator: Callable[[SnapshotRequest], str | None] | None = None
    on_publication_failure: Literal["error", "ordinary"] = "error"
    compression: Compression | None = None
    fetch_config: ClientExternalConfig | None = None
    max_raw_bytes: int = 256 * 1024 * 1024
    max_stored_bytes: int = 256 * 1024 * 1024
    expiration_margin_seconds: float = 30
    max_cached_manifests: int = 256

    def __post_init__(self) -> None:
        """Reject incomplete publication policies and invalid limits."""
        if self.delivery not in ("ordinary", "published") or self.on_publication_failure not in ("error", "ordinary"):
            raise ValueError("invalid catalog snapshot delivery/failure policy")
        if self.delivery == "published" and (self.repository is None or self.identity is None):
            raise ValueError("published catalog snapshots require a repository and cheap identity hook")
        if (
            self.max_raw_bytes <= 0
            or self.max_stored_bytes <= 0
            or self.expiration_margin_seconds < 0
            or self.max_cached_manifests <= 0
        ):
            raise ValueError("invalid catalog snapshot size/lifetime limits")


class CatalogSnapshotManager:
    """One-process singleflight and validated publication cache.

    Construct once per worker. A new manager recovers repository manifests,
    performs one HTTP validation, and never rebuilds a valid publication.
    """

    def __init__(self, policy: CatalogSnapshotPolicy):
        """Configure the repository or publication manager."""
        if policy.repository is None or policy.identity is None:
            raise ValueError("a publication manager requires repository and identity")
        self.policy = policy
        self.repository = policy.repository
        self._validated: dict[str, CatalogPublicationManifest] = {}
        self._locks: weakref.WeakValueDictionary[str, threading.RLock] = weakref.WeakValueDictionary()
        self._mutex = threading.Lock()
        self.statistics = {"builds": 0, "uploads": 0, "fetches": 0}
        self._fetch = policy.fetch_config or ClientExternalConfig(
            fetch_config=FetchConfig(
                max_fetch_bytes=policy.max_stored_bytes, max_decompressed_bytes=policy.max_raw_bytes
            )
        )

    def close(self) -> None:
        """Close the HTTP pool when this manager owns it."""
        if self.policy.fetch_config is None:
            self._fetch.fetch_config.close()

    def _lock(self, key: str) -> threading.RLock:
        with self._mutex:
            return self._locks.setdefault(key, threading.RLock())

    def _remember(self, key: str, manifest: CatalogPublicationManifest) -> None:
        with self._mutex:
            self._validated.pop(key, None)
            while len(self._validated) >= self.policy.max_cached_manifests:
                self._validated.pop(next(iter(self._validated)))
            self._validated[key] = manifest

    def cache_key(self, request: SnapshotRequest) -> str:
        """Resolve the cheap identity and hash the wire, scope and storage profile."""
        assert self.policy.identity is not None
        identity = self.policy.identity(request)
        if not identity.catalog_namespace or not identity.source_revision:
            raise ValueError("publication requires nonempty namespace and source revision")
        if identity.catalog_version != request.catalog_version:
            raise ValueError("publication identity version differs from the catalog version")
        compression = asdict(self.policy.compression) if self.policy.compression else None
        fields = [
            VgiProtocol.protocol_name,
            ".".join(VgiProtocol.protocol_version.split(".")[:2]),
            catalog_contract_hash(),
            _CODEC_REVISION,
            identity.catalog_namespace,
            identity.source_revision,
            identity.visibility_scope if identity.visibility_scope is not None else request.attachment_scope,
            str(identity.catalog_version),
            self.repository.profile,
            compression,
        ]
        return hashlib.sha256(
            json.dumps(fields, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def _check_manifest(self, manifest: CatalogPublicationManifest, key: str, version: int) -> None:
        if (
            manifest.cache_key != key
            or manifest.catalog_version != version
            or manifest.protocol_name != VgiProtocol.protocol_name
            or manifest.wire_revision != ".".join(VgiProtocol.protocol_version.split(".")[:2])
            or manifest.contract_hash != catalog_contract_hash()
            or manifest.codec_revision != _CODEC_REVISION
            or manifest.format_version != _MANIFEST_VERSION
            or manifest.validator_revision != _MANIFEST_VERSION
        ):
            raise ValueError("catalog publication manifest belongs to an incompatible contract or scope")
        if not (
            0 < manifest.raw_size <= self.policy.max_raw_bytes
            and 0 < manifest.stored_size <= self.policy.max_stored_bytes
        ):
            raise ValueError("catalog publication exceeds configured size limits")
        if not manifest.stable and manifest.expires_at is None:
            raise ValueError("catalog publication has no declared reference lifetime")
        _ = manifest.reference  # Validate digest and URL before fetching.

    def _usable(self, manifest: CatalogPublicationManifest) -> bool:
        deadline = time.time() + self.policy.expiration_margin_seconds
        return (
            manifest.expires_at is None
            and manifest.stable
            or manifest.expires_at is not None
            and manifest.expires_at > deadline
        ) and (manifest.retain_until is None or manifest.retain_until > deadline)

    def _readback(self, manifest: CatalogPublicationManifest, expected: bytes | None = None) -> None:
        import aiohttp
        from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_fixed

        schema = rpc_methods(VgiProtocol)["catalog_contents"].result_schema
        self.statistics["fetches"] += 1
        # FetchConfig is frozen. A bounded copy owns a short-lived readback
        # pool, closed below; the caller's configuration and pool stay intact.
        source_limits = self._fetch.fetch_config
        encoded_limit = min(source_limits.max_fetch_bytes, self.policy.max_stored_bytes)
        decoded_limit = min(
            source_limits.max_decompressed_bytes
            if source_limits.max_decompressed_bytes is not None
            else 16 * encoded_limit,
            self.policy.max_raw_bytes,
        )
        limits = replace(source_limits, max_fetch_bytes=encoded_limit, max_decompressed_bytes=decoded_limit)
        try:
            retryer = Retrying(
                stop=stop_after_attempt(min(self._fetch.max_retries, 2) + 1),
                wait=wait_fixed(self._fetch.retry_delay_seconds),
                retry=retry_if_exception_type((OSError, aiohttp.ClientError)),
                reraise=True,
            )
            raw = retryer(fetch_url, manifest.url, limits, url_validator=self._fetch.url_validator)
        except (OSError, aiohttp.ClientError):
            raise RuntimeError(f"Catalog publication readback failed: {redact_url(manifest.url)}") from None
        finally:
            limits.close()
        if len(raw) != manifest.raw_size:
            raise ValueError("catalog publication raw byte length differs from its manifest")
        if hashlib.sha256(raw).hexdigest() != manifest.sha256:
            raise ValueError("catalog publication SHA-256 checksum differs from its manifest")
        source = pa.BufferReader(raw)
        with pa.ipc.open_stream(source) as reader:
            batches = list(reader.iter_batches_with_custom_metadata())
        if source.tell() != len(raw):
            raise ValueError("catalog publication has trailing bytes after its outer IPC stream")
        if len(batches) != 1:
            raise ValueError("catalog publication must contain exactly one unary result batch")
        resolved, metadata = batches[0]
        resolved.validate(full=True)
        if metadata is not None and (metadata.get(LOCATION_KEY) is not None or metadata.get(LOG_LEVEL_KEY) is not None):
            raise ValueError("catalog publication must contain a result, not a pointer or log batch")
        if resolved.num_rows != 1 or not resolved.schema.equals(schema, check_metadata=False):
            raise ValueError("catalog publication must contain exactly one unary result row")
        value = resolved.column(0)[0].as_py()
        if not isinstance(value, bytes) or (expected is not None and value != expected):
            raise ValueError("catalog publication readback differs from the encoded inventory")
        decoded = decode_collection(CatalogContentsResponse, value)
        if len(decoded) != 1:
            raise ValueError("catalog publication response must contain exactly one row")
        response = decoded[0]
        if (
            response.not_modified
            or response.catalog_version != manifest.catalog_version
            or response.etag != manifest.etag
        ):
            raise ValueError("catalog publication manifest does not describe its full response")
        decode_inventory(response)

    def _renew(self, manifest: CatalogPublicationManifest) -> CatalogPublicationManifest | None:
        renewed = self.repository.renew(manifest)
        if renewed is None:
            return None
        self._check_manifest(renewed, manifest.cache_key, manifest.catalog_version)
        expected = replace(manifest, url=renewed.url, expires_at=renewed.expires_at)
        if renewed != expected:
            raise ValueError("reference renewal changed immutable publication metadata")
        return renewed if self._usable(renewed) else None

    def _recover(self, manifest: CatalogPublicationManifest, key: str, version: int) -> CatalogPublicationManifest:
        self._check_manifest(manifest, key, version)
        if not self._usable(manifest):
            renewed = self._renew(manifest)
            if renewed is None:
                raise ValueError("catalog publication location or retention expired")
            manifest = renewed
        self._readback(manifest)
        self._remember(key, manifest)
        return manifest

    def _publish(self, key: str, response: CatalogContentsResponse) -> CatalogPublicationManifest:
        if response.not_modified:
            raise ValueError("only a full catalog snapshot can be published")
        decode_inventory(response)
        inner = response.serialize_to_bytes()
        schema = rpc_methods(VgiProtocol)["catalog_contents"].result_schema
        batch = pa.RecordBatch.from_pydict({"result": [inner]}, schema=schema)
        sink = pa.BufferOutputStream()
        with new_ipc_stream(sink, schema) as writer:
            writer.write_batch(batch)
        raw_size = sink.tell()
        if raw_size > self.policy.max_raw_bytes:
            raise ValueError("catalog publication exceeds raw size limit before upload")
        manager = self
        stored_size = 0

        class LimitedStorage:
            def upload(self, data: bytes, schema: pa.Schema, *, content_encoding: str | None = None) -> str:
                """Upload an immutable RPC object and preserve its content encoding."""
                nonlocal stored_size
                stored_size = len(data)
                if stored_size > manager.policy.max_stored_bytes:
                    raise ValueError("catalog publication exceeds stored size limit before upload")
                manager.statistics["uploads"] += 1
                return manager.repository.storage.upload(data, schema, content_encoding=content_encoding)

        reference = publish_external(batch, LimitedStorage(), self.policy.compression)
        location = self.repository.location(reference)
        manifest = CatalogPublicationManifest(
            cache_key=key,
            manifest_id=uuid.uuid4().hex,
            protocol_name=VgiProtocol.protocol_name,
            wire_revision=".".join(VgiProtocol.protocol_version.split(".")[:2]),
            contract_hash=catalog_contract_hash(),
            codec_revision=_CODEC_REVISION,
            catalog_version=response.catalog_version,
            etag=response.etag,
            sha256=reference.sha256 or "",
            raw_size=raw_size,
            stored_size=stored_size,
            url=reference.url,
            **asdict(location),
        )
        self._check_manifest(manifest, key, response.catalog_version)
        if not self._usable(manifest):
            raise ValueError("new catalog publication expires too soon")
        self._readback(manifest, expected=inner)
        winner = self.repository.commit_if_absent(manifest)
        if winner.manifest_id != manifest.manifest_id:
            return self._recover(winner, key, response.catalog_version)
        self._remember(key, manifest)
        return manifest

    def resolve(
        self, request: SnapshotRequest, build: Callable[[], CatalogContentsResponse], if_none_match: str | None = None
    ) -> CatalogContentsResponse | ExternalRef:
        """Resolve after authorization; warm hits perform no object GET or serialization."""
        if self.policy.validator is not None and if_none_match is not None:
            etag = self.policy.validator(request)
            if etag is not None and etag == if_none_match:
                return CatalogContentsResponse(catalog_version=request.catalog_version, etag=etag, not_modified=True)
        key = self.cache_key(request)
        with self._lock(key):
            manifest = self._validated.get(key)
            if manifest is not None and not self._usable(manifest):
                renewed = self._renew(manifest)
                if renewed is not None:
                    if renewed.url != manifest.url:
                        self._readback(renewed)
                    manifest = renewed
                    self._remember(key, manifest)
                else:
                    self.invalidate(key, manifest.manifest_id)
                    manifest = None
            if manifest is None:
                candidate = self.repository.lookup(key)
                if candidate is not None:
                    try:
                        manifest = self._recover(candidate, key, request.catalog_version)
                    except Exception:
                        # A failed object GET invalidates only the descriptor we
                        # inspected. A concurrently installed replacement survives.
                        self.invalidate(key, candidate.manifest_id)
                if manifest is None:
                    self.statistics["builds"] += 1
                    response = build()
                    if response.catalog_version != request.catalog_version:
                        raise ValueError("catalog changed while building its publication")
                    if self.cache_key(request) != key:
                        raise ValueError("catalog generation changed while building its publication")
                    manifest = self._publish(key, response)
            # Lookup, readback, renewal and conditional commit can all block.
            # A publication may remain valid under its old immutable key, but
            # it must not answer this request after its generation changes.
            if self.cache_key(request) != key:
                raise ValueError("catalog generation changed while resolving its publication")
            if if_none_match is not None and manifest.etag == if_none_match:
                return CatalogContentsResponse(
                    catalog_version=request.catalog_version, etag=manifest.etag, not_modified=True
                )
            return manifest.reference

    def warm(self, request: SnapshotRequest, build: Callable[[], CatalogContentsResponse]) -> ExternalRef:
        """Publish/recover a full snapshot after the caller authorizes the request."""
        result = self.resolve(request, build)
        assert isinstance(result, ExternalRef)
        return result

    def import_publication(self, request: SnapshotRequest, manifest: CatalogPublicationManifest) -> ExternalRef:
        """Validate a trusted descriptor by HTTP before committing or serving it."""
        key = self.cache_key(request)
        with self._lock(key):
            validated = self._recover(manifest, key, request.catalog_version)
            if self.cache_key(request) != key:
                raise ValueError("catalog generation changed while importing its publication")
            winner = self.repository.commit_if_absent(validated)
            if winner.manifest_id != validated.manifest_id:
                winner = self._recover(winner, key, request.catalog_version)
            if self.cache_key(request) != key:
                raise ValueError("catalog generation changed while importing its publication")
            return winner.reference

    def invalidate(self, key: str, manifest_id: str) -> bool:
        """Conditionally evict a descriptor; callers can repair failed client downloads."""
        with self._lock(key):
            cached = self._validated.get(key)
            if cached is not None and cached.manifest_id == manifest_id:
                del self._validated[key]
            return self.repository.invalidate(key, manifest_id)
