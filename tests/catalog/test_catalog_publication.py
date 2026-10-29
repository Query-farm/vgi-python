"""Publication correctness through real HTTP readback, recovery and worker auth."""

from __future__ import annotations

import hashlib
import io
import json
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Annotated, Any

import pyarrow as pa
import pytest
from vgi_rpc import ArrowSerializableDataclass, ArrowType
from vgi_rpc.external import ClientExternalConfig, Compression, ExternalRef

from vgi._test_fixtures.catalog_contents import CATALOG_PROBE, ContentsProbeCatalog, ContentsProbeWorker
from vgi.catalog import AttachOpaqueData, SchemaInfo
from vgi.catalog.contents_codec import decode_collection, decode_inventory, encode_collection
from vgi.catalog.snapshot import (
    CatalogSnapshotManager,
    CatalogSnapshotPolicy,
    MemoryCatalogPublicationRepository,
    S3CatalogPublicationRepository,
    SnapshotIdentity,
    SnapshotRequest,
)
from vgi.exceptions import OpaqueDataNotRecognizedError
from vgi.protocol import CatalogAttachRequest, CatalogContentsResponse


class _Storage:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, str | None]] = {}
        self.uploads = 0
        self.gets = 0
        self.base = ""
        self.corrupt = False

    def upload(self, data: bytes, schema: pa.Schema, *, content_encoding: str | None = None) -> str:
        del schema
        self.uploads += 1
        key = f"/{self.uploads}"
        self.objects[key] = (b"broken" if self.corrupt else data, content_encoding)
        return self.base + key


@pytest.fixture
def storage() -> Any:
    """Serve immutable uploaded bytes over HTTP, including content encoding."""
    store = _Storage()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            store.gets += 1
            data, encoding = store.objects[self.path.split("?")[0]]
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            if encoding:
                self.send_header("Content-Encoding", encoding)
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args: Any) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    store.base = f"http://127.0.0.1:{server.server_port}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield store
    server.shutdown()
    server.server_close()
    thread.join()


def _response() -> CatalogContentsResponse:
    worker = ContentsProbeWorker()
    attach = worker.catalog_attach(
        CatalogAttachRequest(name=CATALOG_PROBE, options=None, data_version_spec=None, implementation_version=None)
    )
    data = asdict(worker.catalog_contents(attach.attach_opaque_data))
    data["etag"] = "generation-1"
    return CatalogContentsResponse(**data)


def _request(scope: str = "attachment-1", version: int = 1) -> SnapshotRequest:
    return SnapshotRequest(catalog=None, attach_opaque_data=b"payload", attachment_scope=scope, catalog_version=version)


def _policy(repository: Any, **kwargs: Any) -> CatalogSnapshotPolicy:
    return CatalogSnapshotPolicy(
        delivery="published",
        repository=repository,
        identity=lambda req: SnapshotIdentity(
            catalog_namespace="test", source_revision="deploy-1", catalog_version=req.catalog_version
        ),
        fetch_config=ClientExternalConfig(max_retries=0, url_validator=None),
        **kwargs,
    )


def _close_policy(policy: CatalogSnapshotPolicy) -> None:
    assert policy.fetch_config is not None
    policy.fetch_config.fetch_config.close()


@pytest.mark.parametrize("compression", [None, Compression(algorithm="gzip"), Compression()])
def test_recovery_and_warm_hits_skip_inventory_and_upload(storage: _Storage, compression: Compression | None) -> None:
    """Restart recovery does one validated GET; subsequent hits do no catalog work."""
    repository = MemoryCatalogPublicationRepository(storage, profile="test", stable_urls=True)
    policy = _policy(repository, compression=compression)
    first = CatalogSnapshotManager(policy)
    response = _response()
    request = _request(version=response.catalog_version)
    try:
        ref = first.resolve(request, lambda: response)
        assert isinstance(ref, ExternalRef)
        assert first.statistics == {"builds": 1, "uploads": 1, "fetches": 1}
        assert first.resolve(request, lambda: pytest.fail("warm build")) == ref
        unchanged = first.resolve(request, lambda: pytest.fail("conditional build"), "generation-1")
        assert isinstance(unchanged, CatalogContentsResponse) and unchanged.not_modified
        assert storage.uploads == 1 and storage.gets == 1
        recovered = CatalogSnapshotManager(policy)
        assert recovered.resolve(request, lambda: pytest.fail("recovery build")) == ref
        assert recovered.statistics == {"builds": 0, "uploads": 0, "fetches": 1}
        assert recovered.resolve(request, lambda: pytest.fail("second recovery build")) == ref
        assert storage.uploads == 1 and storage.gets == 2
    finally:
        _close_policy(policy)


def test_scopes_singleflight_and_conditional_invalidation(storage: _Storage) -> None:
    """Attachments are isolated by default and concurrent callers share one build."""
    repository = MemoryCatalogPublicationRepository(storage, profile="test", stable_urls=True)
    policy = _policy(repository)
    manager = CatalogSnapshotManager(policy)
    response = _response()
    request = _request(version=response.catalog_version)
    try:
        with ThreadPoolExecutor(max_workers=8) as pool:
            refs = list(pool.map(lambda _: manager.resolve(request, lambda: response), range(16)))
        assert all(ref == refs[0] for ref in refs)
        assert manager.statistics["builds"] == 1
        manager.resolve(replace(request, attachment_scope="other"), lambda: response)
        assert storage.uploads == 2
        key = manager.cache_key(request)
        manifest = repository.lookup(key)
        assert manifest is not None
        assert not manager.invalidate(key, "wrong-id")
        assert repository.lookup(key) == manifest
        assert manager.invalidate(key, manifest.manifest_id)
        manager.resolve(request, lambda: response)
        assert storage.uploads == 3
    finally:
        _close_policy(policy)


def test_singleflight_allows_independent_keys_to_build_concurrently(storage: _Storage) -> None:
    """An unrelated scope cannot be stalled behind another snapshot's build."""
    repository = MemoryCatalogPublicationRepository(storage, profile="test", stable_urls=True)
    policy = _policy(repository)
    manager = CatalogSnapshotManager(policy)
    response = _response()
    entered = threading.Barrier(2, timeout=5)

    def build() -> CatalogContentsResponse:
        entered.wait()
        return response

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(manager.resolve, _request(scope=scope, version=response.catalog_version), build)
                for scope in ("first", "second")
            ]
            assert all(isinstance(future.result(), ExternalRef) for future in futures)
        assert manager.statistics == {"builds": 2, "uploads": 2, "fetches": 2}
    finally:
        _close_policy(policy)


def test_failed_readback_never_commits_and_caps_prevent_upload(storage: _Storage) -> None:
    """Corrupt uploads are rejected, and configured caps apply before upload."""
    repository = MemoryCatalogPublicationRepository(storage, profile="test", stable_urls=True)
    policy = _policy(repository)
    manager = CatalogSnapshotManager(policy)
    response = _response()
    request = _request(version=response.catalog_version)
    try:
        storage.corrupt = True
        with pytest.raises((ValueError, RuntimeError, pa.ArrowInvalid)):
            manager.resolve(request, lambda: response)
        assert repository.lookup(manager.cache_key(request)) is None
        storage.corrupt = False
        limited = CatalogSnapshotManager(replace(policy, max_raw_bytes=10))
        with pytest.raises(ValueError, match="before upload"):
            limited.resolve(request, lambda: response)
        assert storage.uploads == 1
    finally:
        _close_policy(policy)


def test_import_validates_nested_catalog_and_contract(storage: _Storage) -> None:
    """Imported descriptors cannot cross scope or bypass nested catalog validation."""
    repository = MemoryCatalogPublicationRepository(storage, profile="test", stable_urls=True)
    policy = _policy(repository)
    manager = CatalogSnapshotManager(policy)
    response = _response()
    request = _request(version=response.catalog_version)
    try:
        manager.resolve(request, lambda: response)
        manifest = repository.lookup(manager.cache_key(request))
        assert manifest is not None
        with pytest.raises(ValueError, match="incompatible"):
            CatalogSnapshotManager(policy).import_publication(replace(request, attachment_scope="other"), manifest)
        malformed = replace(response, functions=b"invalid inner IPC")
        other = replace(request, attachment_scope="bad")
        with pytest.raises(pa.ArrowInvalid):
            manager.resolve(other, lambda: malformed)
        assert storage.uploads == 1
    finally:
        _close_policy(policy)


def test_cheap_validator_avoids_all_publication_work(storage: _Storage) -> None:
    """A cheap current validator can answer before repository lookup or building."""
    repository = MemoryCatalogPublicationRepository(storage, profile="test", stable_urls=True)
    policy = _policy(repository, validator=lambda request: "fresh")
    manager = CatalogSnapshotManager(policy)
    try:
        result = manager.resolve(_request(), lambda: pytest.fail("build"), "fresh")
        assert isinstance(result, CatalogContentsResponse) and result.not_modified
        assert manager.statistics == {"builds": 0, "uploads": 0, "fetches": 0}
    finally:
        _close_policy(policy)


def test_worker_auth_precedes_cache_and_fallback_is_explicit(
    storage: _Storage, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The attachment is still checked on cache hits; optional fallback preserves metadata."""
    repository = MemoryCatalogPublicationRepository(storage, profile="test", stable_urls=True)
    policy = _policy(repository)
    monkeypatch.setattr(ContentsProbeCatalog, "catalog_snapshot_policy", policy)
    worker = ContentsProbeWorker()
    worker._signing_key = b"k" * 32
    attach = worker.catalog_attach(
        CatalogAttachRequest(name=CATALOG_PROBE, options=None, data_version_spec=None, implementation_version=None)
    ).attach_opaque_data
    try:
        assert isinstance(worker.catalog_contents(attach), ExternalRef)
        assert storage.uploads == 1
        with pytest.raises(OpaqueDataNotRecognizedError):
            worker.catalog_contents(b"invalid-attach-envelope")
        assert storage.uploads == 1
        monkeypatch.setattr(
            ContentsProbeCatalog, "catalog_snapshot_policy", replace(policy, on_publication_failure="ordinary")
        )
        storage.corrupt = True
        another = ContentsProbeWorker()
        other_attach = another.catalog_attach(
            CatalogAttachRequest(name=CATALOG_PROBE, options=None, data_version_spec=None, implementation_version=None)
        ).attach_opaque_data
        response = another.catalog_contents(other_attach)
        assert isinstance(response, CatalogContentsResponse)
        assert decode_inventory(response)
    finally:
        _close_policy(policy)


def test_worker_fallback_rejects_generation_change(storage: _Storage, monkeypatch: pytest.MonkeyPatch) -> None:
    """Ordinary fallback cannot hide a generation change during its own build."""
    repository = MemoryCatalogPublicationRepository(storage, profile="test", stable_urls=True)
    revision = [0]
    policy = replace(
        _policy(repository, on_publication_failure="ordinary"),
        identity=lambda req: SnapshotIdentity(
            catalog_namespace="test", source_revision=str(revision[0]), catalog_version=req.catalog_version
        ),
    )
    monkeypatch.setattr(ContentsProbeCatalog, "catalog_snapshot_policy", policy)
    original = ContentsProbeCatalog.catalog_contents

    def changing_contents(*args: Any, **kwargs: Any) -> Any:
        result = original(*args, **kwargs)
        revision[0] += 1
        return result

    monkeypatch.setattr(ContentsProbeCatalog, "catalog_contents", changing_contents)
    worker = ContentsProbeWorker()
    attach = worker.catalog_attach(
        CatalogAttachRequest(name=CATALOG_PROBE, options=None, data_version_spec=None, implementation_version=None)
    )
    try:
        with pytest.raises(ValueError, match="generation changed"):
            worker.catalog_contents(attach.attach_opaque_data)
        assert storage.uploads == 0
    finally:
        _close_policy(policy)


def test_collections_consume_multiple_batches_and_reject_wrong_schema() -> None:
    """Later rows and batches survive; legacy schema attachment bytes disappear."""
    records = [
        SchemaInfo(path=[str(i)], comment=None, tags={"x": str(i)}, attach_opaque_data=AttachOpaqueData(b"secret"))
        for i in range(7)
    ]
    encoded = encode_collection(SchemaInfo, records, batch_size=2)
    assert encoded is not None
    assert decode_collection(SchemaInfo, encoded) == records
    assert len(list(pa.ipc.open_stream(encoded))) == 4
    assert b"secret" not in encoded
    with pytest.raises(ValueError, match="incompatible schema"):
        decode_collection(SchemaInfo, _response().functions)


class _S3Error(Exception):
    def __init__(self, code: str) -> None:
        self.response = {"Error": {"Code": code}}


class _S3:
    """A conditional object store fixture shared by separate repository instances."""

    def __init__(self, store: _Storage) -> None:
        self.store = store
        self.etags: dict[str, str] = {}

    def get_object(self, *, Bucket: str, Key: str) -> Any:
        del Bucket
        if "/" + Key not in self.store.objects:
            raise _S3Error("NoSuchKey")
        return {"Body": io.BytesIO(self.store.objects["/" + Key][0]), "ETag": self.etags[Key]}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, **kwargs: Any) -> None:
        del Bucket
        if kwargs.get("IfNoneMatch") == "*" and Key in self.etags:
            raise _S3Error("PreconditionFailed")
        if "IfMatch" in kwargs and kwargs["IfMatch"] != self.etags.get(Key):
            raise _S3Error("PreconditionFailed")
        self.store.objects["/" + Key] = (Body, kwargs.get("ContentEncoding"))
        self.etags[Key] = str(int(self.etags.get(Key, "0")) + 1)

    def generate_presigned_url(self, operation: str, *, Params: dict[str, str], ExpiresIn: int) -> str:
        del operation, ExpiresIn
        return self.store.base + "/" + Params["Key"]


def test_durable_s3_recovery_and_conditional_manifest_tombstone(storage: _Storage) -> None:
    """A fresh S3 repository recovers published bytes without rebuilding inventory."""
    client = _S3(storage)
    first = S3CatalogPublicationRepository(client, bucket="test", profile="test")
    policy = _policy(first)
    response = _response()
    request = _request(version=response.catalog_version)
    manager = CatalogSnapshotManager(policy)
    try:
        reference = manager.resolve(request, lambda: response)
        second = S3CatalogPublicationRepository(client, bucket="test", profile="test")
        recovered = CatalogSnapshotManager(replace(policy, repository=second))
        assert recovered.resolve(request, lambda: pytest.fail("durable recovery build")) == reference
        key = manager.cache_key(request)
        manifest = second.lookup(key)
        assert manifest is not None
        assert not second.invalidate(key, "not-the-generation")
        assert second.invalidate(key, manifest.manifest_id)
        assert second.lookup(key) is None
        assert second.commit_if_absent(manifest) == manifest
        assert second.lookup(key) == manifest
    finally:
        _close_policy(policy)


def test_s3_renewal_is_persisted_and_preserves_a_concurrent_replacement(
    storage: _Storage, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Restart discovery sees renewed URLs; conditional renewal cannot replace a new manifest."""
    client = _S3(storage)
    sign = client.generate_presigned_url
    signatures = [0]

    def signed_url(*args: Any, **kwargs: Any) -> str:
        signatures[0] += 1
        return sign(*args, **kwargs) + f"?signature={signatures[0]}"

    monkeypatch.setattr(client, "generate_presigned_url", signed_url)
    repository = S3CatalogPublicationRepository(client, bucket="test", profile="test")
    policy = _policy(repository)
    response = _response()
    request = _request(version=response.catalog_version)
    manager = CatalogSnapshotManager(policy)
    try:
        manager.resolve(request, lambda: response)
        key = manager.cache_key(request)
        manifest = repository.lookup(key)
        assert manifest is not None
        object_keys = set(storage.objects)
        renewed = repository.renew(manifest)
        assert renewed is not None and renewed.url != manifest.url
        assert renewed.manifest_id == manifest.manifest_id
        restarted = S3CatalogPublicationRepository(client, bucket="test", profile="test")
        assert restarted.lookup(key) == renewed
        recovered = CatalogSnapshotManager(replace(policy, repository=restarted))
        assert recovered.resolve(request, lambda: pytest.fail("renewal recovery build")) == renewed.reference
        assert recovered.statistics == {"builds": 0, "uploads": 0, "fetches": 1}
        assert set(storage.objects) == object_keys

        put = client.put_object
        replacement = replace(renewed, manifest_id="replacement-generation")
        replace_once = [True]

        def concurrent_put(**kwargs: Any) -> None:
            if replace_once[0] and "IfMatch" in kwargs:
                replace_once[0] = False
                put(**{**kwargs, "Body": json.dumps(asdict(replacement)).encode()})
            put(**kwargs)

        monkeypatch.setattr(client, "put_object", concurrent_put)
        assert repository.renew(renewed) is None
        assert restarted.lookup(key) == replacement
    finally:
        _close_policy(policy)


def test_subprocess_client_resolves_published_catalog() -> None:
    """The high-level subprocess catalog path resolves a published RPC result."""
    from vgi.client import Client

    config = ClientExternalConfig(max_retries=0, url_validator=None)
    try:
        with Client(f"{sys.executable} -m vgi._test_fixtures.catalog_publication", external_location=config) as client:
            attach = client.catalog_attach(name=CATALOG_PROBE, data_version_spec=None, implementation_version=None)
            result = client.contents(attach_opaque_data=attach.attach_opaque_data)
            assert len(result.schemas) == 2
            assert result.schemas[0].tables[0].name == "ten"
    finally:
        config.fetch_config.close()


def test_collection_rejects_concatenated_streams_and_dictionary_nulls() -> None:
    """Stream trailing bytes and null dictionary values cannot hide invalid rows."""

    @dataclass
    class Entry(ArrowSerializableDataclass):
        value: Annotated[str, ArrowType(pa.dictionary(pa.int16(), pa.string()))]

    encoded = encode_collection(Entry, [Entry("a"), Entry("b"), Entry("c")], batch_size=1)
    assert encoded is not None
    assert decode_collection(Entry, encoded) == [Entry("a"), Entry("b"), Entry("c")]
    with pytest.raises(ValueError, match="trailing bytes"):
        decode_collection(Entry, encoded + encoded)
    sink = pa.BufferOutputStream()
    dictionary = pa.DictionaryArray.from_arrays(pa.array([0], type=pa.int16()), pa.array([None], type=pa.string()))
    with pa.ipc.new_stream(sink, Entry.ARROW_SCHEMA) as writer:
        writer.write_batch(pa.RecordBatch.from_arrays([dictionary], schema=Entry.ARROW_SCHEMA))
    with pytest.raises(ValueError, match="contains nulls"):
        decode_collection(Entry, sink.getvalue().to_pybytes())


def test_import_clamps_supplied_fetch_limits(storage: _Storage) -> None:
    """A permissive supplied download policy cannot bypass publication byte caps."""
    repository = MemoryCatalogPublicationRepository(storage, profile="test", stable_urls=True)
    policy = _policy(repository)
    manager = CatalogSnapshotManager(policy)
    response = _response()
    request = _request(version=response.catalog_version)
    try:
        manager.resolve(request, lambda: response)
        manifest = repository.lookup(manager.cache_key(request))
        assert manifest is not None
        # The trusted manifest may still contain erroneous size bookkeeping;
        # the downloader must independently enforce the actual byte limit.
        limited = CatalogSnapshotManager(replace(policy, max_raw_bytes=1000))
        with pytest.raises((ValueError, RuntimeError), match="decompress|bytes|limit"):
            limited.import_publication(request, replace(manifest, raw_size=999))
    finally:
        _close_policy(policy)


def test_import_checks_actual_outer_size_digest_and_stream_end(storage: _Storage) -> None:
    """Even matching hashes cannot make a concatenated outer stream valid."""
    repository = MemoryCatalogPublicationRepository(storage, profile="test", stable_urls=True)
    policy = _policy(repository)
    manager = CatalogSnapshotManager(policy)
    response = _response()
    request = _request(version=response.catalog_version)
    try:
        manager.resolve(request, lambda: response)
        manifest = repository.lookup(manager.cache_key(request))
        assert manifest is not None
        with pytest.raises(ValueError, match="raw byte length"):
            CatalogSnapshotManager(policy).import_publication(
                request, replace(manifest, raw_size=manifest.raw_size - 1)
            )
        with pytest.raises(ValueError, match="SHA-256"):
            CatalogSnapshotManager(policy).import_publication(request, replace(manifest, sha256="f" * 64))
        path = manifest.url.removeprefix(storage.base)
        raw, encoding = storage.objects[path]
        assert encoding is None
        malformed = raw + raw
        storage.objects[path] = malformed, None
        invalid = replace(
            manifest,
            sha256=hashlib.sha256(malformed).hexdigest(),
            raw_size=len(malformed),
            stored_size=len(malformed),
        )
        with pytest.raises(ValueError, match="trailing bytes"):
            CatalogSnapshotManager(policy).import_publication(request, invalid)
    finally:
        _close_policy(policy)


def test_warm_renewal_rejects_changed_immutable_metadata(storage: _Storage) -> None:
    """A faulty renewal cannot silently change scope or content behind a warm ref."""

    class Repository(MemoryCatalogPublicationRepository):
        def renew(self, manifest: Any) -> Any:
            return replace(manifest, sha256="f" * 64, expires_at=9999999999.0)

    repository = Repository(storage, profile="test", stable_urls=True)
    policy = _policy(repository)
    manager = CatalogSnapshotManager(policy)
    response = _response()
    request = _request(version=response.catalog_version)
    try:
        manager.resolve(request, lambda: response)
        key = manager.cache_key(request)
        manifest = repository.lookup(key)
        assert manifest is not None
        manager._validated[key] = replace(manifest, stable=False, expires_at=0)
        with pytest.raises(ValueError, match="immutable"):
            manager.resolve(request, lambda: pytest.fail("renewal must not rebuild"))
    finally:
        _close_policy(policy)


def test_generation_change_does_not_commit(storage: _Storage) -> None:
    """A metadata generation changed during the build cannot be published under the old key."""
    repository = MemoryCatalogPublicationRepository(storage, profile="test", stable_urls=True)
    revision = ["before"]
    policy = replace(
        _policy(repository),
        identity=lambda req: SnapshotIdentity(
            catalog_namespace="test", source_revision=revision[0], catalog_version=req.catalog_version
        ),
    )
    manager = CatalogSnapshotManager(policy)
    response = _response()
    request = _request(version=response.catalog_version)
    key = manager.cache_key(request)

    def build() -> CatalogContentsResponse:
        revision[0] = "after"
        return response

    try:
        with pytest.raises(ValueError, match="generation changed"):
            manager.resolve(request, build)
        assert storage.uploads == 0
        assert repository.lookup(key) is None
    finally:
        _close_policy(policy)


@pytest.mark.parametrize("phase", ["lookup", "readback", "commit"])
def test_resolve_rejects_generation_changes_during_repository_io(
    storage: _Storage, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    """Repository and HTTP work cannot return an obsolete generation or ETag."""
    repository = MemoryCatalogPublicationRepository(storage, profile="test", stable_urls=True)
    revision = ["before"]
    policy = replace(
        _policy(repository),
        identity=lambda req: SnapshotIdentity(
            catalog_namespace="test", source_revision=revision[0], catalog_version=req.catalog_version
        ),
    )
    response = _response()
    request = _request(version=response.catalog_version)
    manager = CatalogSnapshotManager(policy)
    try:
        if phase != "commit":
            CatalogSnapshotManager(policy).resolve(request, lambda: response)
        target, name = (
            (manager, "_readback")
            if phase == "readback"
            else (repository, "commit_if_absent" if phase == "commit" else "lookup")
        )
        original = getattr(target, name)

        def change_generation(*args: Any, **kwargs: Any) -> Any:
            result = original(*args, **kwargs)
            revision[0] = "after"
            return result

        monkeypatch.setattr(target, name, change_generation)
        with pytest.raises(ValueError, match="generation changed"):
            manager.resolve(request, lambda: response, if_none_match=response.etag)
    finally:
        _close_policy(policy)


@pytest.mark.parametrize("phase", ["readback", "commit"])
def test_import_rejects_generation_changes_during_repository_io(
    storage: _Storage, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    """An import rechecks its generation after validation and conditional commit."""
    repository = MemoryCatalogPublicationRepository(storage, profile="test", stable_urls=True)
    revision = ["before"]
    policy = replace(
        _policy(repository),
        identity=lambda req: SnapshotIdentity(
            catalog_namespace="test", source_revision=revision[0], catalog_version=req.catalog_version
        ),
    )
    response = _response()
    request = _request(version=response.catalog_version)
    manager = CatalogSnapshotManager(policy)
    try:
        CatalogSnapshotManager(policy).resolve(request, lambda: response)
        manifest = repository.lookup(manager.cache_key(request))
        assert manifest is not None
        assert repository.invalidate(manifest.cache_key, manifest.manifest_id)
        target, name = (manager, "_readback") if phase == "readback" else (repository, "commit_if_absent")
        original = getattr(target, name)

        def change_generation(*args: Any, **kwargs: Any) -> Any:
            result = original(*args, **kwargs)
            revision[0] = "after"
            return result

        monkeypatch.setattr(target, name, change_generation)
        with pytest.raises(ValueError, match="generation changed"):
            manager.import_publication(request, manifest)
        if phase == "readback":
            assert repository.lookup(manifest.cache_key) is None
    finally:
        _close_policy(policy)


def test_worker_publications_preserve_capability_projection(storage: _Storage, monkeypatch: pytest.MonkeyPatch) -> None:
    """Shared published metadata and conditional replies stay within the negotiated projection."""
    from vgi.catalog.capabilities import SCHEMA_OPTIONS_READ, CatalogCapabilities
    from vgi.catalog.options import OptionState
    from vgi.protocol import ClientCapabilities
    from vgi.typed_values import TypedValueBatch

    repository = MemoryCatalogPublicationRepository(storage, profile="projection", stable_urls=True)
    policy = replace(
        _policy(repository),
        identity=lambda req: SnapshotIdentity(
            catalog_namespace="test",
            source_revision="deploy-1",
            catalog_version=req.catalog_version,
            visibility_scope="shared",
        ),
        validator=lambda req: "raw-provider-etag",
    )
    monkeypatch.setattr(ContentsProbeCatalog, "catalog_snapshot_policy", policy)
    available = CatalogCapabilities(read=[SCHEMA_OPTIONS_READ])
    monkeypatch.setattr(ContentsProbeCatalog, "catalog_capabilities", lambda self, **kwargs: available)
    original = ContentsProbeCatalog.catalog_contents
    options = OptionState(values=TypedValueBatch(pa.record_batch([pa.array([7], type=pa.int32())], names=["limit"])))

    def contents(self: Any, **kwargs: Any) -> Any:
        result = original(self, **kwargs)
        return replace(
            result, schemas=[replace(item, schema=replace(item.schema, options=options)) for item in result.schemas]
        )

    monkeypatch.setattr(ContentsProbeCatalog, "catalog_contents", contents)
    worker = ContentsProbeWorker()
    worker._signing_key = b"k" * 32

    def attach(read: list[str]) -> bytes:
        return worker.catalog_attach(
            CatalogAttachRequest(
                name=CATALOG_PROBE,
                options=None,
                data_version_spec=None,
                implementation_version=None,
                client_capabilities=ClientCapabilities(
                    engine="test",
                    native_formats=[],
                    catalogs=[],
                    can_stream=True,
                    filter_encodings=[],
                    catalog=CatalogCapabilities(read=read),
                ),
            )
        ).attach_opaque_data

    try:
        full = attach([SCHEMA_OPTIONS_READ])
        limited = attach([])
        full_ref = worker.catalog_contents(full)
        limited_ref = worker.catalog_contents(limited)
        assert isinstance(full_ref, ExternalRef) and isinstance(limited_ref, ExternalRef)
        assert full_ref != limited_ref
        assert storage.uploads == 2
        assert worker.catalog_contents(full) == full_ref
        assert worker.catalog_contents(limited) == limited_ref
        assert worker.catalog_contents(full, "raw-provider-etag") == full_ref
        # The same explicitly shared visibility can still reuse an identical projection.
        assert worker.catalog_contents(attach([SCHEMA_OPTIONS_READ])) == full_ref
        assert storage.uploads == 2
        manager = worker._catalog_snapshot_manager(policy)
        manifests = list(manager._validated.values())
        assert len(manifests) == 2 and len({m.etag for m in manifests}) == 2
        responses = []
        for raw, encoding in storage.objects.values():
            assert encoding is None
            with pa.ipc.open_stream(raw) as reader:
                payload = reader.read_all().column(0)[0].as_py()
            responses.append(CatalogContentsResponse.deserialize_from_bytes(payload))
        values = [decode_inventory(response)[0].schema.options for response in responses]
        assert sum(value is None for value in values) == 1
        assert next(value for value in values if value is not None).values.batch.to_pylist() == [{"limit": 7}]
    finally:
        _close_policy(policy)
