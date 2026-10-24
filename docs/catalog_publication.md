# Batched catalog contents and reusable publications

VGI wire revision **2.2.0** retains the `vgi.v2` route and replaces
`catalog_contents` with six catalog-wide Arrow IPC streams: schemas, tables,
views, functions, macros and indexes. Each stream can contain multiple batches.
Null means an empty category. Function and macro subtype fields remain intact.
Existing schema-level RPCs retain their previous collection encoding.

Function metadata stores `arguments`, `output_schema` and
`parameter_default_values` as `dictionary<int32, binary>` columns. Repeated
serialized schemas and typed defaults share dictionary entries by their byte
content. Python callers continue to use bytes for schemas and an optional
`RecordBatch` for defaults. Dictionary replacements across record batches are
supported; values remain opaque binary data rather than text.
Single-object and schema-level function RPCs use these same updated rows.

`SchemaInfo` no longer contains attachment data on the wire. Existing Python
authors may still pass `attach_opaque_data=` to its constructor during migration;
the argument is discarded. Use `CatalogAttachResult.attach_opaque_data` for
subsequent requests. Worker functions still receive their attachment state.

Every minor wire revision is an explicit contract boundary. The server checks
major and minor exactly, so clients and workers must both use 2.2. A patch bump
does not permit schema changes. Serving 2.1 and 2.2 during migration requires
separate deployment URLs or an explicit compatibility implementation.

## Enable publication

Publication is opt-in through `CatalogInterface.catalog_snapshot_policy`.
Ordinary delivery and the existing frozen-catalog response cache remain the
default. Published delivery uses the existing vgi-rpc `ExternalRef` mechanism;
clients resolve the external object on all transports, including subprocess.

Install `vgi-python[external]` on publishing workers and clients that resolve
publications; add `http` when serving or connecting over HTTP, for example
`pip install 'vgi-python[http,external]'`. The `external` extra supplies the RPC
HTTP downloader, retry and compression dependencies. Supply your own configured
S3 client and its SDK when using the S3 repository.

The catalog implementation uses APIs available in published **vgi-rpc 0.50.0**.
An isolated check against that release passed 157 functional catalog, client,
serialization, binary dictionary and publication tests. The package retains its
declared RPC minimum of 0.52.0 for Identity.v2 support; catalog publication does
not introduce a dependency on unpublished RPC APIs.

```python
from vgi.catalog.snapshot import (
    CatalogSnapshotPolicy,
    S3CatalogPublicationRepository,
    SnapshotIdentity,
)
from vgi_rpc.external import Compression

# s3 is your configured boto3-compatible S3/R2 client. The client must support
# conditional PutObject with IfNoneMatch and IfMatch.
repository = S3CatalogPublicationRepository(
    s3,
    bucket="catalog-snapshots",
    profile="production-r2-gzip",
    url_ttl_seconds=3600,
    retention_seconds=86400,
)

class MyCatalog(ExistingCatalog):
    catalog_snapshot_policy = CatalogSnapshotPolicy(
        delivery="published",
        repository=repository,
        identity=lambda request: SnapshotIdentity(
            catalog_namespace="my-catalog",
            source_revision=DEPLOYMENT_REVISION,
            catalog_version=request.catalog_version,
            # Omit this for the safe attachment-local default. Sharing requires
            # an explicit guarantee that metadata is identical for this scope.
            visibility_scope="public-default-options",
        ),
        compression=Compression(algorithm="gzip"),
    )
```

The identity hook must be cheap and must change `source_revision` whenever
metadata changes. A dynamic catalog can use a schema generation rather than a
deployment revision. Include options, resolved implementation/data versions,
tenant or principal, and any other metadata visibility differences in its scope.
Do not put credentials into names or scope strings. Omitting `visibility_scope`
isolates publications by a hash of the attachment envelope. Catalog version zero
still requires a meaningful source revision.

An optional `validator(request) -> str | None` can answer a matching ETag before
repository access or inventory construction. Every request unwraps and authorizes
its attachment first, even on a cache hit. The framework also checks that version
and source identity remain unchanged while it builds a publication.

## Publication lifecycle

The framework writes a complete unary RPC envelope with one `result: binary`
row. That binary contains the catalog response and its six nested streams.
The stored object is compressed according to the policy and includes a mandatory
SHA-256 over the raw envelope. Before committing its manifest, the framework
fetches the actual HTTP URL and validates the digest, envelope, nested schemas,
all rows/batches, schema membership, subtype fields and metadata equality.
Raw and stored byte limits apply before upload and during readback, including
when the caller supplies a custom fetch policy.

The repository stores small trusted manifests separately from immutable objects.
Keys include routing name, major/minor wire revision, generated canonical catalog
contract hash, codec revision, namespace, source revision, visibility scope,
catalog version, and storage/compression profile. A new worker process looks up
the manifest before collecting metadata, then validates it with one HTTP fetch.
A warm hit performs no inventory build, serialization, compression, upload or
object GET. A client receiving the pointer still downloads the result as needed.

`S3CatalogPublicationRepository` uses conditional manifest writes and tombstones
for invalidation. It can re-sign a retained object without rebuilding it.
Configure object lifecycle rules to retain objects for at least the declared
retention interval; keep manifest retention separate. Signed URLs in manifests
are credentials, so the manifest store must be access controlled. Newly changed
retrieval URLs are validated before serving them.

`MemoryCatalogPublicationRepository` supports a supplied RPC uploader for local
use. It requires an explicit URL lifetime or stable-location declaration.
It does not provide persistence across process restarts. A process coordinates
one build per key; conditional repository commit makes separate publishers
converge but can permit duplicate initial uploads across processes. Storage
lifecycle rules should collect unused losing uploads.

The manager bounds its in-memory manifest cache. URL or object-retention expiry
causes renewal or rebuilding. Recovery of a corrupt or missing object
conditionally invalidates that manifest and attempts a fresh publication.
Publication errors fail the RPC by default. Set
`on_publication_failure="ordinary"` to explicitly permit a newly validated
ordinary full response when publication fails.

Workers expose `warm_catalog_contents(attach_opaque_data)`,
`import_catalog_publication(attach_opaque_data, manifest)` and
`invalidate_catalog_publication(attach_opaque_data, manifest_id)` for authorized
operational use. Import uses the same HTTP validation as recovery. A failed client
download does not automatically notify the worker; use conditional invalidation
to repair a known bad publication without deleting a concurrent replacement.

## Validation

Run the focused Python checks with the project environment on `PATH`:

```sh
uv run pytest tests/catalog/test_catalog_contents.py \
  tests/catalog/test_catalog_publication.py \
  tests/catalog/test_client_load_catalog.py tests/catalog/test_serialization.py
```

The publication tests use real HTTP object retrieval with uncompressed, gzip and
zstd results. They exercise restart recovery, singleflight, default attachment
isolation, corrupt publication, explicit fallback, authentication before cached
delivery, caps, generation changes, manifest import and conditional invalidation.
The S3 fixture verifies discovery and conditional writes across repository
instances. Configure a real S3/R2 bucket and deployment CORS before production
rollout; the in-memory conditional object fixture does not validate a vendor's
credentials, CORS settings or lifecycle configuration.

The installable native/browser fixture is:

```sh
python -m vgi._test_fixtures.catalog_publication
```

It accepts ordinary fixture worker options, including `--http`. Only
`contents_probe` uses publication; other standard fixture catalogs retain their
normal behavior. Set `VGI_CATALOG_PUBLICATION_PORT` to expose the local object
server at a predictable port; `/stats` reports uploads and object GETs. Its CORS
headers and preflight support permit browser conformance tests. This fixture is
intended for local testing and its object storage lasts only for the worker's
lifetime.
