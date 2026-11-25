# Changelog

## 0.55.1 (2026-11-24)

Patch release on VGI protocol 3.4 and vgi-rpc 0.57.1. The wire protocol is unchanged.

### Features

- **Storage timeouts** (vgi-rpc 0.57.1): large-value transfers use a 30 s connect timeout and a 30 s stall timeout, with no whole-request deadline, and 3 attempts. `VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS` tunes the stall timeout. S3 catalog publications stream their bodies, so the read timeout acts as a stall timeout there too.
- **Direct object storage for self-hosted workers**: `vgi-serve`, hosted mode and the HTTP fixture server can keep large values in your own bucket instead of the blob vendor. Set `VGI_STORAGE_BACKEND` (`vendor` | `s3` | `gcs` | `none`) with `VGI_STORAGE_S3_*` / `VGI_STORAGE_GCS_*`. S3 uses the full AWS credential chain, including session tokens. `VGI_STORAGE_S3_CHECKSUM=none` turns off upload URLs. Keys go under `rpc/` and `fs/`. Install with the new `vgi-python[s3]` / `vgi-python[gcs]` extras. See `docs/how-to/object-storage.md`.
- **Protocol version negotiation** (`PROTOCOL_VERSIONS_V1`): every server advertises the protocols it gates in `VGI-Protocol-Versions` (`vgi.v3=3.4-3.4`, `vgi.secret.v1=1.0-1.0`, and `vgi.attach_tickets.v1=2.0-2.0` when hosted). A version outside the window is refused with `protocol_version_unsupported`. `VGI_TEST_PROTOCOL_RANGE` overrides a fixture worker's window. Per-version schema snapshots are checked in CI.

### Fixes

- **The `[haybarn]` extra pins `haybarn==2.0.1rc3`** (was `2.0.0rc11`). The 3.4 `vgi` extension is only served to Haybarn 2.0.1, so a worker that attached `vgi` through this extra got the 3.3 build and failed. Haybarn 2.0.1rc3 is now the trusted filter engine (`vgi.duckdb.standard.v1`, filter corpus 92/92); 2.0.0rc11 is still accepted. `[duckdb]` stays `duckdb==2.0.0`.

### Notes

- **Haybarn 2.0.1rc3 is required for the 3.4 `vgi` extension.**
- **macOS: install one engine.** A process that loads both haybarn 2.0.1rc3 and stock duckdb 2.0.0 can crash on macOS ("Fatal Python error: Bus error"). Install either `vgi-python[haybarn]` or `vgi-python[duckdb]`, not both. vgi itself loads only one engine (haybarn if present), so this affects code that imports `duckdb` directly next to haybarn. Linux and Windows are unaffected.

## 0.55.0 (2026-11-23)

### Breaking

- **VGI protocol 3.4.0.** Clients and workers match on exact major and minor, so a 3.4 worker refuses a 3.3
  client and vice versa. The only change is the upload-URL contract below; see
  [docs/protocol/upload-urls-v2.md](docs/protocol/upload-urls-v2.md).
- **Upload URLs v2 (`VGI-Upload-URL-Support: 2`).** Requires vgi-rpc 0.57.0 (upload-URL protocol v2). `__upload_url__/init` takes one `(size, sha256)` entry per upload instead
  of `count`, and each vended PUT URL is signed for exactly that body. `Client` hashes an oversized input, PUTs
  exactly those bytes with the vended `upload_headers` on a bare client (never the RPC client's credentials), and
  puts `vgi_rpc.location.sha256` on the pointer. Workers refuse a request pointer without that digest. A storage
  refusal raises `ClientError` with the S3 error code and a redacted URL. `DemoBlobStorage` and the S3 fixture
  storage implement `generate_upload_url(size, sha256_hex)`, and `vgi-serve --http` with `VGI_BLOB_VENDOR_URL`
  vends sized vendor grants.

## 0.52.2 (2026-11-16)

### Fixes

- **A `Secret()` or `Setting()` on a shared base class's `on_bind()` now reaches its subclasses.** An abstract or
  generic base skips the annotation scan, and a subclass that did not redefine `on_bind()` copied the base's empty
  result, so the secret was never requested and arrived as `None` with no warning. Table and aggregate functions now
  read the `on_bind()` they actually resolve to. Workers that declared `Meta.required_secrets` on every subclass to
  get around this can declare the secret once on the base.
- **Scalar overloads that differ only in their number of column arguments are no longer ambiguous.**
  `f(a)` and `f(a, b)` both matched a one-column call because a missing column scored as a wildcard. The call's
  column count must now fit the overload's column `Param`s (column varargs still take N or more).

### New

- **`Attach()` gives a scalar's `compute()` its ATTACH options.** Annotate a parameter
  `Annotated[dict[str, Any], Attach()]` to receive `attach_options()` of the call's attach (secret options
  included, `{}` outside an attached catalog), as `Auth()` receives the `AuthContext`. Until now only `bind()` could
  read them; overriding the final `process()` to reach `init_call.bind_call.attach_opaque_data` also read the
  wire value, which can still be sealed (HTTP with a signing key). No wire or protocol change.

## 0.52.1 (2026-11-16)

### Fixtures

- **The example worker serves a `contract` schema** for the cross-SDK conformance files the vgi extension added
  (`visibility/example_table_only.test`, `write_contract/example_insert_finalize.test`,
  `write_contract/example_decimal_width.test`): a `table_only` scan with a row id, a `table_only` scan named
  `range`, an INSERT whose count comes only from `finalize` (with an enforced NOT NULL), and decimal32/64/128
  columns. The conformance checklists list the new files.

## 0.52.0 (2026-11-16)

### Engine: DuckDB 2.0

- **The embedded engine moves to DuckDB 2.0.0 / Haybarn 2.0.0rc11**, the engine line the DuckDB `vgi` extension is
  built on. The `[haybarn]` extra now pins `haybarn==2.0.0rc11` and `[duckdb]` pins `duckdb==2.0.0`. Until now both
  pinned 1.5.5, and an environment that also held haybarn 2.0 (to drive the extension in tests) refused every
  pushed-down filter with `vgi.duckdb.standard.v1 requires DuckDB 1.5.5 or Haybarn 1.5.5-rc1; found haybarn
  2.0.0rc11`.
- **The `vgi.duckdb.standard.v1` filter oracle is now upstream DuckDB v2.0.0** (`d54cbbe3df`), with Haybarn
  v2.0.0-rc11 as reference distribution (filter encoding spec section 6). The full filter conformance corpus passes
  unchanged on both 2.0 builds. Engine builds other than these two are refused, as before.
- **CHECK-constraint and generated-column validation read DuckDB 2.0's parsed-expression JSON**, which no longer types
  constants (`literal` {kind, text}), passes function `arguments` instead of `children`, and names cast targets with
  `type_expr`. Literal and cast types are resolved through the engine, as the extension's `ToValue()` /
  `TryDefaultBind` do; named function arguments are rejected, as the extension does. Under 0.51.x with a 2.0 engine
  every table declaring a CHECK or generated column failed at construction with `KeyError: 'value'`.
- `vgi._duckdb.arrow_table()` / `arrow_reader()` wrap `to_arrow_table()` / `to_arrow_reader()`, which DuckDB 2.0 types
  as optional.

### Migrating a worker

Workers that call the engine directly may need the same changes: DuckDB 2.0 relations have no `fetchone()` (use
`connection.execute(...).fetchone()`), and `json_serialize_sql` output changed shape as above. A worker that pins a
1.5 engine (`haybarn==1.5.5rc1`, `duckdb==1.5.5`) cannot install 0.52.0; drop that pin.

### CI

- A new `Engine drift` workflow fails when the `[haybarn]`/`[duckdb]` pins are on a different DuckDB major.minor line
  from the Haybarn tag in the extension's distribution workflow. It runs on every push and weekly.

## 0.51.1 (2026-11-12)

### Fixes

- **Pushed-down filters no longer need numpy.** In 0.51.0 every expression-filter evaluator connection registered
  the `duckdb.spatial/intersects_extent@1` UDF up front, and the engine's `create_function` needs numpy, which no
  user-facing extra declared. A worker installed as `vgi-python[haybarn]` (or `[duckdb]`) therefore failed every
  pushed-down filter with `FilterDeserializationError: ... predicate does not bind under vgi.duckdb.standard.v1:
  Invalid Input Error: 'numpy' is required for this operation`. The UDF is now registered only on the first
  predicate that calls `intersects_extent`, so ordinary filters never call `create_function`. The `haybarn` and
  `duckdb` extras now also declare `numpy>=2.4.1`, which that function does need; filter evaluation cannot run
  without one of those extras, so numpy stays out of the core dependencies. A new CI job installs the built wheel
  with only one engine extra and evaluates pushed-down filters, with and without numpy.

## 0.51.0 (2026-11-11)

### Fixes

- **Concurrent opens of a fresh SQLite storage file no longer fail with "database is locked".** When several worker
  processes opened the same new `FunctionStorageSqlite` or `CatalogStorageSqlite` file at once, the switch to WAL
  could fail at once with `SQLITE_BUSY`: SQLite does not call the busy handler there, so the 30 s connect timeout did
  not apply, and `FunctionStorageSqlite` set `busy_timeout` only after the switch. Both now set `busy_timeout` first,
  retry the WAL switch on `SQLITE_BUSY` with a bounded backoff (up to the same 30 s), and create their schema inside
  `BEGIN IMMEDIATE` so concurrent first-time setup serializes. The `simple_writable`, `schema_reconcile` and `txfix`
  fixture workers do the same. No API change.

### Protocol 3.3: write-path wire changes

Part of `vgi.v3` / `3.3.0` (the version string does not change again).

- **Removed `catalog_table_scan_function_get`.** `catalog_table_scan_branches_get` is the only scan-discovery RPC
  and every worker must answer it; the extension no longer falls back. Author code is unaffected:
  `CatalogInterface.table_scan_function_get` still works, wrapped as a one-branch result by the default
  `table_scan_branches_get`. `Client.table_scan_function_get` now reads the single branch of
  `catalog_table_scan_branches_get` (and raises `CatalogClientError` for a multi-branch table); the client's
  `MethodNotImplemented` fallback is gone.
- **`catalog_table_create` returns `TableCreateResponse(created: bool not null)`** (new in `vgi.protocol`).
  `CatalogInterface.table_create` may return `False` when `on_conflict` is `IGNORE` and the table already
  existed; `True` or `None` (the old return) means it was created. The extension uses it so
  `CREATE TABLE IF NOT EXISTS ... AS SELECT` no longer appends into an existing table.
  `Client.table_create` returns the flag. `MetaWorker` (several catalogs in one process) relays the routed
  worker's reply instead of dropping it.
- **The write-options batch carries only `result_mode`.** DuckDB rewrites every `INSERT ... ON CONFLICT` /
  `INSERT OR REPLACE` into a MERGE before the catalog sees it, so `on_conflict` / `on_conflict_columns` were only
  ever `"throw"` / `[]`. `WriteOptions.on_conflict` and `WriteOptions.on_conflict_columns` remain as deprecated
  read-only properties with those values; they are no longer constructor arguments.
- Codegen: every SDK's generated registry and schemas drop the removed method and gain the
  `catalog_table_create` result; vgi-java gains the generated `TableCreateResponse` record.

### Protocol 3.3: table scans and write functions are table-only

- **Requires the matching DuckDB extension.** The protocol version is now `vgi.v3` / `3.3.0`; peers match on
  major and minor, so 3.3 workers and 3.2 clients reject each other.
- `FunctionInfo` gains `visibility` (`"public"`, the default, or `"table_only"`), with the constants
  `FUNCTION_VISIBILITY_PUBLIC`, `FUNCTION_VISIBILITY_TABLE_ONLY` and `FUNCTION_VISIBILITIES` in `vgi.catalog`.
  See `docs/protocol/function-visibility.md`.
- `ReadOnlyCatalogInterface` now lists every function a `Table` names (scan, insert, update, delete) even when it is
  not in the schema's `functions`, so DuckDB always has its capabilities (projection/filter pushdown, row ids,
  `has_finalize`, required secrets). Scans that were not listed used to run with every capability off.
- **User-visible:** write functions are always table-only, and a table's scan is table-only unless the table sets
  the new `Table(expose_scan=True)` or the scan class is also registered in a schema's `functions`. Table-only
  functions disappear from `duckdb_functions()` and cannot be called from SQL.
- The requirement to repeat an UPDATE/DELETE table's scan in the schema's `functions` is gone (it is listed for you;
  a scan repeated there stays public).
- **Hand-written catalogs** (custom `schema_contents` / `table_scan_function_get`) must list every function their
  tables name in the function listing, or the extension refuses the scan with a Binder Error naming the function. Set
  `visibility=FUNCTION_VISIBILITY_TABLE_ONLY` on the ones that should not be SQL functions.
- `Schema.to_schema_info` counts the tables' functions in `estimated_object_count`.
