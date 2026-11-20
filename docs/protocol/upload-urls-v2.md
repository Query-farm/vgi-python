# Upload URLs v2 (protocol 3.4)

Status: **normative**. Introduced in `vgi.v3` / `3.4.0`.

Protocol 3.4.0 is a minor bump of `vgi.v3`. Clients and workers match on exact major and minor, so 3.4 peers
refuse 3.3 peers. Nothing else in the VGI protocol changes in 3.4. The bump marks the vgi-rpc transport change
below, which every peer must make at the same time.

## What changed

Over HTTP, a request larger than the worker's `VGI-Max-Request-Bytes` goes up through a server-vended pre-signed
URL. Before 3.4 those URLs were unsized (`__upload_url__/init` took a `count`), so storage accepted any bytes.
Since vgi-rpc upload-URL protocol v2:

- the worker advertises `VGI-Upload-URL-Support: 2` (it was `true`);
- `__upload_url__/init` takes `uploads: list<struct<size: int64, sha256: utf8>>`, one entry per upload. Each
  vended PUT URL is signed for exactly that size and, where storage can check it (S3, R2, the blob vendor),
  that SHA-256. The response adds `upload_headers` (`map<utf8, utf8>`), the headers the PUT must carry;
- the old `count` request is refused with HTTP 400 (`retired count shape`), and a client refuses a `true` (v1)
  server (`upload protocol is too old`);
- a request pointer must carry `vgi_rpc.location.sha256`. A worker refuses one without it before fetching
  (`missing vgi_rpc.location.sha256`) and verifies the fetched bytes against it (`sha256 mismatch`). In 3.4 the
  DuckDB extension also requires the digest on result pointers.

The wire contract (schemas, refusals, per-backend signed headers) is specified in vgi-rpc-python's
`docs/WIRE_PROTOCOL.md`, section *Upload URL generation*.

## In vgi-python

- `vgi.client.Client` externalizes an oversized input with the v2 flow: it hashes, requests the URL, PUTs on a
  bare client with `upload_headers`, and puts the digest on the pointer.
- `vgi-serve --http` with `VGI_BLOB_VENDOR_URL` vends sized vendor grants (`POST /v1/blobs/put`,
  `purpose: "rpc"`) through `vgi_rpc.vendor.VendorStorage`.
- `vgi.http.demo_storage.DemoBlobStorage.generate_upload_url(size, sha256_hex)` checks the PUT the way S3 does
  (`400 IncompleteBody`, `403 SignatureDoesNotMatch`, `400 BadDigest`).
