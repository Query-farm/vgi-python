---
description: "Keep a self-hosted worker's large values in your own S3, S3-compatible, or GCS bucket instead of the blob vendor."
---

# Keep large values in your own bucket

A worker served with `vgi-serve --http` can move large values out of its HTTP
responses and requests: results over a size threshold, large arguments, stored
stream state, and large function-storage values. Orchard-hosted workers use
the Query.Farm blob vendor for this. A self-hosted worker can use its
operator's own bucket instead: AWS S3, any S3-compatible service (MinIO, Ceph,
RustFS, Cloudflare R2, Wasabi, Backblaze B2), or Google Cloud Storage. The
worker signs the URLs itself; no vendor is involved.

You configure it with environment variables. Every VGI SDK reads the same
names.

## Quick start: S3

```bash
pip install 'vgi[http,s3]'
export VGI_STORAGE_S3_BUCKET=my-vgi-bucket
export VGI_STORAGE_S3_PREFIX=vgi/
export AWS_REGION=eu-west-1          # or VGI_STORAGE_S3_REGION
vgi-serve my_worker:MyWorker --http
```

Credentials come from the standard AWS chain: `AWS_*` environment variables,
a profile (including `role_arn` and `credential_process`), web identity
(EKS IRSA, GitHub OIDC), container credentials, or EC2 instance metadata. To
use keys that differ from the process's AWS identity, set
`VGI_STORAGE_S3_ACCESS_KEY_ID`, `VGI_STORAGE_S3_SECRET_ACCESS_KEY` and, for
temporary keys, `VGI_STORAGE_S3_SESSION_TOKEN`.

Add a lifecycle rule that expires the prefix after one or two days. The
worker never deletes objects.

## S3-compatible services

```bash
export VGI_STORAGE_S3_BUCKET=vgi
export VGI_STORAGE_S3_ENDPOINT=https://minio.internal:9000
export VGI_STORAGE_S3_REGION=us-east-1     # "auto" for Cloudflare R2
```

With an endpoint set, the bucket goes in the URL path
(`VGI_STORAGE_S3_FORCE_PATH_STYLE`, default `true` with an endpoint), and
boto3's automatic CRC32 checksum on uploads is turned off, because several
S3-compatible services reject it. Set `AWS_REQUEST_CHECKSUM_CALCULATION` to
override that.

If DuckDB clients reach storage at a different address than the worker does
(for example `http://minio:9000` inside Docker and `https://s3.example.com`
outside), set `VGI_STORAGE_S3_PUBLIC_ENDPOINT` to the outside address. It is
used only in the signed URLs handed to clients.

Signed URLs over plain `http://` are refused unless you set
`VGI_STORAGE_ALLOW_PLAIN_HTTP=1`. The DuckDB extension needs its
`vgi_storage_allow_plain_http` setting too.

### Services that don't enforce SHA-256

Upload grants rely on the service rejecting a body whose
`x-amz-checksum-sha256` doesn't match. For a service that doesn't enforce it,
set `VGI_STORAGE_S3_CHECKSUM=none`. The worker then issues no upload grants:
large arguments and requests stay inline. Results and stored state still go to
the bucket, and their readers check the SHA-256 themselves.

## GCS

```bash
pip install 'vgi[http,gcs]'
export VGI_STORAGE_GCS_BUCKET=my-vgi-bucket
export VGI_STORAGE_GCS_PREFIX=vgi/
```

Credentials come from Application Default Credentials, or from a
service-account key file in `VGI_STORAGE_GCS_CREDENTIALS_FILE`. Signed URLs
are V4. `VGI_STORAGE_GCS_ENDPOINT` points at an emulator or another
GCS-compatible endpoint.

## All settings

| Variable | Meaning | Default |
|---|---|---|
| `VGI_STORAGE_BACKEND` | `vendor`, `s3`, `gcs` or `none` | Inferred: `vendor` if `VGI_BLOB_VENDOR_URL` is set, then `s3`, then `gcs`, else `none` |
| `VGI_STORAGE_S3_BUCKET` | Bucket (required for `s3`) | |
| `VGI_STORAGE_S3_PREFIX` | Key prefix | empty |
| `VGI_STORAGE_S3_ENDPOINT` | Custom endpoint URL | AWS |
| `VGI_STORAGE_S3_REGION` | Signing region | `AWS_REGION`, `AWS_DEFAULT_REGION`, `us-east-1` |
| `VGI_STORAGE_S3_FORCE_PATH_STYLE` | Bucket in the URL path | `true` with an endpoint |
| `VGI_STORAGE_S3_PUBLIC_ENDPOINT` | Endpoint used only in signed URLs | the endpoint |
| `VGI_STORAGE_S3_CHECKSUM` | `sha256` or `none` | `sha256` |
| `VGI_STORAGE_S3_ACCESS_KEY_ID`, `_SECRET_ACCESS_KEY`, `_SESSION_TOKEN` | Explicit credentials | AWS chain |
| `VGI_STORAGE_S3_VERIFY` | `1`: check the bucket at startup | checked on first use |
| `VGI_STORAGE_GCS_BUCKET`, `_PREFIX`, `_ENDPOINT`, `_CREDENTIALS_FILE` | GCS bucket and options | ADC |
| `VGI_STORAGE_URL_TTL_SECONDS` | Signed URL lifetime | 3600 |
| `VGI_STORAGE_ALLOW_PLAIN_HTTP` | Allow `http://` signed URLs | off |
| `VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS` | Fail a transfer when no bytes move for this long | 30 |

Setting more than one of `VGI_BLOB_VENDOR_URL`, `VGI_STORAGE_S3_BUCKET` and
`VGI_STORAGE_GCS_BUCKET` without `VGI_STORAGE_BACKEND` stops the worker at
startup with a message naming them.

A signed URL never outlives the credentials that signed it: with temporary
credentials, its lifetime is the smaller of `VGI_STORAGE_URL_TTL_SECONDS` and
the credentials' remaining lifetime minus 60 seconds. Before signing an upload
grant, the worker refreshes credentials that have less than 15 minutes left.

## The bucket check

On first use, or at startup with `VGI_STORAGE_S3_VERIFY=1`, the worker sends a
HEAD request for the bucket. If it fails, the error says what to fix: the
bucket doesn't exist, access is denied (naming where the credentials came
from), or the bucket is in a different region (naming that region).

## Next steps

- [Persist state across workers](state-storage.md) covers function storage,
  whose large values also go to this bucket with `VGI_STORAGE_EXTERNALIZE=1`.
- [Upload URLs v2](../protocol/upload-urls-v2.md) is the normative description
  of upload grants.
