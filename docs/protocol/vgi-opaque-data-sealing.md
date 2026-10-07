# Opaque values: sealing (normative)

Status: normative · 2026-10-07

`catalog_attach` returns `attach_opaque_data`, and `catalog_transaction_begin`
returns `transaction_opaque_data`. The client stores these values and sends
them back on every later call for that catalog or transaction. They are the
worker's own state, held by the client.

No spec said how a worker must protect them, and SDKs diverged. Two SDKs carried
every attach option, secret ones included, in plaintext. The extension logged
the raw bytes, so those secrets reached `duckdb_logs`. Three SDKs let a client
edit or replay the value to reach another catalog or session. This page states
the rule every SDK follows.

## Scope

- **Covered values:** `attach_opaque_data`, `transaction_opaque_data`, and any
  other worker-issued value a client stores and returns.
- **Authenticating transports** (HTTP, and any transport that carries a
  per-call identity): the rules below are **required**.
- **OS-owned transports** (a subprocess over stdio, a unix socket under the
  same user): the operating system owns the trust boundary, so sealing is
  optional. Even there, the "no secrets" and "never log raw" rules still apply.

## Rules

1. **Seal.** On an authenticating transport, the worker MUST seal each value
   with an AEAD cipher keyed by the deployment's signing key: the same key that
   seals HTTP state tokens. That is `VGI_SIGNING_KEY` when configured, or a key
   the worker generates at startup when it is not. An unset `VGI_SIGNING_KEY`
   never means "don't seal".
   - XChaCha20-Poly1305 is recommended, with the envelope
     `version || nonce || ciphertext || tag`, as used for state tokens.
   - The value is only ever opened by the deployment that sealed it, so the
     exact bytes need not match between SDKs. The properties below must.
   - Reference: `vgi_rpc.crypto.seal_bytes` with version byte `0x02` for both
     values, so an envelope is `0x02 || nonce(24) || ciphertext || tag(16)`.
     The key is `VGI_SIGNING_KEY`'s UTF-8 bytes, any length, normalized by
     `vgi_rpc.crypto.normalize_key`. When it is unset, `vgi-serve` mints
     `secrets.token_urlsafe(32)` (and logs a warning that values won't survive a
     restart), and `vgi-fixture-http` mints 32 random bytes.
   - Reference attach plaintext: a fresh 16-byte UUID (the storage shard key)
     followed by the catalog's own bytes. Under a `MetaWorker`, a routing header
     naming the catalog wraps it inside the seal, never outside it.
   - Anonymous callers on an authenticating transport get sealed values too,
     bound to the anonymous identity below.
2. **Bind to the caller.** The AAD MUST include the caller's identity. A value
   sealed for one caller MUST NOT open for another. The reference AAD is:

   ```text
   "vgi.attach_opaque_data.v1" 0x00 || identity
   identity = 0x01 || domain || 0x00 || principal   (authenticated caller)
            | 0x00 || "anonymous"                   (anyone else)
   ```

   `domain` and `principal` are the UTF-8 strings of the caller's auth
   context. A missing domain is empty, so `(domain, principal)` both count:
   the same principal name under another auth domain does not open the value.
3. **Bind transactions to their attach.** `transaction_opaque_data` MUST bind
   both the caller and the parent `attach_opaque_data`. The reference AAD is
   `"vgi.transaction_opaque_data.v1" 0x00 || identity || 0x00 ||
   attach_envelope`. `attach_envelope` is the *sealed* attach value exactly as
   that call presents it, not its plaintext. So a transaction minted under one
   attach can't be replayed under another, even by the same principal. That
   holds even for two attaches of the same catalog.
4. **Reject uniformly; never fall back.**
   - A value that fails to open MUST be rejected. That covers a wrong caller, a
     wrong parent attach, tampering, a malformed value, or an unknown key.
   - Every one of those cases MUST give the same, classified error:

     | Field | Value |
     |---|---|
     | `error_code` | `INVALID_ARGUMENT` |
     | `error_kind` | `opaque_data_not_recognized` |
     | message | exactly `<field> not recognized`, where `<field>` is `attach_opaque_data` or `transaction_opaque_data` |
     | `error_details` | none |

     The two fields' errors differ only in the field name. A probing caller
     learns nothing about which check failed, and no refusal is `UNKNOWN`.
   - Routing is a check too. A worker that picks a handler from the value, such
     as the reference `MetaWorker`, MUST answer an unroutable value with this
     same error. It MUST NOT describe the failure or list what it does serve.
   - Reference: `vgi.exceptions.OpaqueDataNotRecognizedError`, a `ValueError`
     subclass that declares `error_code` and `error_kind` for vgi-rpc's error
     model. vgi-rpc's Python server puts the exception class name in front of
     every log message, so the reference's `vgi_rpc.log_message` reads
     `OpaqueDataNotRecognizedError: <field> not recognized`. The bare message
     travels as `exception_message` in `vgi_rpc.log_extra`. The conformance
     group strips a leading `"<error_type>: "` before comparing, so an SDK that
     sends the bare message passes too.
   - There is **no plaintext fallback**: no prefix, flag or missing call context
     may skip the open.
5. **No secrets in plaintext.** Attach options declared `secret=True` MUST NOT
   appear in plaintext in any opaque value.
   - On an authenticating transport the seal satisfies this.
   - On an OS-owned transport that skips sealing, leave secret options out of
     the value. Re-derive them or keep them server-side.
   - Reference: the framework never copies attach options into the value. It
     wraps only the catalog's own bytes, so this rule binds the catalog
     implementation. A catalog that keeps options in its attach bytes MUST drop
     the secret ones, or keep only a digest. The `attach_options` fixture
     catalog echoes only its declared non-secret options. The `ticket_probe`
     fixture keeps `region` and `sha256(api_key)[:12]`, never the key.
6. **Unpredictable identifiers.** Any id inside an opaque value (session id,
   attach uuid) MUST come from a cryptographically secure random source, never
   a seeded PRNG such as `mt19937`. The reference uses `uuid.uuid4()`.
7. **Never log raw.** Workers and clients MUST NOT log, trace or report the raw
   bytes, the full hex, or a raw-hex *prefix* of an opaque value. That covers
   logs, OTel attributes, Sentry tags and breadcrumbs, and error messages.
   - Log a short hash instead: the first 12 hex characters of SHA-256 over the
     value's **lowercase hex text**, `sha256(hex(value).encode()).hexdigest()[:12]`.
   - Hashing the hex text rather than the bytes matches
     `vgi_rpc.sentry.short_hash`. The same value then carries the same token in
     the worker's logs and in vgi-rpc's Sentry tags, and an SDK that hashes the
     same way can be correlated with it. The reference helper is
     `vgi._redact.short_hash`.
   - See [Known gap](#known-gap) for the one place a secret still reaches a log.

## Transports without sealing (reference)

With no signing key, the reference stores and opens values unchanged. That
covers a subprocess over stdio and a unix socket. `attach_opaque_data` is then
`uuid(16) || catalog bytes`, and any caller on that transport can use any
value. The operating system's process ownership is the boundary. Rules 5, 6
and 7 still apply. A sealed value presented to an unsealed worker, or the
reverse, is rejected or mis-read; values never cross transports.

## Known gap

vgi-rpc's access log, at `DEBUG`, records each request's full payload as
base64 in `request_data`. A `catalog_attach` request carries its attach
options, so a secret option reaches that log in plaintext. Other requests put
sealed values in it. Rule 7 does not yet hold there, and the fix belongs in
vgi-rpc, for example by redacting `secret=True` options and opaque fields from
`request_data`. Until it lands, treat a `DEBUG` access log as secret-bearing
and don't enable it in production. At `INFO` and above the payload is
omitted.

## Rotation

Rotating the signing key invalidates every outstanding value. Clients then get
`not recognized` and must re-attach. This is intended.

## Conformance

`tests/sdk_conformance/test_opaque_sealing.py` in vgi-python is the
cross-SDK group. It runs against any SDK's fixture worker over HTTP and drives
it with vgi-python's `Client`, so the test controls every opaque byte. It
checks that:

- a value attached as one principal and replayed as another is rejected, for
  both attach and transaction values;
- a value with one flipped byte (first, middle or last) is rejected, for both
  attach and transaction values;
- a transaction value replayed under a different attach of the same catalog,
  by the same principal, is rejected;
- forged values in every shape an SDK produced unsealed are rejected: a bare
  uuid, `uuid || catalog bytes`, a `writable:` prefix, an Arrow IPC options
  batch, JSON, and an envelope-shaped random value;
- every refusal has `error_code` `INVALID_ARGUMENT`, `error_kind`
  `opaque_data_not_recognized`, the exact message `<field> not recognized`
  and no details. Within a field all refusals are identical, and the two
  fields differ only in the field name;
- a `secret=True` attach option is absent from the attach and transaction
  values, as bytes, as hex, and as base64;
- a `secret=True` attach option is absent from the *unsealed* values on
  stdio too. This is `test_opaque_sealing_stdio.py`, enabled by
  `VGI_SDK_STDIO_WORKER=<worker command>`;
- optionally, the worker's log holds neither the secret nor any 12-byte window
  of a value's hex, nor its base64.

The owner's own use of each value is checked first, so a worker that rejects
everything does not pass. Probes go through `catalog_version`, which every
worker must answer by opening both values.

### Running it

The group skips entirely unless `VGI_SDK_HTTP_URL` is set, so a plain `pytest`
stays green.

| Variable | Meaning |
|---|---|
| `VGI_SDK_HTTP_URL` | Base URL of the running fixture worker (required). |
| `VGI_SDK_BEARER_A`, `VGI_SDK_BEARER_B` | Bearers for two distinct principals. The defaults are `vgi-test-alice` and `vgi-test-bob`, the fixture test bearers. |
| `VGI_SDK_CATALOG` | Catalog for the replay, tamper and transaction cases. The default is the first that attaches with no options; `example` is tried first. |
| `VGI_SDK_SECRET_CATALOG` | Catalog with a secret option. The default is `ticket_probe`, then any catalog with a secret option whose required options are strings. With none, that case skips. |
| `VGI_SDK_PLAINTEXT_HEX` | Extra forged values, as comma-separated hex: the exact shape this SDK would produce unsealed. |
| `VGI_SDK_STDIO_WORKER` | Worker command for the stdio rule-5 check, in its own module that skips when this is unset. Use the worker that serves the secret-option catalog. |
| `VGI_SDK_WORKER_LOG` | Path of the worker's captured stdout and stderr. Set it to enable the log check. |

Each SDK's CI starts its fixture worker with `--http`, reads the `PORT:<n>`
line, and runs the group from a vgi-python checkout:

```bash
"$FIXTURE_WORKER" --http >worker.log 2>&1 &
until grep -q '^PORT:' worker.log; do sleep 0.2; done
port=$(sed -n 's/^PORT://p' worker.log | head -1)

git clone --depth 1 https://github.com/Query-farm/vgi-python.git vgi-python
cd vgi-python && uv sync --all-extras
VGI_SDK_HTTP_URL="http://127.0.0.1:$port" \
VGI_SDK_WORKER_LOG="$OLDPWD/worker.log" \
    uv run pytest tests/sdk_conformance -q
```

| SDK | `$FIXTURE_WORKER` |
|---|---|
| Python | `vgi-fixture-http --port 0`, which is HTTP already: drop `--http` |
| Go | `vgi-example-worker-go` (`go build ./cmd/vgi-example-worker`) |
| Rust | `target/debug/vgi-example-worker` |
| C# | `fixtures/QueryFarm.Vgi.ExampleWorker/bin/Debug/net10.0/vgi-example-worker` |
| C++ | the fixture worker, run with `VGI_FIXTURE_TEST_BEARERS=1` so it accepts the test bearers |
| Java, TypeScript | the SDK's example fixture worker |

A worker that refuses correctly but with another code, kind or message
fails with a list of exactly what differs. If the two bearers resolve to the
same principal, or both to anonymous, the
replay cases fail. The assertion says so. A worker without a secret-option
catalog skips that one case and names the reason.
