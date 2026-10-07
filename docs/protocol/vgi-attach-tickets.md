# VGI Attach Tickets

**Status:** Normative. Reference implementation: vgi-python `vgi/attach_ticket.py`.
**Protocol:** `vgi.attach_tickets.v1`, version `1.0.0`. No change to `vgi.v2`.
**Vectors:** [`vgi/_test_fixtures/attach_ticket_vectors.json`](https://github.com/Query-farm/vgi-python/blob/main/vgi/_test_fixtures/attach_ticket_vectors.json)

An SDK implements attach tickets from this page alone. The words MUST, MUST
NOT and SHOULD are used as in RFC 2119.

## 1. Purpose

An attach ticket lets a runner reattach a user's catalog later, as that user,
without ever seeing the user's attach options. While the user is attached and
logged in, the client calls `seal_attach` on the worker. The worker seals the
options the user attached with, secret ones included, into a ticket that only
this worker can open. Later a runner attaches with one option,
`vgi_attach_ticket`, and the worker restores the original attach before any
catalog code runs.

A ticket says *what* to attach. A sealed grant (`vgi_rpc.Identity.v1`
`issue_grant`) says *who* attaches. The two are separate on purpose:

- **A ticket carries no authority.** It opens only under the caller's
  principal, so it attaches nothing without a grant (or login) for the same
  principal. Issuing one needs no fresh login, and a leaked ticket alone
  exposes nothing usable.
- **A ticket adds no authentication path.** Secret options never enter
  `AuthContext`.

## 2. Token format

```
ticket   = "vgia1." base64url_nopad( envelope )
envelope = version(1) = 0x01 || nonce(24) || XChaCha20-Poly1305(key, nonce, payload, aad)   ; ciphertext || tag(16)
key      = normalize(VGI_SIGNING_KEY)
aad      = "vgi.attach_ticket.v1" 0x00 || UTF-8(principal)
```

- **Envelope.** The same XChaCha20-Poly1305 construction and
  `version || nonce || ciphertext || tag` layout every SDK already uses to seal
  `attach_opaque_data` (vgi-rpc's `crypto.seal_bytes`). The version byte is
  `0x01`, fixed by this format and independent of the attach envelope's own
  version. The nonce MUST be 24 fresh random bytes per ticket.
- **Key.** The worker's `VGI_SIGNING_KEY`, as bytes (the UTF-8 of the
  environment value), normalized exactly as for the attach envelope: a 32-byte
  key is used as-is; any other length is replaced by `SHA-256(key)`.
- **AAD.** The **principal only**. The attach envelope binds
  `(auth domain, principal)`; a ticket MUST NOT, because it is sealed while the
  user is logged in (domain `jwt`, say) and opened when a runner presents their
  grant (domain `grant`). The principal is the same in both, since a grant is
  minted for its caller's principal. The distinct AAD prefix means an attach
  envelope never opens as a ticket, nor the reverse.
- **Encoding.** Unpadded base64url (RFC 4648 §5). Only the canonical spelling
  is valid: no `=`, no `+` or `/`, length mod 4 ≠ 1, and zero trailing bits
  (re-encoding the decoded bytes MUST give the same text).
- **Size.** A verifier refuses a ticket longer than 32768 characters before
  decoding it.

### 2.1 Payload

All integers little-endian. A *string* is a `u16` byte length followed by
that many bytes of UTF-8.

| # | Field | Encoding | Rule |
| --- | --- | --- | --- |
| 1 | `issued_at` | int64 | Unix seconds |
| 2 | `expires_at` | int64 | Unix seconds; `0` means no expiry; otherwise `> issued_at` |
| 3 | `ticket_id` | string | Exactly 32 lowercase hex characters (16 random bytes) |
| 4 | `catalog_name` | string | Non-empty |
| 5 | `data_version_spec` | string | `""` means none (`null` on the wire) |
| 6 | `implementation_version` | string | `""` means none (`null` on the wire) |
| 7 | `options` | `u32` length + bytes | Arrow IPC stream of the one-row options record, exactly as `CatalogAttachRequest.options` carries it; length `0` means no options; at most 16384 bytes |

The options bytes are opaque to the ticket format. A port seals the bytes it
received (or its own serialization of the same record) and, on redemption,
deserializes them as it would `CatalogAttachRequest.options`.

## 3. Verifying a ticket

Steps in this order. Every failure before step 6 is
`attach_ticket_invalid`, with no distinction between causes.

1. **Prefix.** The text starts with `vgia1.`.
2. **Length.** At most 32768 characters.
3. **Base64url.** The remainder is canonical unpadded base64url (§2).
4. **Caller.** The caller is authenticated and has a non-empty principal.
   An anonymous caller never opens a ticket.
5. **Open.** Open the envelope with the worker's key and
   `aad = "vgi.attach_ticket.v1" 0x00 || UTF-8(caller principal)`, expecting
   version `0x01`. Any failure (short envelope, wrong version, wrong key, wrong
   principal, tampering) is `attach_ticket_invalid`. **There is no separate
   principal comparison:** the AAD is the check.
6. **Parse.** Strictly, per §2.1: exact lengths, strict UTF-8, no trailing
   bytes, the field rules in the table. Failure is `attach_ticket_invalid`.
7. **Lifetime.** With `skew = 60` seconds and `now` the current Unix time:
   `issued_at > now + skew` (not yet valid) or, when `expires_at ≠ 0`,
   `now ≥ expires_at + skew` (expired) is `attach_ticket_expired`.

The lifetime is inside the ciphertext, so it is trusted only after step 5.
`attach_ticket_expired` is therefore only ever reported for an authentic
ticket presented by its own principal.

## 4. Errors

Errors use vgi-rpc's error model: `vgi_rpc.error_code`, `vgi_rpc.error_kind`
and `vgi_rpc.error_details`.

| `error_kind` | `error_code` | Details | When |
| --- | --- | --- | --- |
| `attach_ticket_invalid` | `INVALID_ARGUMENT` | `BadRequest`, field `vgi_attach_ticket` | §3 steps 1–6, a non-string ticket value, or a worker without a signing key |
| `attach_ticket_expired` | `FAILED_PRECONDITION` | `PreconditionFailure`, type `ATTACH_TICKET`, subject `vgi_attach_ticket` | §3 step 7 |
| `invalid_request` | `INVALID_ARGUMENT` | `BadRequest`, one violation per bad field | Another option beside the ticket (§6); a `seal_attach` request that fails validation (§5.3) |
| `action_denied` | `PERMISSION_DENIED` | `ErrorInfo`, `metadata.action = "seal_attach"` | Anonymous `seal_attach`, or a worker refusing by policy |

No error message, log line or detail ever contains the ticket text or a
restored option value.

## 5. `vgi.attach_tickets.v1`

### 5.1 Hosting

A worker hosts the protocol when **all** of these hold, and otherwise does not
host it at all (absent, not hosted-and-refusing, so a client learns the answer
from `vgi_rpc.Reflection.v1` `list_protocols`):

1. The transport is HTTP. Other transports have no caller identity.
2. `VGI_SIGNING_KEY` is configured explicitly. A key the process generated for
   itself MUST NOT enable the protocol: every ticket would die on restart. (A
   pre-fork server that generates one key and hands it to its children marks
   it as generated; vgi-python uses the private `VGI_SIGNING_KEY_MINTED=1`.)
3. The worker can issue grants: grant keys are configured
   (`VGI_RPC_GRANT_KEYS`) or the worker supplies its own `mint_grant`. A ticket
   is useless without a grant.

It is hosted beside `vgi.v2` and never changes how a `vgi.v2` request is
dispatched. Wire name `vgi.attach_tickets.v1`, `protocol_version = "1.0.0"`.

### 5.2 Shapes

One unary method, `seal_attach(request: SealAttachRequest) -> AttachTicket`.

`SealAttachRequest`:

| Field | Arrow type | Meaning |
| --- | --- | --- |
| `catalog_name` | `utf8` not null | The catalog the caller attached |
| `options` | `binary` nullable | Arrow IPC stream of the one-row options record, as `CatalogAttachRequest.options`; null for none |
| `data_version_spec` | `utf8` not null | As given at ATTACH; `""` for none |
| `implementation_version` | `utf8` not null | As given at ATTACH; `""` for none |
| `ttl_seconds` | `int64` not null | Requested lifetime; `0` = as long as the worker allows |

`AttachTicket`:

| Field | Arrow type | Meaning |
| --- | --- | --- |
| `ticket` | `utf8` not null | The `vgia1.` text |
| `expires_at` | `float64` not null | Unix seconds; `+inf` when the ticket has no expiry |

### 5.3 Rules

In this order:

1. **Caller.** Anonymous (unauthenticated, or no principal) is
   `action_denied`. The principal sealed is always the caller's; there is no
   subject parameter.
2. **No freshness requirement.** Unlike `issue_grant`, a stale login may seal.
3. **Validation**, all violations reported together as `invalid_request`
   with one `BadRequest` field violation each:
   - `ttl_seconds < 0` → field `ttl_seconds`.
   - `options` with more than one row → field `options`. Zero rows or null
     means no options.
   - `catalog_name` not among the worker's catalogs (`catalog_catalogs`) →
     field `catalog_name`.
   - An option whose name (case-insensitive) is `vgi_attach_ticket` → field
     `options.<name>`.
   - An option the catalog does not declare in its `attach_option_specs`
     (names compared case-insensitively) → field `options.<name>`.
   - A declared `required` option missing → field `options.<spec name>`.
   - Options larger than 16384 bytes once serialized → field `options`.

   Validation never attaches: it has no side effects. Option *values* are not
   type-checked here; the catalog checks them when the ticket is redeemed.
4. **Lifetime.** Let `max` be the worker's grant maximum lifetime: the grant
   keys' `max_ttl_seconds` when grant keys are configured, otherwise
   `VGI_RPC_GRANT_MAX_TTL_SECONDS` when set, otherwise none.
   `issued_at = now` (integer seconds), and

   | `ttl_seconds` | `max` set | `expires_at` |
   | --- | --- | --- |
   | `0` | yes | `issued_at + max` |
   | `0` | no | `0` (no expiry) |
   | `> 0` | yes | `issued_at + min(ttl_seconds, max)` |
   | `> 0` | no | `issued_at + ttl_seconds` |

   So the two halves of a held entry (grant and ticket) expire together.
5. **Seal** per §2 with a fresh `ticket_id` and nonce. Return the text and
   `expires_at` (`+inf` for `0`).

A worker MAY refuse by policy with `action_denied`.

## 6. Redeeming: what `catalog_attach` does

When a `vgi.v2` `catalog_attach` request's options contain a key equal,
case-insensitively, to `vgi_attach_ticket`, the SDK framework, before any
catalog code:

1. **Rejects any other option alongside it** — including a second spelling of
   the ticket key — as `invalid_request`, one `BadRequest` violation
   `options.<name>` per extra option. The sealed options are authoritative, so
   there is nothing to merge. This check comes before the ticket is opened.
2. **Verifies the ticket** per §3 under the caller's principal and the
   worker's signing key. A non-string value, or a worker with no signing key
   (subprocess and unix transports), is `attach_ticket_invalid`.
3. **Replaces the request** with the one the user originally made:
   `name = catalog_name`, `options =` the sealed options (null when length
   0), `data_version_spec` / `implementation_version =` the sealed strings
   (null when `""`). `client_capabilities` is kept from the incoming request.
   The incoming `name` is ignored.
4. **Proceeds as a normal attach** with the restored request: the catalog's
   own `catalog_attach` runs unchanged, sealing of `attach_opaque_data`,
   lifecycle logging (`loggable_attach_options` sees the restored options as
   usual) and tracing all behave as if the user had typed it.

A worker composing several catalogs (vgi-python's `MetaWorker`) redeems
**before** routing, so the sealed catalog name, not the request's name, picks
the catalog.

**Reserved name.** `vgi_attach_ticket` is reserved: declaring an attach option
with that name (compared case-insensitively) MUST fail at worker startup.

**Never logged.** Neither the ticket nor the restored options (beyond what
`loggable_attach_options` returns) may reach a log, span, breadcrumb or error.

**Principal namespaces.** A ticket sealed for `alice` opens for any caller
whose principal is `alice`, whatever authenticated them. That is already true
of grants. A deployment whose authenticators map one string to different
people has that problem today; tickets do not add it.

**Key rotation.** Tickets live as long as `VGI_SIGNING_KEY` does. Rotating it
invalidates every ticket, as it already invalidates attach envelopes.

## 7. Fixture catalog: `ticket_probe`

Every SDK's fixture worker serves this catalog identically (in vgi-python,
`vgi/_test_fixtures/ticket_probe.py`, in both `vgi-fixture-worker` and
`vgi-fixture-http`). The cross-SDK sqllogictests in
`vgi/test/sql/integration/attach_ticket/` run against it.

- Catalog `ticket_probe`, default schema `main`.
- Attach options, in this order:

  | Name | Type | Required | Secret | Default |
  | --- | --- | --- | --- | --- |
  | `region` | `VARCHAR` | no | no | `'us-east-1'` |
  | `api_key` | `VARCHAR` | **yes** | **yes** | none |

- Table `main.probe`, backed by the table function `main.ticket_probe` (no
  arguments). Exactly one row:

  | Column | Type | Value |
  | --- | --- | --- |
  | `region` | `VARCHAR` | The attached `region`, or `'us-east-1'` |
  | `api_key_sha256` | `VARCHAR` | The first 12 lowercase hex characters of `SHA-256(UTF-8(api_key))` |

  The key itself is never returned. Option names are matched
  case-insensitively.

- Attaching without `api_key` fails (the option is required). So a reattach
  with nothing but `vgi_attach_ticket` reading the same row proves the secret
  took effect without travelling again.

For example, `api_key = 'sk-test-0123456789'` reads back as
`api_key_sha256 = '0d3b56072291'`.

### 7.1 Fixture HTTP server

The fixture HTTP server hosts `vgi.attach_tickets.v1` when started with both:

```bash
VGI_SIGNING_KEY=<any stable value> \
VGI_RPC_GRANT_KEYS=<base64 of 32 bytes> \
vgi-fixture-http
```

Its default test authenticator accepts `Bearer vgi-test-alice` (principal
`alice`) and `Bearer vgi-test-bob` (principal `bob`) as **fresh** logins (it
stamps `auth_time = now`), so a client can `issue_grant` with nothing but a
bearer token. When grant keys are configured, a `Bearer vgig1.…` falls
through to the sealed-grant authenticator; any other unknown bearer stays
anonymous as before.

## 8. Vectors

`attach_ticket_vectors.json` pins the format byte for byte. Every SDK's unit
tests MUST consume it. Keys and options are standard base64; nonces and
payloads are hex. `defaults` supplies `signing_key_b64` and `now` for cases
that omit them.

| Section | Each case gives | A port must |
| --- | --- | --- |
| `mint` | key, principal, catalog, options bytes, version specs, `issued_at`, `expires_at`, `ticket_id`, `nonce_hex`, plus `aad_hex`, `payload_hex` and `token` | Seal with exactly these inputs and produce exactly `payload_hex`, `aad_hex` and `token` |
| `accept` | `token`, `principal`, `now` | Open it and produce exactly `claims` (options compared as bytes) |
| `reject` | `token`, `principal`, optional `now` / key | Refuse it with exactly `error_kind` |
| `redeem` | the incoming options map (strings), `principal` | Apply §6 and either produce `result` (catalog name, version specs, decoded options; `null` means the request is untouched) or refuse with `error_kind` |

The reject cases cover a wrong principal, an anonymous caller, a principal
differing only in case, tampering, another worker's key, expiry and
not-yet-valid (with the 60 s skew edges in `accept`), an attach envelope
presented as a ticket (both as minted and re-sealed with the ticket version
byte), a `(domain, principal)` AAD, non-canonical / padded / standard-alphabet
base64, wrong and missing prefixes, short and over-long tickets, trailing,
truncated and overrunning payloads, oversize options, bad ticket ids, an empty
or non-UTF-8 catalog name, and an empty lifetime.

The file is generated by `scripts/gen_attach_ticket_vectors.py`; vgi-python's
`tests/test_attach_ticket.py` fails if the two drift.

## 9. Porting checklist

Each of these, removed, MUST turn a test red in your SDK:

- The principal in the AAD (or swapping in the attach envelope's
  `(domain, principal)` AAD).
- The expiry check.
- The "no other option beside the ticket" rule.
- Redeeming before routing, in a worker that composes catalogs.
- The reserved-name check on attach option declarations.
- The hosting conditions: no protocol without an explicit key, without the
  ability to issue grants, or off HTTP.
