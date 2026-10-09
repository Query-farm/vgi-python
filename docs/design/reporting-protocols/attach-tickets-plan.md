# Plan: attach tickets and `vgi_export_session()`

Status: original implementation plan · shipped (see [IMPLEMENTATION.md](IMPLEMENTATION.md))

Reporting integration revised 2026-10-07: the export's location and catalog
name do not uniquely identify an attachment. The reporting client adds an
explicit attachment reference and stores distinct catalog delegations as
specified in [credentials.md](credentials.md). Grant-only service delegations
use Identity directly. Neither addition changes the shipped ticket format,
`seal_attach`, or `vgi_export_session()` schema described below.

Goal: serialize a user's attached DuckDB session so any runner can reattach it
later as that user, without seeing the user's secrets. Design context is in
[credentials.md](credentials.md). This plan covers the worker side (every SDK)
and the extension. vgi-rpc needs no change: tickets are a VGI artifact, sealed
the way VGI already seals `attach_opaque_data`.

## Decisions to confirm

| # | Decision | Recommendation | Why |
| --- | --- | --- | --- |
| T1 | A `vgi.v2` method, or a separate protocol? | **Separate: `vgi.attach_tickets.v1`** | `vgi.v2` is version-gated on exact major+minor. A new `vgi.v2` method means 2.2.0, and every SDK plus the extension must release in lockstep, as `catalog_contents` did. A separate protocol is optional per worker (D1), discovered by reflection, and needs no `vgi.v2` change. |
| T2 | How the ticket reaches `catalog_attach` | **A reserved ATTACH option, `vgi_attach_ticket`**, opened by the SDK framework before the catalog's own `catalog_attach` runs | `CatalogAttachRequest.options` is already an open record, so the `vgi.v2` wire is unchanged. The extension sends it only to workers whose reflection lists `vgi.attach_tickets.v1`, so an old worker never silently ignores it. |
| T3 | Where sealing lives | **Each SDK's existing attach-envelope sealer** (Python `_seal_attach`, Java `OpaqueDataSealer`, C++ `seal_attachment`, …), keyed by `VGI_SIGNING_KEY`, with a ticket-specific AAD | Every SDK already seals `attach_opaque_data` with an AEAD bound to the caller, using vgi-rpc's crypto primitive where the port exposes one. Tickets are the same kind of VGI artifact. No vgi-rpc change, no new key, no new cipher. |
| T4 | What gets sealed | **The options the extension attached with**, sent by the extension; validated against the catalog's declared `AttachOptionSpec`s, not by a trial attach | The extension holds the original values, secrets included. Stateless workers don't keep them. Validation without attaching has no side effects. |
| T5 | Ticket and grant separate | **Yes** | See [credentials.md](credentials.md#two-parts-deliberately): a ticket carries no authority, so it needs no fresh login, never enters `AuthContext`, and adds no authentication path. |

## Token format

```
ticket   = "vgia1." base64url_nopad( envelope )
envelope = the SDK's attach envelope: version || nonce || ciphertext || tag, under VGI_SIGNING_KEY
aad      = "vgi.attach_ticket.v1" 0x00 || UTF-8(principal)
```

**Same sealer, different AAD.** `attach_opaque_data` binds
`(auth domain, principal)`. A ticket must bind the **principal only**, because
it's sealed while the user is logged in (domain `jwt`, say) and opened when a
runner presents their grant (domain `grant`). The principal is the same in both:
a grant is minted for its caller's principal. Binding the principal in the AAD
also makes the principal check free: a ticket opened under any other principal
fails authentication, so there is no separate comparison to forget.

The prefix keeps tickets distinguishable from other strings and versioned. The
distinct AAD domain means an attach envelope can never be opened as a ticket, or
the reverse.

**Payload** (integers little-endian; strings are `u16` length plus UTF-8):

| Field | Encoding |
| --- | --- |
| `issued_at`, `expires_at` | int64 seconds (`expires_at = 0` means no expiry) |
| `ticket_id` | string (32 hex) |
| `catalog_name` | string |
| `data_version_spec`, `implementation_version` | string ("" = none) |
| `options` | `u32` length plus Arrow IPC bytes of the options record, exactly as in `CatalogAttachRequest.options` |

Capped at 16 KiB of options. Verification: prefix, canonical base64url, open
under the caller's principal (failure is `attach_ticket_invalid`), strict
parse, then lifetime with 60 s skew.

**Key and lifetime.**

- Tickets live as long as `VGI_SIGNING_KEY` does. Rotating it invalidates every
  ticket, as it already invalidates attach envelopes.
- The protocol is hosted only when the signing key is configured explicitly. A
  per-process generated key would make tickets die on restart.
- The maximum lifetime follows the worker's grant maximum, so the two halves of
  a delegation expire together.

**Principal namespaces must be consistent per worker.** A ticket sealed for
`alice` opens for any caller whose principal is `alice`, whatever domain
authenticated them. That's already true of grants, and a deployment whose
authenticators map the same string to different people has that problem today.
The spec says so.

## `vgi.attach_tickets.v1`

One method. Hosted on HTTP when `VGI_SIGNING_KEY` is set explicitly and the
worker can issue grants (grant keys, or its own `mint_grant`), because a ticket
is useless without a grant.

```python
@dataclass
class SealAttachRequest:
    catalog_name: str
    options: bytes                    # Arrow IPC record, as CatalogAttachRequest.options
    data_version_spec: str = ""
    implementation_version: str = ""
    ttl_seconds: int = 0              # 0 = as long as the worker allows

@dataclass
class AttachTicket:
    ticket: str
    expires_at: float                 # +inf when the worker sets no maximum
```

| Method | Rules |
| --- | --- |
| `seal_attach(request) -> AttachTicket` | Caller must be authenticated (anonymous is `action_denied`). The principal sealed is the caller's. Options are validated against the catalog's declared options (`invalid_request` with `BadRequest`). No freshness requirement. A worker may refuse by policy (`action_denied`). |

## Reattach: what every SDK's `catalog_attach` does

When `options` contains `vgi_attach_ticket`, the framework, before any catalog
code:

1. Rejects any other catalog option alongside it (`invalid_request`). The sealed
   options are authoritative, so there is nothing to merge.
2. Verifies the ticket: an invalid one is `invalid_request`
   (`attach_ticket_invalid`); an expired one is `FAILED_PRECONDITION`
   (`attach_ticket_expired`).
3. The principal check is the open itself: the AAD carries the caller's
   principal, so a ticket presented by anyone else fails as
   `attach_ticket_invalid`. A stolen ticket is useless without that user's
   grant.
4. Calls the catalog's `catalog_attach` with the sealed `catalog_name`, options
   and version specs, as if the user had typed them. Catalog code needs no
   change.

`vgi_attach_ticket` becomes a reserved option name: declaring an `AttachOption`
with it fails at startup. Tickets and restored secret options are never logged,
and `loggable_attach_options` sees the restored options as usual.

## Extension

**`vgi_export_session(aliases := NULL, ttl_seconds := NULL)`**, a table
function. For each attached catalog (or each alias given):

1. Not `TYPE vgi`: emit the row with `status = 'not_vgi'`.
2. Reflection on the worker: if `vgi.attach_tickets.v1`, or `issue_grant` on
   `vgi_rpc.Identity.v1`, is missing, emit `not_supported`.
3. Call `issue_grant(purpose = "vgi.unattended", scopes = [], ttl_seconds)`
   using the catalog's existing auth. A `stale_auth` refusal is
   `stale_login`, so Cupola can step up and retry.
4. Call `seal_attach` with the catalog name, the **original** option values
   (secrets included) and the version specs.
5. Emit `alias, location, catalog_name, grant, ticket, expires_at, status,
   message`.

Per-row failures never abort the export.

**`attach_ticket` ATTACH option.**

- It's a secret option, like `bearer_token`: redacted from `duckdb_databases()`,
  never logged, and folded into the cache identity only as a salted hash.
- Before `catalog_attach`, the extension checks reflection for
  `vgi.attach_tickets.v1` and fails clearly if it's absent.
- It sends the ticket as the `vgi_attach_ticket` option, and nothing else from
  the ATTACH options.

**Prerequisites in the extension:**

- **Keep the original attach option values per catalog,** secrets included,
  in memory only. They're needed for `seal_attach`. Verify whether today's
  reconnect path already does; if not, add it.
- **Call non-`vgi.v2` protocols** through the existing RPC client, using the
  per-protocol routing key, for `Identity.v1` and `vgi.attach_tickets.v1`.
- **Work in the wasm build.** Cupola is where exports happen, so the HTTP path
  must work under Emscripten.

## Work and order

| Step | Repo | Work |
| --- | --- | --- |
| 0 | vgi (extension), vgi-python | **Reflection in the clients.** Every worker hosts `vgi_rpc.Reflection.v1`, but no VGI client calls it. Extension: a `vgi_protocols(...)` table function (by location like `vgi_catalogs()`, or by attached catalog alias) returning `protocol_name, protocol_version, protocol_hash`, plus a cached per-catalog "hosts protocol X?" check for the extension's own features. vgi-python: `Client.list_protocols()`. The TypeScript client already has it (`vgi-rpc-typescript` `introspect`). Cross-SDK sqllogictests in `test/sql/integration/reflection/`. |
| 1 | vgi-python (reference SDK) | `vgi.attach_tickets.v1`, ticket payload, the `catalog_attach` interception, reserved option name, `attach_ticket_vectors.json`, fixture worker with a secret attach option. Spec in `vgi/docs`. |
| 2 | vgi (extension) | `vgi_export_session()`, the `attach_ticket` option, retained options, the Identity and tickets clients, wasm. sqllogictests (below). In parallel with step 1, against the Python fixture. |
| 3 | 6 other VGI SDKs | Port step 1 on each SDK's existing attach sealer and pass the same sqllogictests. No vgi-rpc releases are needed first. |
| 4 | Cupola | Export on save and on login renewal; `put_delegations` to the scheduler and alert hosts. Not part of this plan's release. |

No `vgi.v2` version bump and no vgi-rpc release, so the SDKs and the extension release independently.

## Tests

**Cross-SDK sqllogictests**, in `~/Development/vgi/test/sql/integration/attach_ticket/`,
over HTTP, against every SDK's fixture worker:

- Export, detach, then reattach with `bearer_token` plus `attach_ticket`. Query
  results match, and a secret option took effect without being in the reattach.
- A ticket presented with another principal's grant is refused.
- An expired ticket, a tampered ticket, and a ticket from another worker's key
  are refused.
- Extra options alongside `attach_ticket` are refused.
- A worker without the protocol gives a clear error and `status =
  'not_supported'`.
- `duckdb_databases()` shows the ticket redacted.
- `vgi_export_session()` on a mix of VGI and `:memory:` catalogs reports a
  status per row.

**Unit tests:** in every port and SDK, `attach_ticket_vectors.json` with exact
mint, accept and reject cases: fixed key, nonce and clock, including a
principal mismatch and an attach envelope presented as a ticket.

**Mutation checks:** drop the principal from the AAD (or use the
`(domain, principal)` attach AAD), skip expiry, drop the "no other options"
rule, and skip the reflection check before sending. Each must turn
a test red.

## Risks

- **Ticket size.** Options travel in the `catalog_attach` body, not a header,
  so a 16 KiB cap is generous. A runner storing many of them is a storage
  question, not a wire one.
- **Upstream secret rotation.** A ticket keeps the old value. The catalog's own
  error surfaces on reattach, and the owner re-exports. Nothing detects this
  early.
- **Workers without grant keys** can't issue either half, so their catalogs
  can't run unattended. That's the intended opt-in.
- **The wasm HTTP path** is the most likely place for surprises. Prototype
  `vgi_export_session()` in Cupola first.
