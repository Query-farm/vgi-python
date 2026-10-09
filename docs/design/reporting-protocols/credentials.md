# Unattended sessions

Python dataclasses and RPC interface: [generated reference](reference/delegations.md).
Lifecycle and encoding rules: [wire behavior](wire-contracts.md).

Reports, schedules and alerts read data from catalogs on other workers, and the
work may run on another host while nobody is logged in. This file defines how a
user's attached DuckDB session is **serialized** so that any runner, anywhere,
can **reattach** it later as that user, without ever seeing the user's secrets:

- **Attach tickets:** each attached worker seals the attachment's options,
  secret ones included, into a ticket only it can open, and agrees to honour
  it later.
- **Catalog delegations:** one per execution principal per attachment reference, deposited
  with whichever host runs unattended work (`vgi.delegations.v1`).
- **Service delegations:** a grant without a ticket for a standalone service,
  such as the report store. No catalog attachment is required.

Shared conventions are in [README.md](README.md#shared-conventions). The shipped
worker method and extension pieces are described in the original
[attach-tickets-plan.md](attach-tickets-plan.md). The attachment references and
service delegations below are proposed reporting-layer additions.

## Serializing a session

While the user is attached and logged in, the client runs:

```sql
SELECT * FROM vgi_export_session();
-- alias | location                  | catalog_name | grant      | ticket     | expires_at | status
-- sales | https://sales.example.com | main         | vgig1.…    | vgia1.…    | …          | ok
-- hr    | https://hr.example.com    | people       | vgig1.…    | vgia1.…    | …          | ok
-- tmp   | :memory:                  |              |            |            |            | not_vgi
```

One row per attached catalog. For each VGI attachment the extension asks that
worker for two things, using the session's own credentials:

- **A grant** (`vgi_rpc.Identity.v1` `issue_grant`): *who* the runner acts as.
- **An attach ticket** (`seal_attach`): *what* to attach. The worker seals the
  options the catalog was attached with, secret ones included, together with
  the catalog name and the caller's principal. Only that worker can open it.

Rows the worker can't or won't serialize say why in `status` (`not_vgi`,
`not_supported`, `stale_login`, `refused`) instead of failing the whole export.

A runner on any host reattaches with both, and never sees an option or a
secret:

```sql
ATTACH 'sales' (TYPE vgi, LOCATION 'https://sales.example.com',
                bearer_token 'vgig1.…', attach_ticket 'vgia1.…');
```

The grant authenticates the runner as the user. The worker opens the ticket,
checks its principal matches the grant's, and attaches with the sealed options
exactly as the user did.

## Two parts, deliberately

The grant and the ticket stay separate:

- **Grants already work everywhere.** Every port and SDK authenticates
  `vgig1.` bearers today. Tickets add no new authentication path.
- **A ticket is not a credential.** It carries no authority. Without a grant
  for the same principal it attaches nothing, so issuing one needs no fresh
  login, and one leaking on its own exposes nothing usable.
- **Secrets stay out of identity.** Secret options never enter `AuthContext`
  claims, where they could be logged or forwarded.
- **They change at different rates.** A rotated upstream API key needs a new
  ticket; a new login only needs new grants.

To the runner they are one row, held and presented together.

## The session description in a report

A report envelope carries only what's safe to share and needed to show a
"connect these sources" checklist:

The fields, defaults and Arrow types are defined by the
[Python dataclasses](reference/delegations.md).

Cupola fills the location and catalog from the non-secret export columns and
assigns a random UUID `attachment_id` to each saved connection. It retains that
reference across exports and credential renewal. Two attachments of the same
remote catalog with different options, tenants or data versions get different
references, even when they have the same location and catalog name. Changing
that connection's tenant, options or data version creates a new reference;
rotating credentials for the same intended source may retain it. References
are not hashes of secrets, aliases, grants or randomized ticket bytes.

The author chooses which sources the report needs. No grants or tickets ever
go into a report. A reference identifies a source slot, not a credential or an
authorization: each execution principal explicitly provisions their own attachment for that
slot. When scheduling someone else's report, the client asks the execution principal to map
each unprovisioned reference to their own attached source; it never guesses
from an alias or `(location, catalog_name)` match. The selected source must
match the declared location and catalog. Multiple aliases may deliberately
refer to the same attachment reference.

## Delegations

The fields, defaults and Arrow types are defined by the
[Python dataclasses](reference/delegations.md).

**Normalized location**: lowercase scheme and DNS host, explicit port (443
for HTTPS, 80 for HTTP), and an absolute path with dot segments removed and
trailing slashes removed (root becomes empty). Reject userinfo, query and
fragment. Preserve path case, percent escapes and internal slashes; do not
decode escaped separators. Different paths remain different services. IPv6
hosts retain brackets. HTTPS is required except loopback HTTP.

For catalog delegations, resolve an omitted/default catalog name to the
explicit name returned by the attachment before saving the source and export.
`attachment_id` and `ticket` must be nonempty. Service delegations require
empty `catalog_name`, `attachment_id` and `ticket`; other combinations are
`invalid_request`. Both kinds require a nonempty grant.

### `vgi.delegations.v1`

Hosted by any service that runs work unattended (in practice beside
`vgi.schedules.v1` and `vgi.alerts.v1`). The authenticated caller owns that delegation namespace. Management owners
cannot put or read another execution principal's secrets by naming an ID.

| Method | Kind | Purpose |
| --- | --- | --- |
| `put_delegations(…)` | U | Store or replace the caller's delegations, keyed by `(kind, location, catalog_name, attachment_id)`; duplicate keys in one request are `invalid_request` |
| `list_delegations(…)` | S | The caller's delegations with `expires_at` and `updated_at`. Never grants or tickets |
| `revoke_delegation(…)` | U | Destroy exactly the caller's matching delegation |

To run a report, query or rule, the runner looks up the execution principal's `catalog`
delegation for each source's exact `(location, catalog_name, attachment_id)`
and attaches it under the source's `alias`. A missing delegation is
`grant_required`, naming the location and attachment reference. There is no
fallback to a different reference at the same location. Replacing one
reference during renewal must leave all other references unchanged.
Schedules and rules carry no credentials of their own. Their execution
identity chooses the delegation namespace; their management owner does not.
Expiry is nullable UTC microseconds in this reporting protocol; null means
unlimited. Renewal/revoke have the exact request ID and version preconditions
in the wire contract, so a stale operation cannot overwrite newer credentials.

### Standalone service grants

Before saving a `render_report` schedule, the client confirms the report
service location through the user's connection flow, discovers `Identity.v1`,
and calls `issue_grant` there as the execution principal using the issuer's accepted authentication flow. It deposits a
`kind = service` delegation with that exact normalized location. A report
store may host only `vgi.reports.v1` and `Identity.v1`; neither `vgi.v2` nor
attach tickets are prerequisites for this flow. A service that cannot issue
usable grants cannot be read unattended and produces `grant_required`.

The scheduler looks up `(service, ReportRef.service_url, "", "")` before
reading the revision, then resolves that revision's catalog delegations. A
catalog grant at the same URL is not an implicit substitute. The report-store
grant is sent only to that service and never forwarded to the renderer.
Renewal repeats `issue_grant` on login and replaces that service row;
revocation deletes it independently of catalog rows at the same URL.

### SQL binding

Schema `delegations`. Derived by the [shared read-binding rules](README.md#sql-binding).

| Table | List / get |
| --- | --- |
| `delegations` | `list_delegations` |

`list_delegations` is the only read table function. The table contains the
caller's delegation keys, expiry and update times; it has no grant or ticket
columns. Put and revoke use their authenticated RPC methods.

The client still runs the existing `vgi_export_session()` helper, which
returns credentials to that authenticated client. In memory it combines each
successful export with the explicitly saved attachment reference, creates a
`kind = catalog` delegation, and calls `put_delegations`. Service grants are
sent through the same RPC with `kind = service`. Neither path inserts
credentials into protocol tables or embeds them in SQL text.

```sql
SELECT kind, location, catalog_name, attachment_id, expires_at
FROM ops.delegations.delegations;
```

## Who does what

| Step | Who | Rule |
| --- | --- | --- |
| Issue | Each attached worker or explicitly connected service | A grant needs a login no older than 900 s. A catalog ticket needs only an authenticated caller. Either can be refused |
| Export | The client, as the user | `vgi_export_session()`, only for catalogs the user attached themselves. A report author can't make a reader export at a location the reader never attached |
| Store | Each host that runs unattended work | Protected at rest and never returned; each host chooses storage and key administration and keeps its own copy |
| Reattach | Whatever host runs the session | Presents a delegation only to its own location, only for that job |

Nothing assumes co-location: report store, scheduler, renderer, alert
evaluator and data workers can all be different hosts. When a job moves between
them (a scheduler asking a renderer), only the catalog delegations needed for
that job travel with it and are dropped when it ends.

**Scheduling someone else's report.** Use the schedule's selected execution
principal's delegations, never the report author's merely because they authored
it. That identity must provision the required attachment mappings and the
report-store service grant. Choosing a principal requires host authorization;
a management transfer neither copies delegations nor changes this choice.

**Renewal.** Whenever the execution principal authenticates, Cupola re-exports the sessions behind
their expiring catalog delegations, retaining the explicit reference mapping,
and calls `put_delegations`. Service grants renew directly through Identity.
Delegations from workers with no maximum lifetime never need it. A rotated
secret needs a fresh export from a
session attached with the new value.

**Revocation.** `revoke_delegation` removes the matching version from future lookups on that
host. It does not recall secrets already sent to a running job or renderer.
Rotating a worker's grant key invalidates its grants; rotating its
`VGI_SIGNING_KEY` invalidates its tickets.

## What still can't be done unattended

- **Identity-provider claims.** A grant's `AuthContext` has the execution principal's
  `principal`, not their groups or tenant. Any `AccessPolicy` that reads
  claims, on a data worker or the report store, must decide from the principal
  or fail closed.
- **Scopes are advisory.** By default a grant is as powerful as its owner on
  that worker.
- **Non-VGI attachments** (local files, `:memory:`, other database types) can't
  be exported. A report that depends on one can't run unattended.
