# Unattended sessions

Reports, schedules and alerts read data from catalogs on other workers, and the
work may run on another host while nobody is logged in. This file defines how a
user's attached DuckDB session is **serialized** so that any runner, anywhere,
can **reattach** it later as that user, without ever seeing the user's secrets:

- **Attach tickets:** each attached worker seals the attachment's options,
  secret ones included, into a ticket only it can open, and agrees to honour
  it later.
- **Delegations:** one per owner per attached catalog, deposited with whichever
  host runs unattended work (`vgi.delegations.v1`).

Shared conventions are in [README.md](README.md#shared-conventions). The new
worker method and extension pieces are planned in
[attach-tickets-plan.md](attach-tickets-plan.md).

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

```python
@dataclass
class DataSource:
    alias: str               # catalog name in the session, e.g. "sales"
    location: str            # worker URL
    catalog_name: str = ""   # remote catalog, if not the worker's default
    label: str = ""          # "Sales warehouse (prod)", for people
    required: bool = True
```

Cupola fills it from the non-secret columns of `vgi_export_session()` when the
author saves, with a checklist so the author can untick catalogs the report
doesn't use. No grants or tickets ever go into a report: they belong to the
person whose credentials they carry.

## Delegations

```python
@dataclass
class Delegation:
    location: str            # normalized worker URL
    catalog_name: str
    grant: str               # write-only
    ticket: str              # write-only
    expires_at: float        # the earlier of the two; +inf when neither expires
```

**Normalized location**: scheme, lowercased host, explicit port, and path
without a trailing slash, so two workers behind one host on different paths
stay distinct.

### `vgi.delegations.v1`

Hosted by any service that runs work unattended (in practice beside
`vgi.schedules.v1` and `vgi.alerts.v1`). The caller is always the owner.

| Method | Kind | Purpose |
| --- | --- | --- |
| `put_delegations(delegations)` | U | Store or replace the caller's delegations, keyed by `(location, catalog_name)` |
| `list_delegations()` | S | The caller's delegations with `expires_at` and `updated_at`. Never grants or tickets |
| `revoke_delegation(location, catalog_name)` | U | Destroy one delegation |

To run a report, query or rule, the runner takes each `DataSource`, looks up the
owner's delegation for its `(location, catalog_name)`, and attaches it under
the source's `alias`. A missing delegation is `grant_required`, naming the
source.
Schedules and rules carry no credentials of their own.

### SQL binding

Schema `delegations`. Derived by the rules in [README.md](README.md#sql-binding); this is the annotation.

| Table | List / get | `INSERT` | `UPDATE` | `DELETE` |
| --- | --- | --- | --- | --- |
| `delegations` | `list_delegations` | `put_delegations`, one call per statement (replaces by key) | | `revoke_delegation` |

`grant` and `ticket` are write-only. The columns match `vgi_export_session()`,
so delegating a whole session is one statement and no credential appears in
the SQL text:

```sql
INSERT INTO ops.delegations.delegations BY NAME
SELECT * EXCLUDE (alias, status, message) FROM vgi_export_session()
WHERE status = 'ok';
```

## Who does what

| Step | Who | Rule |
| --- | --- | --- |
| Issue | Each attached worker | A grant needs a login no older than 900 s. A ticket needs only an authenticated caller. Either can be refused |
| Export | The client, as the user | `vgi_export_session()`, only for catalogs the user attached themselves. A report author can't make a reader export at a location the reader never attached |
| Store | Each host that runs unattended work | Encrypted under a key outside its database; never returned; each host keeps its own copy |
| Reattach | Whatever host runs the session | Presents a delegation only to its own location, only for that job |

Nothing assumes co-location: report store, scheduler, renderer, alert
evaluator and data workers can all be different hosts. When a job moves between
them (a scheduler asking a renderer), the delegations for that job travel with it
and are dropped when it ends.

**Scheduling someone else's report.** The schedule's owner reattaches with
their own delegations, exported from their own session, never the author's. If the
owner hasn't attached a source the report needs, they're asked to before
saving the schedule.

**Renewal.** Whenever the owner logs in, Cupola re-exports the sessions behind
their expiring delegations and calls `put_delegations`. Delegations from workers with no
maximum lifetime never need it. A rotated secret needs a fresh export from a
session attached with the new value.

**Revocation.** `revoke_delegation` destroys the only copy on that host.
Rotating a worker's grant key invalidates its grants; rotating its
`VGI_SIGNING_KEY` invalidates its tickets.

## What still can't be done unattended

- **Identity-provider claims.** A grant's `AuthContext` has the owner's
  `principal`, not their groups or tenant. Any `AccessPolicy` that reads
  claims, on a data worker or the report store, must decide from the principal
  or fail closed.
- **Scopes are advisory.** By default a grant is as powerful as its owner on
  that worker.
- **Non-VGI attachments** (local files, `:memory:`, other database types) can't
  be exported. A report that depends on one can't run unattended.
