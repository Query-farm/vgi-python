# vgi.reports.v1

A report is a standard envelope plus an opaque body, with a linear chain of
immutable numbered revisions, one published pointer, and tombstone redaction.
Every method is required; a host that can't accept writes says
`writable = false` and refuses them. Shared conventions are in
[README.md](README.md#shared-conventions).

## Model

- **Revision.** An immutable snapshot of envelope and body with
  `revision_number` (1, 2, 3 …, shown as "v7"), author, time, `kind` (`edit`,
  `agent`, `restore`, `import`) and message. The newest is the head.
- **Publication.** `published_revision_id` points at the revision readers see.
  `publish` moves it; publishing an empty revision id unpublishes. Revision rows
  record `published_at` and `published_by` when they were published.
- **Redaction.** `redact_revision` replaces a revision's envelope and body with
  a tombstone that keeps its number, original author and time, and records the
  redacting actor, time and reason. Numbering never gaps. The head and the
  published revision can't be redacted; commit or publish a replacement first.
- **Deletion.** `delete_report`. Whether that is soft, and for how long, is
  implementation policy.
- **Drafts and restore are client-side.** Clients keep their working copy
  locally (Cupola already does) and commit with `expected_revision_id`; restore
  is reading an old revision and committing it with `kind = "restore"`.

## Envelope

```python
@dataclass
class ReportEnvelope:
    title: str
    description: str = ""
    path: str = ""                       # library folder, "/"-separated
    tags: list[str] = []
    body_format: str                     # "cupola.evidence/1"
    data_sources: list[DataSource] = []  # credentials.md
    parameters: list[ParameterSpec] = []

@dataclass
class ParameterSpec:
    key: str                             # $key in SQL
    label: str
    type: str                            # text | number | date | boolean | select | multi_select | date_range
    required: bool = False
    default_json: str = "null"
```

`data_sources` says which catalogs the report needs, so a renderer, a scheduler
or another browser can rebuild its session. `parameters` is the report's public
interface, so a schedule editor can show inputs without parsing the body.

Records add `report_id`, `head_revision_id`, `published_revision_id`,
`created_at/by`, `updated_at/by`, `body_sha256` and `allowed_actions`.
Revision rows add `published_at/by` and, once redacted, `redacted_at/by` and
`redaction_reason`.

## Methods

U = unary, S = producer stream. All are required.

| Method | Kind | Purpose |
| --- | --- | --- |
| `get_report_service_info()` | U | `display_name`, `writable`, body formats, size limits |
| `list_reports(query, path_prefix, tags, owned_by_me, published_only)` | S | Library rows without bodies, filtered by read permission |
| `get_report(report_id, revision_id="")` | U | Envelope and body. With no revision id, editors get the head and everyone else the published revision; `revision_served` says which |
| `list_revisions(report_id)` | S | History, newest first, tombstones included |
| `create_report(request_id, envelope, body, message)` | U | New report with revision 1 |
| `commit_revision(report_id, expected_revision_id, envelope, body, kind, message)` | U | New head; `conflict` if the head moved |
| `publish(report_id, revision_id, expected_published_revision_id)` | U | Move the published pointer; empty `revision_id` unpublishes |
| `delete_report(report_id, expected_revision_id)` | U | |
| `redact_revision(report_id, revision_id, reason)` | U | Requires `redact` |

**Errors:** `not_found` (with the optional access hint), `action_denied`,
`read_only_service`, `conflict`, `invalid_request`, `quota_exceeded`,
`service_unavailable` ([README.md](README.md#errors)).

**Unattended reads.** A scheduler or renderer reads reports with the owner's
grant on the report service. Grants carry no identity-provider claims, so an
`AccessPolicy` that authorizes by claims refuses them; it must decide from the
principal ([credentials.md](credentials.md#what-still-cant-be-done-unattended)).

## SQL binding

Schema `reports`. Derived by the rules in [README.md](README.md#sql-binding); this is the annotation.

| Table | List / get | `INSERT` | `UPDATE` | `DELETE` |
| --- | --- | --- | --- | --- |
| `reports` | `list_reports` / `get_report` (fetches `body`) | `create_report` | `commit_revision`, expected = the row's `head_revision_id` | `delete_report`, same |

`list_revisions` has a required `report_id`, so it stays a table function.
`publish` and `redact_revision` are procedures. Extra columns: `request_id` and
`message` on `INSERT`; `message` and `kind` on `UPDATE`.

```sql
SELECT title, path, published_revision_id FROM ops.reports.reports
WHERE path LIKE '/finance/%';

SELECT revision_number, author, kind, message
FROM ops.reports.list_revisions(report_id := 'r_1');

CALL ops.reports.publish(report_id := 'r_1', revision_id := 'rev_7',
                         expected_published_revision_id := 'rev_5');
```

## Read-only publisher

`ReadOnlyReportStore.from_files(path)` serves `.cupola-reports.json` exports
from a package directory and implements the whole protocol: reads serve the
files, writes are refused with `read_only_service`, `writable = false`. The
revision id is the content hash and the published revision is the head.

## Import

An import of a `.cupola-reports.json` file is `create_report` followed by
`commit_revision` for each historical revision, kind `import`, with the original
kinds kept in the message. Cupola's "remove from history" maps to
`redact_revision`.
