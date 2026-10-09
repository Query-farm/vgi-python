# vgi.reports.v1

Python dataclasses and RPC interface: [generated reference](reference/reports.md).
Lifecycle and encoding rules: [wire behavior](wire-contracts.md).

A report is a standard envelope plus an opaque body, with a linear chain of
immutable numbered revisions, one published pointer, and tombstone redaction.
Reports live at the library root or in persistent folders nested to any depth
within the worker's advertised limit. Folders can exist without any reports.
Every method is required; a host that can't accept writes says
`writable = false` and refuses them. Shared conventions are in
[README.md](README.md#shared-conventions).

## Model

- **Folder.** `FolderRecord` has a stable `folder_id`, `parent_folder_id`, name,
  ownership and version. A null parent is the service's virtual root. Sibling
  folder names are unique; report titles can repeat. Folder IDs remain stable
  when renamed or moved.
- **Placement.** `ReportRow.folder_id` identifies the report's current folder;
  null means root. Placement is resource metadata, separate from the revision
  envelope. Each report has one location. Moving a report preserves its ID,
  publication, revision history and references from schedules.
- **Revision.** An immutable snapshot of envelope and body with
  `revision_number` (1, 2, 3 …, shown as "v7"), author, time, `kind` (`edit`,
  `agent`, `restore`, `import`) and message. The newest is the head.
- **Publication.** `published_revision_id` points at the revision readers see.
  `publish` moves it; publishing a null revision id unpublishes. Revision rows
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

The fields, defaults and Arrow types are defined by the
[Python dataclasses](reference/reports.md).

`data_sources` says which catalogs the report needs, so a renderer, a scheduler
or another browser can rebuild its session. `parameters` is the report's public
interface, so a schedule editor can show inputs without parsing the body.

The exact `ReportRow`, `ReportResult` and `RevisionRow` schemas separate
resource metadata from the served revision. Ownership, parent ownership and
resource version, including the report's current folder, live outside the
immutable envelope. Attribution survives
author removal; `set_ownership` never changes report IDs, history or data.

## Folders and browsing

Create folders with `create_folder`, then pass `folder_id` when creating a
report. `move_report` relocates an existing report. `update_folder` supplies the
complete new name and parent to rename, move, or do both atomically. Moving a
folder moves its subtree without rewriting its descendants or report revisions.
`delete_folder` accepts only empty folders; users must move or delete the
contents explicitly first. There is no recursive delete.

To browse root, call `list_folders(recursive=False)` and
`list_reports(recursive=False)`. Inside a folder, use
`list_folders(parent_folder_id=folder_id, recursive=False)` and
`list_reports(folder_id=folder_id, recursive=False)`. With `recursive=True`,
each call includes descendants. Omitting all filters preserves the whole-library
views used by the SQL tables. The two streams are independent snapshots; a client
refreshes after edits rather than assuming an atomic combined directory listing.

For example, an RPC client can create and use a hierarchy with direct arguments:

```python
finance = client.create_folder(request_id="folder-finance", name="Finance")
quarterly = client.create_folder(
    request_id="folder-quarterly", name="Quarterly", parent_folder_id=finance.folder_id
)
report = client.create_report(
    request_id="report-sales", envelope=envelope, body=body, folder_id=quarterly.folder_id
)
client.move_report(
    report_id=report.report_id, expected_version=report.version,
    request_id="move-sales", folder_id=finance.folder_id,
)
```

Folder containment and `ownership.parent_owner_ref` represent different things;
a folder identifies placement and a scope for worker permission rules, while
the latter identifies a management authority. Folder ownership can be
transferred through `set_folder_ownership` if its author leaves.
Exact naming, concurrency and authorization rules are in the
[folder wire contract](wire-contracts.md#report-folders).

## Folder permissions

Workers may define and enforce policies on folders and reports. The worker
chooses how permissions are assigned, whether they inherit through the hierarchy,
and how moves or ownership changes affect access. The protocol does not prescribe
an authorization interface, policy model or evaluation algorithm.

`allowed_actions` on folders/reports and `root_allowed_actions` in service info
communicate caller-specific permission hints. Workers use the shared visibility
and refusal conventions to communicate their decisions; clients do not derive
permissions from folder placement or ownership.

## Methods

U = unary, S = producer stream. All are required.

| Method | Kind | Purpose |
| --- | --- | --- |
| `get_report_service_info(…)` | U | `display_name`, `writable`, body formats, size limits |
| `list_reports(…)` | S | Reports in a folder or subtree, filtered by read permission |
| `get_report(…)` | U | Envelope and body. With no revision id, editors get the head and everyone else the published revision; `revision_served` says which |
| `list_revisions(…)` | S | History, newest first, tombstones included |
| `create_report(…)` | U | New report with revision 1 in a folder or at root |
| `move_report(…)` | U | Change folder without creating a content revision |
| `commit_revision(…)` | U | New head; `conflict` if the head moved |
| `publish(…)` | U | Move the published pointer; null `revision_id` unpublishes |
| `delete_report(…)` | U | |
| `redact_revision(…)` | U | Requires `redact` |
| `set_ownership(…)` | U | Transfer management ownership; preserve author and execution credentials |
| `list_folders(…)` | S | Child folders or descendants, including empty folders |
| `get_folder(…)` | U | Folder metadata, ownership and permission hints |
| `create_folder(…)` | U | Create an empty folder at root or under another folder |
| `update_folder(…)` | U | Rename and/or reparent a folder, preserving its subtree |
| `delete_folder(…)` | U | Delete an empty folder; never delete descendants implicitly |
| `set_folder_ownership(…)` | U | Transfer folder management without cascading ownership changes |

**Errors:** `not_found` (with the optional access hint), `action_denied`,
`read_only_service`, `conflict`, `invalid_request`, `quota_exceeded`,
`service_unavailable` ([README.md](README.md#errors)).

**Unattended reads.** A scheduler or renderer reads reports with the execution principal's
grant on the report service. Grants carry no identity-provider claims, so an
access policy that authorizes by claims refuses them; it must decide from the
principal ([credentials.md](credentials.md#what-still-cant-be-done-unattended)).
Standalone stores need only reports and Identity for this flow: the caller
deposits a grant-only `service` delegation as specified in
[credentials.md](credentials.md#standalone-service-grants). No catalog or
attach ticket is required. For scheduled renders the scheduler reads the
revision and passes it inline; the renderer never receives that service grant.

## SQL binding

Schema `reports`. Derived by the [shared read-binding rules](README.md#sql-binding).

| Table | List / get |
| --- | --- |
| `reports` | `list_reports` / `get_report` (fetches `body` for the row's revision) |
| `folders` | `list_folders` / `get_folder` |

Read table functions: `get_report_service_info`, `list_reports`, `get_report`,
`list_revisions`, `list_folders` and `get_folder`. Revision enumeration requires
`report_id`, so it is not also a table. Folder creation and reorganization,
like all other mutations, remain RPC-only.

```sql
SELECT folder_id, name FROM ops.reports.folders WHERE parent_folder_id IS NULL;

SELECT envelope.title, folder_id, published_revision_id
FROM ops.reports.list_reports(folder_id := 'f_finance', recursive := true);

SELECT revision_number, author, kind, message
FROM ops.reports.list_revisions(report_id := 'r_1');
```

## Read-only publisher

`ReadOnlyReportStore.from_files(path)` serves `.cupola-reports.json` exports
from a package directory and implements the whole protocol: reads serve the
files, writes are refused with `read_only_service`, `writable = false`. The
revision id is the content hash and the published revision is the head.
The file loader exposes the configured folder hierarchy, including declared
empty folders, through the same reads. It assigns stable folder/report IDs
independent of display names; flat inputs default to root. All folder writes
are refused alongside report writes. The package manifest format is an
implementation detail, not a new reporting wire format.

## Import

An import of a `.cupola-reports.json` file is `create_report` followed by
`commit_revision` for each historical revision, kind `import`, with the original
kinds kept in the message. Cupola's "remove from history" maps to
`redact_revision`.
An importer creates the destination folders first and supplies the resulting
`folder_id` to `create_report`. A legacy path is an import hint, never the
identity or location of an immutable revision in the new protocol.
