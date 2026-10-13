# View security in the coordinated successor protocol

Status: implemented preparation work on the DuckDB 1.5 branch; proposed for the
combined successor release. This document does not qualify DuckDB 2 execution
or close the [release gates](duckdb-2.0-manifest.json).

## Representation

`ViewInfo.security` is a required, non-null Arrow `utf8` field after `aliases`.
It has two case-sensitive values:

| Value | Meaning |
| --- | --- |
| `regular` | An ordinary view. |
| `barrier` | A view whose engine must enforce an optimization barrier. This maps to DuckDB 2 `ViewSecurityType::SECURE_VIEW`. |

This is not a SQL SECURITY DEFINER/INVOKER or privilege-granting mode. The query
remains a SELECT-only `definition`, accompanied by ordered raw `aliases` and
the existing comment/tag/column-comment metadata. Sending a complete CREATE
VIEW statement in `definition` is not an alternative encoding.

Missing, NULL, unknown, or wrongly typed read metadata must not be interpreted
as `regular`. Public constructors may default new ordinary views to `regular`,
but serialized metadata must carry the value explicitly. Readers that cannot
enforce a barrier when binding/materializing the query must reject that view
before executing it. This applies regardless of a peer's claimed capabilities.
Metadata-only consumers may retain a barrier record without executing it.

`catalog_view_create` carries a `security` string after `aliases`. Its ordinary
creation default is `regular`. A barrier request is explicit. Unknown/NULL modes
are rejected; neither IGNORE nor REPLACE allows an unsupported request to lose
the flag silently. A successful provider stores the mode together with the
definition and aliases, and must retain it through metadata changes, discovery,
revalidation, persistence, and transaction completion.

## Capability agreements

| Profile | Category | Promise |
| --- | --- | --- |
| `vgi.view.barrier.read.v1` | `read` | Preserve barrier metadata; enforce the barrier if materializing/executing, otherwise reject execution. A provider can supply the explicit mode. |
| `vgi.view.barrier.create.v1` | `ddl` | Accept and persist barrier creation/replacement without semantic loss. Requires the read profile in the same agreement. |

A client offer is not authorization. Providers still apply their access and
mutation rules. Selection must intersect the offer with current provider and
worker support. A transaction involving barrier DDL additionally requires
`vgi.transaction.ddl.v1`.

The Python worker checks selected/current profiles on get, schema listing and
full snapshots. It rejects unsupported barrier reads rather than projecting
away the flag. Its capability-aware snapshot hash includes security metadata;
a stale ETag or a token from an incapable attachment cannot bypass the check.
Withdrawing provider support also invalidates permission to use an old sealed
agreement. Checks precede provider CREATE effects, including IGNORE/REPLACE.

Existing barrier views are checked before replacement, rename, comment changes
and drop. Replacing a barrier with `regular` requires the barrier creation
agreement as well; this prevents an unaware caller's default mode from erasing
the flag. Transactional metadata mutations require the DDL transaction profile.
These worker preflights are not a concurrency primitive. Atomic replacement,
authorization, dependency handling and concurrent mutation/commit semantics
remain provider obligations and broader qualification work.

## Implemented coverage and limits

| Layer | Implemented | Still required |
| --- | --- | --- |
| All seven SDKs | Explicit mode in metadata records, descriptors and codecs; CREATE wire parameter; shared regular/barrier IPC vectors. | Full negotiated barrier operation support in every SDK. |
| Python worker | Attachment-scoped read/create profiles, stale-agreement rejection, snapshot/revalidation checks and mutation preflights. | Production provider and concurrent lifecycle qualification. |
| SQLite reference | Public RPC persistence, reconnect, rollback/commit, replacement, rename and comment preservation. | Query binding/execution; this reference only stores metadata. |
| Bundled ordinary mutable providers | Reject barrier/unknown CREATE modes before effects, including IGNORE/REPLACE. | A provider implementation that advertises and fulfills barrier creation. |
| Native DuckDB 1.5 | Always emits ordinary CREATE mode; rejects barrier/missing/NULL/unknown/wrong-type metadata before binding through get, listing and bulk-snapshot paths. | It cannot execute barrier views and does not offer the barrier read profile. |
| Native DuckDB 2 | Source mapping identified. | Set `CreateViewInfo.security_type`, prove optimizer barrier behavior, rebind/cache refresh and dependent-query behavior before offering support. |

The Python protocol tests exercise the real public HTTP client and durable
reference. SDK tests exercise their actual codecs and unsupported mutation
paths. Native malformed-wire fixtures bypass Python's own checks and assert
which discovery route was used. Passing these tests does not establish native
DuckDB 2 barrier enforcement.

Creation comments/tags/column comments, view-column comment mutation,
owner-relative query resolution, dependencies, fixed output contracts and the
remaining catalog operations stay in the same coordinated release plan. No
intermediate public protocol version is being published.
