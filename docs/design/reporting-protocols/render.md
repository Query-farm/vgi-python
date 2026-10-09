# vgi.report_render.v1

Python dataclasses and RPC interface: [generated reference](reference/render.md).
Lifecycle and encoding rules: [wire behavior](wire-contracts.md).

Rendering a report is a job: start it, poll it, collect artifacts. The first
implementation is headless Cupola, because Cupola reports render only in a
browser; other renderers can register for other body formats. The scheduler
uses it for `render_report` actions and Cupola for large exports. Shared
conventions are in [README.md](README.md#shared-conventions).

**Why jobs, not one call.** A render runs every query, waits for charts to
settle and builds a PDF. That takes longer than HTTP intermediaries tolerate,
and the caller needs to cancel and retry it.

The fields, defaults and Arrow types are defined by the
[Python dataclasses](reference/render.md).

The caller passes the body inline. A scheduler has already read the revision
(it needs the envelope's `data_sources` to check grants), so the renderer never
needs credentials for the report store and is a function of body, sources,
parameters and grants.

| Method | Kind | Purpose |
| --- | --- | --- |
| `get_renderer_info(…)` | U | Body formats, output media types, timeout ceiling, allowed source locations |
| `start_render(…)` | U | Returns `job_id`, status `queued` |
| `get_render(…)` | U | Status (`queued`, `running`, `succeeded`, `failed`, `cancelled`), `poll_after_seconds`, artifacts, and on failure the cause's `code`, `kind` and data source |
| `cancel_render(…)` | U | Best effort |

**Artifacts** are `{filename, media_type, size_bytes, sha256, url, expires_at}`,
with a short-lived download URL. Callers that keep artifacts copy them.

**Credentials.** The renderer follows the presenting rule in
[credentials.md](credentials.md#who-does-what): a grant goes only to the
location it was minted at, and is dropped when the job ends. It also enforces
an operator allowlist of source locations, published in `get_renderer_info`,
so a report can't point a session at an arbitrary host.

The request accepts only catalog delegations matching its `data_sources` by
location, catalog name and attachment reference. Reject service delegations
and unrelated catalog credentials with `invalid_request`; a missing required
reference is `grant_required`. Never substitute another attachment at the
same location. Start retries reuse the same request ID and are bounded by the
shared deduplication window, as in the
[execution contract](schedules.md#execution-and-retries).

Rendering and setup must be read-only apart from session-local temporary
objects. Reject persistent or external effects before dispatch. Exact job status,
nullable times, error fields and cancellation preconditions are in the wire
contract. Job retention, storage, key administration and deployment are worker
policy.

## SQL binding

Schema `report_render`. Derived by the [shared read-binding rules](README.md#sql-binding).

Read table functions: `get_renderer_info` and `get_render`. There are no tables
beyond `info`, because jobs cannot be listed. Starting and cancelling renders
are RPC-only; a SQL client can inspect a job whose ID it already has.

## Reference renderer (non-normative)

- Headless Cupola keeps today's isolation: sandboxed components run in an
  opaque-origin iframe that can't read tokens.
- Always produce tagged, accessible PDFs.
