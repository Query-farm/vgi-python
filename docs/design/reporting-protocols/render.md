# vgi.report_render.v1

Rendering a report is a job: start it, poll it, collect artifacts. The first
implementation is headless Cupola, because Cupola reports render only in a
browser; other renderers can register for other body formats. The scheduler
uses it for `render_report` actions and Cupola for large exports. Shared
conventions are in [README.md](README.md#shared-conventions).

**Why jobs, not one call.** A render runs every query, waits for charts to
settle and builds a PDF. That takes longer than HTTP intermediaries tolerate,
and the caller needs to cancel and retry it.

```python
@dataclass
class RenderRequest:
    body_format: str
    body: bytes                           # the revision to render, passed inline
    data_sources: list[DataSource]        # the session to rebuild (credentials.md)
    delegations: list[Delegation] = []  # owner's grant + ticket per source, this job only
    parameter_values: list[ParamValue] = []  # literals; the caller resolves relative tokens
    outputs: list[str] = ["application/pdf"]  # also text/html, image/png (page-1 preview)
    locale: str = "en-US"
    time_zone: str = "UTC"
    timeout_seconds: int = 600
```

The caller passes the body inline. A scheduler has already read the revision
(it needs the envelope's `data_sources` to check grants), so the renderer never
needs credentials for the report store and is a function of body, sources,
parameters and grants.

| Method | Kind | Purpose |
| --- | --- | --- |
| `get_renderer_info()` | U | Body formats, output media types, timeout ceiling, allowed source locations |
| `start_render(request_id, request)` | U | Returns `job_id`, status `queued` |
| `get_render(job_id)` | U | Status (`queued`, `running`, `succeeded`, `failed`, `cancelled`), `poll_after_seconds`, artifacts, and on failure the cause's `code`, `kind` and data source |
| `cancel_render(job_id)` | U | Best effort |

**Artifacts** are `{name, media_type, size_bytes, sha256, url, expires_at}`,
with a short-lived download URL. Callers that keep artifacts copy them.

**Credentials.** The renderer follows the presenting rule in
[credentials.md](credentials.md#who-does-what): a grant goes only to the
location it was minted at, and is dropped when the job ends. It also enforces
an operator allowlist of source locations, published in `get_renderer_info`,
so a report can't point a session at an arbitrary host.

## SQL binding

Schema `report_render`. Derived by the rules in [README.md](README.md#sql-binding); this is the annotation.

No tables beyond `info`: render jobs can't be listed. `start_render`,
`get_render` and `cancel_render` are table functions, so a script can start a
render and poll it; Cupola and schedulers use RPC.

## Reference renderer (non-normative)

- Headless Cupola keeps today's isolation: sandboxed components run in an
  opaque-origin iframe that can't read tokens.
- Always produce tagged, accessible PDFs.
