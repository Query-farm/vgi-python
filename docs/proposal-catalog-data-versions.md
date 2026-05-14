# Proposal: Catalog data-version release manifest

**Status:** draft, for review.
**Scope:** `vgi-python`. C++ extension is unaffected unless we later expose
this via DuckDB SQL (out of scope here).

## Why

Today the catalog protocol carries a *compatibility contract* for data
versions — `CatalogInfo.data_version_spec` advertises a semver range like
`>=1.0.0,<4.0.0`. That's the right shape for "will my pin work?"

But it leaves two real questions unanswered:

1. **What versions have actually been released?** A user staring at the
   describe page sees the range and has to guess `1.5.0` vs `1.0.0` —
   nothing on the wire tells them what's been published. The internal
   `SUPPORTED_VERSIONS` tuple in `vgi/_test_fixtures/versioned_tables.py`
   exists but is private to the worker.
2. **What changed between two versions?** Today there is no way to find
   out without reading the worker's source.

Conflating these with the spec would be a mistake — the spec is a stable
contract, the release history grows every release. They answer different
questions and belong in different places. This proposal adds the missing
piece for **data versions only**.

### Why only data versions

Data versions and implementation versions are independent concerns and
data engineering has been hurt by tying them together. A worker's code
(implementation version) can release on its own cadence — that's just git
history and lives on GitHub. A catalog's *data* (schemas, tables, columns,
content semantics) can release independently and is what users actually
need to pin against. The protocol should only carry what's specific to
the catalog; everything implementation-level can be a URL pointer.

### Why not pre-bake release notes into the protocol

Long-form release notes belong in CHANGELOG.md / GitHub releases — a
single source of truth that already exists. The protocol carries enough
to render a "release timeline" UI: version, date, one-line summary,
breaking flag. Anything richer is a link out.

## Non-goals

- No `implementation_version_releases` field. Implementations link to
  source/build via a URL; their changelog lives in git.
- No diff API between two versions. The page can attach as v1 then v2
  and diff client-side using existing protocol — no new method needed
  for that, and it's a follow-up UI feature.
- No exposure through DuckDB SQL (e.g. `vgi_catalog_data_versions(...)`).
  Today the describe page calls the new method directly in-process. If
  Cupola or the C++ extension want SQL-level access, that's a separate
  RFC.
- No multi-version Apply preset on the page. The data-version input
  stays free-text; the release timeline is informational, with each row
  optionally clickable to fill the input.

## Protocol changes

### 1. New record: `CatalogDataVersionRelease`

In `vgi/catalog/catalog_interface.py`, alongside `CatalogInfo`:

```python
@dataclass(frozen=True)
class CatalogDataVersionRelease(ArrowSerializableDataclass):
    """One published data version of a catalog.

    Workers populate this via :meth:`CatalogInterface.catalog_data_versions`
    so clients (describe page, Cupola, programmatic readers) can render a
    discoverable release timeline. Long-form notes are not carried — link
    to a CHANGELOG/GitHub release via the catalog's ``source_url``.
    """

    # Concrete version, not a spec. e.g. "1.0.0", "2.4.1". Semver carries
    # the breaking-change signal directly — major bumps are breaking,
    # minor/patch are not. Don't denormalize that into a separate field.
    version: str

    # Release date (UTC). None means the worker doesn't track dates.
    released_at: Annotated[datetime | None, ArrowType(pa.timestamp("us", tz="UTC"))] = None

    # One-line human summary. Empty string when unknown.
    summary: str = ""

    # Optional per-release link to detailed notes — a CHANGELOG anchor,
    # a GitHub release page, a PR, a migration guide. Distinct from the
    # catalog-level ``source_url``: that points at the repo as a whole,
    # this points at what changed in *this* release. ``None`` when the
    # worker doesn't track per-release URLs.
    notes_url: str | None = None
```

### 2. New method on `CatalogInterface`

```python
def catalog_data_versions(self, *, name: str) -> list[CatalogDataVersionRelease]:
    """Return the published data-version history for one catalog.

    Default implementation returns ``[]`` — workers that don't track
    release history don't need to implement this. Order is
    newest-first; the page renders in that order without re-sorting.

    Calling this on a catalog name the worker doesn't expose raises
    ``ValueError`` (mirrors :meth:`catalog_attach`).
    """
    del name
    return []
```

This lives on the abstract `CatalogInterface`, with the default empty
list inherited by `ReadOnlyCatalogInterface`. No abstract `@abstractmethod`
decorator — callers tolerate empty lists.

### 3. New optional field on `CatalogInfo`: `source_url`

```python
@dataclass(frozen=True)
class CatalogInfo(ArrowSerializableDataclass):
    ...
    # Optional URL pointing at where the catalog's source / build /
    # release history lives (e.g. GitHub repo, internal build system).
    # ``None`` when the worker doesn't advertise a source location.
    source_url: str | None = None
```

This is the implementation-version-related piece. We do *not* enumerate
implementation releases — they're git history. A single URL is enough
for the describe page to render "View source / docs →".

### 4. RPC plumbing

`catalog_data_versions` joins the existing `_ATTACH_ID_METHODS`-style
tables in `vgi/protocol.py` and `vgi/worker.py` so it dispatches over
both subprocess and HTTP transports. The shape mirrors `catalog_catalogs`
(no attach_opaque_data; just a name). Subclassing `MetaWorker` correctly forwards
the call to the right child worker by catalog name.

## Worker-side adoption

`vgi/_test_fixtures/versioned_tables.py` becomes the reference. It
already has a `VERSION_TABLES` dict; we add a release manifest constant
and the override:

```python
DATA_VERSION_RELEASES = (
    CatalogDataVersionRelease(
        version="3.0.0",
        released_at=datetime(2026, 4, 15, tzinfo=UTC),
        summary="Removed deprecated 'animals' table.",
        notes_url="https://github.com/Query-farm/vgi-python/releases/tag/data-v3.0.0",
    ),
    CatalogDataVersionRelease(
        version="2.0.0",
        released_at=datetime(2026, 2, 1, tzinfo=UTC),
        summary="Added 'plants' table.",
        notes_url="https://github.com/Query-farm/vgi-python/releases/tag/data-v2.0.0",
    ),
    CatalogDataVersionRelease(
        version="1.1.0",
        released_at=datetime(2026, 1, 10, tzinfo=UTC),
        summary="Added 'sound' column to animals.",
    ),
    CatalogDataVersionRelease(
        version="1.0.0",
        released_at=datetime(2026, 1, 1, tzinfo=UTC),
        summary="Initial release.",
    ),
)


class VersionedTablesCatalog(ReadOnlyCatalogInterface):
    def catalogs(self) -> list[CatalogInfo]:
        return [
            CatalogInfo(
                name=CATALOG_NAME,
                implementation_version=DEFAULT_IMPLEMENTATION_VERSION,
                data_version_spec=DATA_VERSION_SPEC,
                source_url="https://github.com/Query-farm/vgi-python",
            ),
        ]

    def catalog_data_versions(self, *, name: str) -> list[CatalogDataVersionRelease]:
        if name != CATALOG_NAME:
            raise ValueError(f"Unknown catalog: {name!r}")
        return list(DATA_VERSION_RELEASES)
```

Other fixtures (`versioned.py`, `attach_options.py`, `worker.py`) inherit
the empty default and need no change.

## Describe page UI

In `vgi/http/worker_page.py`:

1. Render the release timeline as a small expandable panel below the
   Data version input, keyed off the active catalog. Each row: version
   (clickable to fill the input), released_at, a one-line summary, and
   — when ``notes_url`` is set — a "details →" link out. Major-version
   transitions can be visually highlighted client-side from the version
   string alone.
2. When `CatalogInfo.source_url` is non-None, render a small "View
   source →" link inline next to the implementation chip.

Both elements are conditional — workers that don't fill them in get
exactly the page they have today. No backwards-compat fudges; the
defaults handle it.

## Cupola

Once the protocol changes land, Cupola can call
`catalog_data_versions(name)` over its existing RPC client and render
the same timeline (and the `source_url`) without further VGI-side work.
File a follow-up issue on `Query-farm/vgi-web-frontend` asking it to
consume both surfaces.

## Tests

- `tests/test_catalog_interface.py` — add a unit test for the default
  empty list and for round-tripping a populated list through Arrow
  serialization.
- `tests/test_worker_page.py` — three new cases:
  - When the worker exposes releases, the timeline renders with each
    version, summary, and breaking chip.
  - When the worker exposes `source_url`, the link renders.
  - When neither is set, the page matches today's output (regression).
- `tests/conformance/` — once the protocol surface is plumbed, add a
  parity test that round-trips `catalog_data_versions` end-to-end via
  the example worker (a future fixture grows release info).

## Step-by-step rollout

1. Land `CatalogDataVersionRelease` + the abstract method default + the
   `source_url` field. No worker behavior change yet. Tests cover the
   empty-default contract.
2. Plumb the RPC dispatch in `protocol.py` / `worker.py` /
   `meta_worker.py`. End-to-end test against the existing example
   worker still returning empty.
3. Adopt in `versioned_tables.py`. Conformance + integration tests
   exercise the populated path.
4. Render in `worker_page.py`. Visual regression via the existing
   tests + manual preview against `VersionedTablesWorker`.
5. File the Cupola follow-up issue on `Query-farm/vgi-web-frontend`.

Each step is independently shippable; the protocol additions are
forward-compatible (workers that don't override see no change).

## Open questions

- **`released_at` timezone semantics** — proposing UTC. Alternatives
  are wall-clock with a tz field (overkill) or epoch seconds (loses
  human readability). UTC is consistent with how the worker page
  formats other timestamps.
- **Order contract** — the doc says newest-first. Could leave it
  unspecified and let the page sort. Unspecified is brittle; locking
  newest-first lets every consumer treat the list as a feed.
- **Per-release URL naming.** Going with `notes_url` (what changed in
  this release) to keep it visually distinct from the catalog-level
  `source_url` (where the code lives). Open to `release_notes_url` if
  the shorter name reads ambiguously.
