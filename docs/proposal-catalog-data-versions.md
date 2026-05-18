# Proposal: Discoverable data versions

**Status:** draft, for review.
**Scope:** `vgi-python`. C++ extension untouched.

## Why

`CatalogInfo.data_version_spec` is the compatibility contract — a semver
range like `>=1.0.0,<4.0.0`. It answers *"will my pin work?"*

It doesn't answer *"which versions actually exist, and what changed?"* —
the questions a human needs answered to pick a version. The describe
page today shows the range and lets you guess.

This proposal closes that gap for data versions. Implementation versions
stay outside the protocol; they're git history.

## The change

One new record, two new fields on an existing record. Nothing else.

```python
# vgi/catalog/catalog_interface.py

@dataclass(frozen=True)
class CatalogDataVersionRelease(ArrowSerializableDataclass):
    """One published data version of a catalog."""

    version: str
    released_at: Annotated[
        datetime | None, ArrowType(pa.timestamp("us", tz="UTC"))
    ] = None
    summary: str = ""
    notes_url: str | None = None


@dataclass(frozen=True)
class CatalogInfo(ArrowSerializableDataclass):
    ...
    # Concrete published data versions, newest-first. Empty when the
    # worker doesn't track release history.
    releases: list[bytes] = field(default_factory=list)

    # Where this worker's code lives — repo, build, docs. None when the
    # worker doesn't advertise a source location.
    source_url: str | None = None
```

`releases` follows the existing `attach_option_specs` pattern: each
entry is a serialized `CatalogDataVersionRelease`.

That's the entire protocol change.

## Why inlined, no new method

`catalogs()` is already the discovery RPC, and releases *are* discovery
data. Splitting them into `catalog_data_versions(name)` would mean two
round-trips for what one call should answer, plus dispatch plumbing in
`protocol.py` / `worker.py` / `meta_worker.py` — pure churn for no
new capability. A typical worker has fewer than twenty releases at a
few hundred bytes each; the bandwidth cost is rounding error.

If some worker ever has thousands of releases, it can publish a
truncated list and link to the full history via `source_url`. That's
an implementer choice, not a protocol-design constraint.

## Worker adoption

`vgi/_test_fixtures/versioned_tables.py` is the reference. Today it has
a private `SUPPORTED_VERSIONS` tuple; surface it through
`CatalogInfo.releases`:

```python
_RELEASES = (
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
                releases=[r.serialize() for r in _RELEASES],
            ),
        ]
```

Every other worker — `versioned.py`, `attach_options.py`, the example
worker — inherits the empty defaults and gets exactly the page they
have today.

## Describe page

`vgi/http/worker_page.py:_collect_catalog_panels` already deserializes
`attach_option_specs`; releases follow the same path. Render:

- A release-history panel under the **Data version** input. Each row:
  version (clickable, fills the input), released_at, summary, optional
  "details →" link when `notes_url` is set.
- "View source →" inline next to the **Implementation** chip when
  `source_url` is set.

Both are conditional on the worker populating the fields. Workers with
no releases see no panel — zero visual change from today.

## What this isn't

- No diff-between-versions API. The page can attach two versions and
  compute the delta from existing protocol if anyone ever asks for it.
- No DuckDB SQL exposure (`vgi_catalog_releases(...)`, etc.). The
  describe page consumes the field directly; Cupola will too. SQL is a
  separate RFC if and when needed.
- No release-history field for implementation versions. Workers point
  at git via `source_url`; the protocol stops there.

## Rollout

Three commits, each shippable on its own:

1. **Protocol.** Add `CatalogDataVersionRelease` + the two `CatalogInfo`
   fields. Tests cover the empty defaults and a populated Arrow round-trip.
2. **Adopt.** Populate `releases` and `source_url` in
   `versioned_tables.py`. Conformance + integration confirm the
   non-empty path round-trips end-to-end.
3. **Render.** Timeline + source link in `worker_page.py`. Visual
   check against `VersionedTablesWorker`.

No RPC method dispatch, no new HTTP routes, no Cupola coordination
required to ship.

## Open question

`releases` ordering — locking newest-first in the contract so every
consumer treats the list as a feed without re-sorting. Open to
leaving unspecified if you'd rather not commit to that.
