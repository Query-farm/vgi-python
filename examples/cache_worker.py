# /// script
# requires-python = ">=3.13"
# dependencies = ["vgi-python"]
# ///
"""A table function that advertises its result as cacheable, and revalidates it.

Caching is *advertised*, not requested: the worker attaches ``vgi.cache.*``
metadata to the **first** data batch it emits, and the client (the DuckDB
extension) decides what to do with it. Nothing is cached unless you say so.

This worker exposes ``rates()``, standing in for a slow upstream — a remote API,
a rate-limited service — whose answer is worth reusing. It shows the whole
vocabulary:

- a **freshness lifetime** (``ttl``), so repeat queries inside the window never
  reach the worker at all;
- a **validator** (``etag``) plus ``revalidatable=True``, which is what lets the
  client ask "is this still good?" instead of paying for a recompute;
- the **304-equivalent** reply — a 0-row batch carrying
  ``CacheControl(not_modified=True)`` — which tells the client its stored payload
  is still fresh.

``out`` is typed as vgi-rpc's ``OutputCollector``, which knows nothing about
caching; the object the framework actually passes is a VGI wrapper that accepts
``cache_control=``. Cast to :class:`vgi.protocol.VgiOutputCollector` to reach it
with types intact — every cache-aware function does this.

    ATTACH 'rates' (TYPE vgi, LOCATION 'uv run cache_worker.py');
    SELECT * FROM rates.rates();   -- second call inside the TTL never lands here
"""

from dataclasses import dataclass
from typing import cast

import pyarrow as pa

from vgi import Worker
from vgi.cache_control import CacheControl
from vgi.catalog import Catalog, Schema
from vgi.protocol import VgiOutputCollector
from vgi.table_function import (
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)

_SCHEMA = pa.schema([("currency", pa.string()), ("rate", pa.float64())])

# Stands in for whatever makes your upstream's answer change. A real worker
# would derive this from the source — a last-modified header, a version column,
# a content hash — and it is the ONLY thing revalidation compares.
_DATA_VERSION = '"rates-v3"'

_ROWS = {"currency": ["EUR", "GBP", "JPY"], "rate": [1.09, 1.27, 0.0067]}


@dataclass(slots=True, frozen=True, kw_only=True)
class RatesArgs:
    """``rates()`` takes no arguments — the whole table is the result."""


@bind_fixed_schema
@init_single_worker
class Rates(TableFunctionGenerator[RatesArgs, None]):
    """Emit the rate table once, advertising it as cacheable for 5 minutes."""

    FIXED_SCHEMA = _SCHEMA

    class Meta:
        """Function metadata."""

        name = "rates"
        description = "Exchange rates from a slow upstream, cacheable for 5 minutes"

    @classmethod
    def process(cls, params: ProcessParams[RatesArgs, None], state: None, out: OutputCollector) -> None:
        """Answer the scan — or, if the client's copy is still current, say so."""
        vgi_out = cast(VgiOutputCollector, out)

        # Conditional request: the client holds a stale-but-revalidatable copy
        # and is asking whether it may keep it. Both validators are None on a
        # normal call, so this branch is simply skipped.
        if params.if_none_match == _DATA_VERSION:
            vgi_out.emit(
                pa.RecordBatch.from_pylist([], schema=_SCHEMA),  # zero rows: "keep what you have"
                cache_control=CacheControl(
                    ttl=300,
                    etag=_DATA_VERSION,
                    revalidatable=True,
                    not_modified=True,
                ),
            )
            out.finish()
            return

        # Normal path: stream the result and advertise how it may be reused.
        # The metadata rides on the FIRST data batch; attaching it later has no
        # effect, because by then the client has decided how to treat the stream.
        vgi_out.emit(
            pa.RecordBatch.from_pydict(_ROWS, schema=_SCHEMA),
            cache_control=CacheControl(
                ttl=300,  # reusable for 5 minutes without asking
                etag=_DATA_VERSION,  # ...and after that, cheap to revalidate
                revalidatable=True,  # gates whether the client ever asks
                stale_while_revalidate=60,  # serve stale while refreshing behind it
                stale_if_error=600,  # serve stale rather than fail
            ),
        )
        out.finish()


class CacheWorker(Worker):
    """A worker exposing the ``rates`` catalog."""

    catalog = Catalog(
        name="rates",
        schemas=[Schema(name="main", functions=[Rates])],
    )


if __name__ == "__main__":
    CacheWorker().run()
