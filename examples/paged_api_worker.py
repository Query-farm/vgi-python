# /// script
# requires-python = ">=3.13"
# dependencies = ["vgi-python"]
# ///
"""A lateral function backed by a paged API, returning one API page per reply.

``orders(customer_id)`` lists a customer's orders from an API that returns them a
page at a time behind a cursor. Rather than fetching every page before replying,
``process()`` fetches the first page and emits it with ``more=True``; the framework
then calls ``resume()`` for each next page. The state holds only where to resume
(which input row, which page cursor), never fetched rows, so it is small enough to
travel in an HTTP state token.

    ATTACH 'shop' (TYPE vgi, LOCATION 'uv run paged_api_worker.py');
    SELECT c.id, o.order_id, o.total
    FROM (VALUES (1), (2), (3)) c(id), shop.orders(c.id) o;
"""

from dataclasses import dataclass
from typing import Annotated, Any, cast

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass

from vgi import Arg, Worker
from vgi.catalog import Catalog, Schema
from vgi.invocation import BindResponse
from vgi.table_function import BindParams, ProcessParams
from vgi.table_in_out_function import OutputCollector, RowTransformFunction

PAGE_SIZE = 5


def fetch_orders_page(customer_id: int, cursor: str) -> tuple[list[dict[str, Any]], str]:
    """Stand-in for an HTTP API: one page of a customer's orders and the next cursor.

    Customer ``n`` has ``7 * n`` orders. An empty next cursor means the last page.
    """
    start = int(cursor or "0")
    total = 7 * customer_id
    orders = [
        {"order_id": customer_id * 1000 + k, "total": round(10.0 + k * 2.5, 2)}
        for k in range(start, min(start + PAGE_SIZE, total))
    ]
    end = start + len(orders)
    return orders, str(end) if end < total else ""


@dataclass(slots=True, frozen=True, kw_only=True)
class OrdersArgs:
    """One positional input column: the customer to list orders for."""

    customer_id: Annotated[int, Arg(0, doc="Customer whose orders to list")]


@dataclass(kw_only=True)
class OrdersCursor(ArrowSerializableDataclass):
    """Where the next reply resumes: the input's customer ids, a row, and that row's page cursor."""

    customer_ids: list[int] | None = None
    row: int = 0
    cursor: str = ""


class Orders(RowTransformFunction[OrdersArgs, OrdersCursor]):
    """List each input customer's orders, one API page per reply."""

    @classmethod
    def on_bind(cls, params: BindParams[OrdersArgs]) -> BindResponse:
        """A fixed output schema."""
        return BindResponse(output_schema=pa.schema([("order_id", pa.int64()), ("total", pa.float64())]))

    @classmethod
    def initial_state(cls, params: ProcessParams[OrdersArgs]) -> OrdersCursor:
        """An empty cursor; ``process()`` fills it for each input batch."""
        return OrdersCursor()

    @classmethod
    def process(
        cls, params: ProcessParams[OrdersArgs], state: OrdersCursor, batch: pa.RecordBatch, out: OutputCollector
    ) -> None:
        """Start a new input batch at its first row, then emit the first page."""
        state.customer_ids = [-1 if c is None else c for c in batch.column("customer_id").to_pylist()]
        state.row = 0
        state.cursor = ""
        cls.resume(params, state, out)

    @classmethod
    def resume(cls, params: ProcessParams[OrdersArgs], state: OrdersCursor, out: OutputCollector) -> None:
        """Emit the next non-empty page, with ``more=True`` unless it is the last."""
        ids = state.customer_ids or []
        orders: list[dict[str, Any]] = []
        row = state.row
        while state.row < len(ids) and not orders:
            row = state.row
            orders, state.cursor = fetch_orders_page(ids[row], state.cursor) if ids[row] > 0 else ([], "")
            if not state.cursor:  # that customer is done: the next page starts the next row
                state.row += 1
        # Every row of a page came from one input row; parent_rows says which.
        cast(Any, out).emit(
            pa.RecordBatch.from_pylist(orders, schema=params.output_schema),
            parent_rows=[row] * len(orders),
            more=state.row < len(ids),
        )


class ShopWorker(Worker):
    """A worker exposing the ``shop`` catalog with the paged ``orders`` function."""

    catalog = Catalog(
        name="shop",
        schemas=[Schema(path=["main"], functions=[Orders])],
    )


if __name__ == "__main__":
    ShopWorker().run()
