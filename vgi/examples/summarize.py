# ruff: noqa: D102, D106
"""LLM-powered summarize aggregate function.

Accumulates text per group during UPDATE, then calls an LLM in FINALIZE
to produce a summary. The prompt is a regular column, so it can vary per
rollup level using GROUPING_ID in SQL.

Requires the ``anthropic`` package: ``pip install vgi[llm]``

Example SQL::

    SELECT department, category,
           qf_llm_summarize(
               review_body,
               CASE GROUPING_ID(department, category)
                   WHEN 0 THEN 'Summarize these product reviews.'
                   WHEN 1 THEN 'Identify trends across this department.'
                   WHEN 3 THEN 'High-level overview of all reviews.'
               END
           )
    FROM reviews
    GROUP BY ROLLUP(department, category)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Annotated

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType

from vgi.aggregate_function import AggregateFunction
from vgi.arguments import Param, Returns
from vgi.metadata import NullHandling, OrderDependence
from vgi.table_function import ProcessParams

logger = logging.getLogger(__name__)

__all__ = ["SummarizeFunction"]

# Maximum chars of accumulated text before triggering LLM compression
# during combine(). Keeps state size bounded for high-cardinality rollups.
_COMPRESS_THRESHOLD = 50_000

# Default prompt when the user doesn't provide one.
_DEFAULT_PROMPT = "Summarize the following text."


@dataclass(kw_only=True)
class SummarizeState(ArrowSerializableDataclass):
    """Accumulated text and metadata for one group."""

    texts: Annotated[str, ArrowType(pa.string())] = ""
    count: Annotated[int, ArrowType(pa.int64())] = 0
    prompt: Annotated[str, ArrowType(pa.string())] = ""


class SummarizeFunction(AggregateFunction[SummarizeState]):
    """LLM-powered text summarization aggregate.

    Accumulates text during UPDATE, optionally compresses during COMBINE
    when state exceeds a size threshold, and produces a summary per group
    in FINALIZE via a single LLM call.

    SQL: ``SELECT qf_llm_summarize(text_col, prompt_col) FROM t GROUP BY category``
    """

    class Meta:
        name = "qf_llm_summarize"
        description = "Summarize text using an LLM"
        null_handling = NullHandling.DEFAULT
        order_dependent = OrderDependence.NOT_ORDER_DEPENDENT

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> SummarizeState:
        return SummarizeState()

    @classmethod
    def update(
        cls,
        states: dict[int, SummarizeState],
        group_ids: pa.Int64Array,
        text: Annotated[pa.StringArray, Param(doc="Text to summarize")],
        prompt: Annotated[pa.StringArray, Param(doc="Prompt for the LLM")],
    ) -> None:
        for i in range(len(group_ids)):
            gid = group_ids[i].as_py()
            val = text[i].as_py()
            if val is None:
                continue
            s = states[gid]
            sep = "\n---\n" if s.texts else ""
            states[gid] = SummarizeState(
                texts=s.texts + sep + val,
                count=s.count + 1,
                prompt=prompt[i].as_py() or _DEFAULT_PROMPT,
            )

    @classmethod
    def combine(
        cls,
        source: SummarizeState,
        target: SummarizeState,
        params: ProcessParams[None],
    ) -> SummarizeState:
        if not source.texts:
            return target
        if not target.texts:
            return source
        combined = target.texts + "\n---\n" + source.texts
        total = source.count + target.count
        prompt = target.prompt or source.prompt
        # Compress via LLM if accumulated text is too large
        if len(combined) > _COMPRESS_THRESHOLD:
            logger.info(
                "Compressing %d chars (%d items) in combine",
                len(combined),
                total,
            )
            combined = cls._compress(combined)
        return SummarizeState(texts=combined, count=total, prompt=prompt)

    @classmethod
    def finalize(
        cls,
        group_ids: pa.Int64Array,
        states: dict[int, SummarizeState],
        params: ProcessParams[None],
    ) -> Annotated[pa.RecordBatch, Returns(pa.string())]:
        from concurrent.futures import Future, ThreadPoolExecutor

        # Submit LLM calls concurrently, capped at 10 to avoid rate limits
        gid_list = [gid.as_py() for gid in group_ids]
        futures: dict[int, Future[str]] = {}
        with ThreadPoolExecutor(max_workers=10) as pool:
            for gid in gid_list:
                s = states[gid]
                if s is not None and s.texts:
                    futures[gid] = pool.submit(cls._summarize, s.texts, s.count, s.prompt)

        results: list[str | None] = [futures[gid].result() if gid in futures else None for gid in gid_list]
        return pa.record_batch({"result": pa.array(results, type=pa.string())})

    @classmethod
    def _summarize(cls, texts: str, count: int, prompt: str) -> str:
        """Call the LLM to produce a final summary, with retry on rate limit."""
        import time

        import anthropic

        client = anthropic.Anthropic()
        for attempt in range(5):
            try:
                response = client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=8192,
                    messages=[
                        {
                            "role": "user",
                            "content": f"{prompt}\n\n({count} items)\n\n{texts}",
                        }
                    ],
                )
                return response.content[0].text
            except anthropic.RateLimitError:
                wait = 2**attempt * 5  # 5, 10, 20, 40, 80 seconds
                logger.warning("Rate limited, retrying in %ds (attempt %d/5)", wait, attempt + 1)
                time.sleep(wait)
        msg = "Rate limit exceeded after 5 retries"
        raise RuntimeError(msg)

    @classmethod
    def _compress(cls, texts: str) -> str:
        """Compress accumulated text to stay within token limits."""
        import time

        import anthropic

        client = anthropic.Anthropic()
        for attempt in range(5):
            try:
                response = client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=2048,
                    messages=[
                        {
                            "role": "user",
                            "content": (
                                "Condense the following into key themes and "
                                "representative details. Preserve specifics that "
                                "would be lost in a generic summary.\n\n" + texts
                            ),
                        }
                    ],
                )
                return response.content[0].text
            except anthropic.RateLimitError:
                wait = 2**attempt * 5
                logger.warning("Rate limited in compress, retrying in %ds (attempt %d/5)", wait, attempt + 1)
                time.sleep(wait)
        msg = "Rate limit exceeded after 5 retries"
        raise RuntimeError(msg)
