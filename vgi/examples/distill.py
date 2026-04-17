# ruff: noqa: D102, D106
"""LLM-powered topic distillation aggregate function.

Accumulates labels during UPDATE, then in FINALIZE runs a multi-level
pyramid normalization: chunk → normalize → consolidate → theme.
Each level uses the LLM to group items, with validation and repair
loops to ensure every input is mapped and target sizes are respected.

Requires the ``anthropic`` package: ``pip install vgi[llm]``

Example SQL::

    SELECT qf_llm_distill(label, 3, '[30, 6]')
    FROM all_labels

Returns a JSON object mapping each input label to its path through all levels:
    {"weekend-brunch-slow": ["service-timing", "Service & Staff"], ...}
"""

from __future__ import annotations

import json
import logging
import time
from collections import Counter
from dataclasses import dataclass
from typing import Annotated

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType

from vgi.aggregate_function import AggregateFunction
from vgi.arguments import ConstParam, Param, Returns
from vgi.metadata import NullHandling, OrderDependence
from vgi.table_function import ProcessParams

logger = logging.getLogger(__name__)

__all__ = ["DistillFunction"]

_MAX_RETRIES = 3
_CHUNK_SIZE = 128


@dataclass(kw_only=True)
class DistillState(ArrowSerializableDataclass):
    """Accumulated labels as newline-separated text with counts."""

    labels_json: Annotated[str, ArrowType(pa.string())] = "{}"


class DistillFunction(AggregateFunction[DistillState]):
    """Multi-level topic distillation aggregate.

    Accumulates labels during UPDATE, then runs a pyramid normalization
    in FINALIZE to produce a hierarchical taxonomy.

    SQL: ``SELECT qf_llm_distill(label, 3, '[30, 6]') FROM t``
    """

    class Meta:
        name = "qf_llm_distill"
        description = "Distill labels into a multi-level taxonomy"
        null_handling = NullHandling.DEFAULT
        order_dependent = OrderDependence.NOT_ORDER_DEPENDENT

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> DistillState:
        return DistillState()

    @classmethod
    def update(
        cls,
        states: dict[int, DistillState],
        group_ids: pa.Int64Array,
        label: Annotated[pa.StringArray, Param(doc="Label to distill")],
        target_sizes: Annotated[
            list,
            ConstParam("Target sizes per level as integer array, e.g. [30, 6]", arrow_type=pa.list_(pa.int32())),
        ] = None,
    ) -> None:
        for i in range(len(group_ids)):
            gid = group_ids[i].as_py()
            val = label[i].as_py()
            if val is None:
                continue
            s = states[gid]
            counts: dict[str, int] = json.loads(s.labels_json)
            counts[val] = counts.get(val, 0) + 1
            states[gid] = DistillState(labels_json=json.dumps(counts))

    @classmethod
    def combine(cls, source: DistillState, target: DistillState, params: ProcessParams[None]) -> DistillState:
        src_counts: dict[str, int] = json.loads(source.labels_json)
        tgt_counts: dict[str, int] = json.loads(target.labels_json)
        for k, v in src_counts.items():
            tgt_counts[k] = tgt_counts.get(k, 0) + v
        return DistillState(labels_json=json.dumps(tgt_counts))

    @classmethod
    def finalize(
        cls,
        group_ids: pa.Int64Array,
        states: dict[int, DistillState],
        params: ProcessParams[None],
    ) -> Annotated[pa.RecordBatch, Returns(pa.string())]:
        results: list[str | None] = []
        # Get const param: target_sizes as integer list
        target_sizes = [20]
        if params.args and params.args.positional and params.args.positional[0] is not None:
            val = params.args.positional[0].as_py()
            if isinstance(val, list):
                target_sizes = val
            elif isinstance(val, str):
                target_sizes = json.loads(val)
        num_levels = len(target_sizes) + 1

        for gid in group_ids:
            s = states[gid.as_py()]
            if s is None:
                results.append(None)
                continue
            counts: dict[str, int] = json.loads(s.labels_json)
            if not counts:
                results.append(None)
                continue
            taxonomy = cls._build_taxonomy(counts, num_levels, target_sizes)
            results.append(json.dumps(taxonomy))

        return pa.record_batch({"result": pa.array(results, type=pa.string())})

    @classmethod
    def _build_taxonomy(
        cls,
        label_counts: dict[str, int],
        num_levels: int,
        target_sizes: list[int],
    ) -> dict[str, list[str]]:
        """Build the full multi-level taxonomy.

        Returns: {source_label: [level1_cat, level2_cat, ...], ...}
        """
        # Pad target_sizes if fewer than num_levels - 1
        while len(target_sizes) < num_levels - 1:
            target_sizes.append(max(target_sizes[-1] // 3, 3) if target_sizes else 10)

        all_labels = list(label_counts.keys())
        logger.info("Building taxonomy: %d labels, %d levels, targets=%s", len(all_labels), num_levels, target_sizes)

        # Level 0 is the raw labels. Build mappings level by level.
        # level_mappings[i] maps items at level i to items at level i+1
        level_mappings: list[dict[str, str]] = []
        current_items = label_counts  # {item: count}

        for level_idx in range(num_levels - 1):
            target = target_sizes[level_idx]
            logger.info("Level %d: %d items -> target %d", level_idx + 1, len(current_items), target)

            if len(current_items) <= target:
                # Already at or below target — identity mapping
                mapping = {k: k for k in current_items}
            else:
                mapping = cls._run_level(current_items, target)

            level_mappings.append(mapping)

            # Build next level's input: group counts by mapped value
            next_items: dict[str, int] = {}
            for item, count in current_items.items():
                mapped = mapping.get(item, item)
                next_items[mapped] = next_items.get(mapped, 0) + count
            current_items = next_items

            logger.info("Level %d produced %d groups", level_idx + 1, len(current_items))

        # Build the full path for each source label
        taxonomy: dict[str, list[str]] = {}
        for label in all_labels:
            path: list[str] = []
            current = label
            for mapping in level_mappings:
                current = mapping.get(current, current)
                path.append(current)
            taxonomy[label] = path

        return taxonomy

    @classmethod
    def _run_level(
        cls,
        items: dict[str, int],
        target_size: int,
    ) -> dict[str, str]:
        """Run one level of normalization with validation and repair.

        Chunks items into groups of _CHUNK_SIZE, normalizes each chunk,
        then validates that all items are mapped and target size is met.
        """
        item_list = sorted(items.keys(), key=lambda k: -items[k])

        # Phase 1: Chunk and normalize
        all_mappings: dict[str, str] = {}
        for i in range(0, len(item_list), _CHUNK_SIZE):
            chunk = item_list[i : i + _CHUNK_SIZE]
            chunk_with_counts = [f"{label} ({items[label]}x)" for label in chunk]

            result = cls._call_llm(
                "\n".join(chunk_with_counts),
                f"Group these items into natural categories. Use short kebab-case names. "
                f"Aim for roughly {target_size} total categories across all batches. "
                f"Every item MUST appear exactly once. Return one mapping per line:\n"
                f"original-item|category-name",
            )
            parsed = cls._parse_pipe_output(result)

            # Map back to original labels (strip the count suffix)
            for label in chunk:
                key_with_count = f"{label} ({items[label]}x)"
                mapped = parsed.get(key_with_count) or parsed.get(label)
                if mapped:
                    all_mappings[label] = mapped

        # Phase 2: Validate — check all items mapped
        missing = [k for k in items if k not in all_mappings]
        if missing:
            logger.warning("Level missing %d/%d items, repairing...", len(missing), len(items))
            all_mappings = cls._repair_missing(all_mappings, missing, items)

        # Phase 3: Check target size
        unique_groups = set(all_mappings.values())
        if len(unique_groups) > target_size * 1.5:
            logger.info("Too many groups (%d > %d), consolidating...", len(unique_groups), target_size)
            all_mappings = cls._consolidate_groups(all_mappings, items, target_size)

        return all_mappings

    @classmethod
    def _repair_missing(
        cls,
        mappings: dict[str, str],
        missing: list[str],
        items: dict[str, int],
    ) -> dict[str, str]:
        """Repair missing mappings by re-submitting unmapped items."""
        existing_categories = sorted(set(mappings.values()))
        cat_list = ", ".join(existing_categories[:50])

        for attempt in range(_MAX_RETRIES):
            chunk_text = "\n".join(f"{label} ({items[label]}x)" for label in missing)
            result = cls._call_llm(
                chunk_text,
                f"Map each item to one of these existing categories, or create a new one if none fit. "
                f"Existing categories: {cat_list}\n"
                f"Every item MUST appear exactly once. Return one mapping per line:\n"
                f"original-item|category-name",
            )
            parsed = cls._parse_pipe_output(result)
            for label in list(missing):
                key_with_count = f"{label} ({items[label]}x)"
                mapped = parsed.get(key_with_count) or parsed.get(label)
                if mapped:
                    mappings[label] = mapped
                    missing.remove(label)

            if not missing:
                break
            logger.warning("Still missing %d items after attempt %d", len(missing), attempt + 1)

        # Last resort: map remaining to "other"
        for label in missing:
            mappings[label] = "other"
            logger.warning("Forced '%s' to 'other'", label)

        return mappings

    @classmethod
    def _consolidate_groups(
        cls,
        mappings: dict[str, str],
        items: dict[str, int],
        target_size: int,
    ) -> dict[str, str]:
        """Merge groups to get closer to target size."""
        group_counts: Counter[str] = Counter()
        for label, group in mappings.items():
            group_counts[group] += items.get(label, 1)

        group_list = [f"{group} ({count} items)" for group, count in group_counts.most_common()]
        result = cls._call_llm(
            "\n".join(group_list),
            f"Merge these categories into no more than {target_size} groups. "
            f"Combine near-duplicates and small groups. "
            f"Every category MUST appear exactly once. Return one mapping per line:\n"
            f"original-category|merged-category",
        )
        group_mapping = cls._parse_pipe_output(result)

        # Apply the group merging
        for label in mappings:
            old_group = mappings[label]
            key_with_count = f"{old_group} ({group_counts[old_group]} items)"
            new_group = group_mapping.get(key_with_count) or group_mapping.get(old_group, old_group)
            mappings[label] = new_group

        return mappings

    @classmethod
    def _parse_pipe_output(cls, text: str) -> dict[str, str]:
        """Parse pipe-delimited LLM output into a dict."""
        mappings: dict[str, str] = {}
        for line in text.split("\n"):
            if "|" not in line:
                continue
            parts = line.split("|", 1)
            key = parts[0].strip()
            val = parts[1].strip()
            if key and val:
                mappings[key] = val
        return mappings

    @classmethod
    def _call_llm(cls, text: str, prompt: str) -> str:
        """Call the LLM with retry on rate limit."""
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
                            "content": f"{prompt}\n\n{text}",
                        }
                    ],
                )
                return response.content[0].text
            except anthropic.RateLimitError:
                wait = 2**attempt * 5
                logger.warning("Rate limited, retrying in %ds (attempt %d/5)", wait, attempt + 1)
                time.sleep(wait)
        msg = "Rate limit exceeded after 5 retries"
        raise RuntimeError(msg)
