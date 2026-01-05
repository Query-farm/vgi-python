"""Tests for typed work queue methods: enqueue_work_items and dequeue_work_item."""

from __future__ import annotations

import pickle
from dataclasses import dataclass
from typing import Any, ClassVar, Self, cast

import pyarrow as pa

from vgi.arguments import Arg, Arguments
from vgi.function import Serializable
from vgi.invocation import InitResult
from vgi.table_function import (
    Output,
    OutputGenerator,
    TableCardinality,
    TableFunctionGenerator,
    TableFunctionInitInput,
)
from vgi.testing import TableFunctionTestClient


@dataclass
class RangeWorkItem:
    """A work item representing a range of integers to generate."""

    start: int
    end: int

    def serialize(self) -> bytes:
        """Serialize this work item to bytes."""
        return pickle.dumps((self.start, self.end))

    @classmethod
    def deserialize(cls, data: bytes) -> Self:
        """Deserialize a work item from bytes."""
        start, end = pickle.loads(data)
        return cls(start, end)


@dataclass
class FileChunk:
    """A work item representing a chunk of a file to process."""

    path: str
    offset: int
    length: int

    def serialize(self) -> bytes:
        """Serialize this work item to bytes."""
        return pickle.dumps(
            {
                "path": self.path,
                "offset": self.offset,
                "length": self.length,
            }
        )

    @classmethod
    def deserialize(cls, data: bytes) -> Self:
        """Deserialize a work item from bytes."""
        d = pickle.loads(data)
        return cls(path=d["path"], offset=d["offset"], length=d["length"])


class TypedPartitionedRangeFunction(TableFunctionGenerator):
    """A table function that uses typed work queue methods.

    This is similar to PartitionedRangeFunction but uses the typed
    enqueue_work_items and dequeue_work_item methods.
    """

    class Meta:
        """Metadata for TypedPartitionedRangeFunction."""

        name = "typed_partitioned_range"
        description = "Generates a partitioned range using typed work queue"
        max_workers = 1  # Single worker for deterministic testing

    count: int = Arg[int](0, doc="Total count")  # type: ignore[assignment]

    CHUNK_SIZE: ClassVar[int] = 10

    @property
    def output_schema(self) -> pa.Schema:
        """Return output schema with single integer column."""
        return pa.schema([pa.field("value", pa.int64())])

    @property
    def cardinality(self) -> TableCardinality:
        """Return cardinality estimate."""
        return TableCardinality(estimate=self.count, max=self.count)

    def initialize_global_state(self, init_input: pa.RecordBatch) -> InitResult:
        """Populate the work queue with typed range work items."""
        self.init_input = TableFunctionInitInput.deserialize(init_input)
        self.execution_identifier = self.storage.global_put(self.init_input.serialize())

        # Create typed work items
        work_items: list[RangeWorkItem] = []
        for start in range(0, self.count, self.CHUNK_SIZE):
            end = min(start + self.CHUNK_SIZE, self.count)
            work_items.append(RangeWorkItem(start=start, end=end))

        if work_items:
            self.enqueue_work_items(work_items)

        return InitResult(self.execution_identifier)

    def process(self) -> OutputGenerator:
        """Generate values by pulling typed work items from the queue."""
        while True:
            item = self.dequeue_work_item(RangeWorkItem)
            if item is None:
                break

            # item is fully typed as RangeWorkItem
            values = list(range(item.start, item.end))
            yield Output(
                pa.RecordBatch.from_pydict({"value": values}, schema=self.output_schema)
            )


class FileProcessingFunction(TableFunctionGenerator):
    """A table function that simulates file chunk processing with typed work items."""

    class Meta:
        """Metadata for FileProcessingFunction."""

        name = "file_processing"
        description = "Simulates processing file chunks"
        max_workers = 1

    num_files: int = Arg[int](0, doc="Number of files")  # type: ignore[assignment]
    chunks_per_file: int = Arg[int](1, doc="Chunks per file")  # type: ignore[assignment]

    @property
    def output_schema(self) -> pa.Schema:
        """Return output schema with file chunk columns."""
        return pa.schema(
            cast(
                list[tuple[str, pa.DataType]],
                [
                    ("path", pa.string()),
                    ("offset", pa.int64()),
                    ("length", pa.int64()),
                ],
            )
        )

    @property
    def cardinality(self) -> TableCardinality:
        """Return cardinality estimate."""
        return TableCardinality(
            estimate=self.num_files * self.chunks_per_file,
            max=self.num_files * self.chunks_per_file,
        )

    def initialize_global_state(self, init_input: pa.RecordBatch) -> InitResult:
        """Populate the work queue with file chunks."""
        self.init_input = TableFunctionInitInput.deserialize(init_input)
        self.execution_identifier = self.storage.global_put(self.init_input.serialize())

        # Create file chunk work items
        work_items: list[FileChunk] = []
        for i in range(self.num_files):
            for j in range(self.chunks_per_file):
                work_items.append(
                    FileChunk(
                        path=f"/data/file_{i}.csv",
                        offset=j * 1000,
                        length=1000,
                    )
                )

        if work_items:
            self.enqueue_work_items(work_items)

        return InitResult(self.execution_identifier)

    def process(self) -> OutputGenerator:
        """Process file chunks from the typed work queue."""
        while True:
            chunk = self.dequeue_work_item(FileChunk)
            if chunk is None:
                break

            # chunk is fully typed as FileChunk
            yield Output(
                pa.RecordBatch.from_pydict(
                    {
                        "path": [chunk.path],
                        "offset": [chunk.offset],
                        "length": [chunk.length],
                    },
                    schema=self.output_schema,
                )
            )


def _sorted_values(values: list[Any]) -> list[int]:
    """Sort a list of values, filtering out None."""
    return sorted(v for v in values if v is not None)


class TestTypedWorkQueueMethods:
    """Tests for enqueue_work_items and dequeue_work_item methods."""

    def test_typed_range_generates_correct_values(self) -> None:
        """Typed work queue should produce the complete range of values."""
        with TableFunctionTestClient(TypedPartitionedRangeFunction) as client:
            outputs = list(
                client.table_function(arguments=Arguments(positional=(pa.scalar(25),)))
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 25

        values = _sorted_values(table.column("value").to_pylist())
        assert values == list(range(25))

    def test_typed_range_empty_count(self) -> None:
        """Typed work queue with count=0 should produce no output."""
        with TableFunctionTestClient(TypedPartitionedRangeFunction) as client:
            outputs = list(
                client.table_function(arguments=Arguments(positional=(pa.scalar(0),)))
            )

        assert len(outputs) == 0

    def test_typed_range_single_chunk(self) -> None:
        """Typed work queue with small count should produce single chunk."""
        with TableFunctionTestClient(TypedPartitionedRangeFunction) as client:
            outputs = list(
                client.table_function(arguments=Arguments(positional=(pa.scalar(5),)))
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 5

        values = _sorted_values(table.column("value").to_pylist())
        assert values == list(range(5))

    def test_file_processing_generates_all_chunks(self) -> None:
        """File processing function should generate all file chunks."""
        with TableFunctionTestClient(FileProcessingFunction) as client:
            outputs = list(
                client.table_function(
                    arguments=Arguments(positional=(pa.scalar(3), pa.scalar(2)))
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 6  # 3 files * 2 chunks

        # Verify we got all expected file paths
        paths = set(table.column("path").to_pylist())
        assert paths == {"/data/file_0.csv", "/data/file_1.csv", "/data/file_2.csv"}

    def test_file_processing_chunk_structure(self) -> None:
        """File chunks should have correct offset and length."""
        with TableFunctionTestClient(FileProcessingFunction) as client:
            outputs = list(
                client.table_function(
                    arguments=Arguments(positional=(pa.scalar(1), pa.scalar(3)))
                )
            )

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 3

        # All from same file
        paths = table.column("path").to_pylist()
        assert all(p == "/data/file_0.csv" for p in paths)

        # Offsets should be 0, 1000, 2000
        offsets = _sorted_values(table.column("offset").to_pylist())
        assert offsets == [0, 1000, 2000]

        # All lengths should be 1000
        lengths = table.column("length").to_pylist()
        assert all(ln == 1000 for ln in lengths)


class TestSerializableProtocol:
    """Tests for the Serializable protocol implementation."""

    def test_range_work_item_roundtrip(self) -> None:
        """RangeWorkItem should serialize and deserialize correctly."""
        original = RangeWorkItem(start=100, end=200)
        serialized = original.serialize()
        restored = RangeWorkItem.deserialize(serialized)

        assert restored.start == 100
        assert restored.end == 200

    def test_file_chunk_roundtrip(self) -> None:
        """FileChunk should serialize and deserialize correctly."""
        original = FileChunk(path="/data/test.csv", offset=5000, length=1000)
        serialized = original.serialize()
        restored = FileChunk.deserialize(serialized)

        assert restored.path == "/data/test.csv"
        assert restored.offset == 5000
        assert restored.length == 1000

    def test_range_work_item_conforms_to_serializable(self) -> None:
        """RangeWorkItem should satisfy the Serializable protocol."""
        item = RangeWorkItem(start=0, end=10)

        # Check it has the required methods
        assert hasattr(item, "serialize")
        assert hasattr(RangeWorkItem, "deserialize")
        assert callable(item.serialize)
        assert callable(RangeWorkItem.deserialize)

        # Protocol conformance (structural typing)
        def accepts_serializable(s: Serializable) -> bytes:
            return s.serialize()

        result = accepts_serializable(item)
        assert isinstance(result, bytes)
