"""Unit tests for VGI protocol classes.

Tests cover Invocation, Arguments, GlobalInitResult, and table_function classes.
"""

import pyarrow as pa
import pytest

from vgi.function import (
    Arg,
    Arguments,
    ArgumentValidationError,
    GlobalInitResult,
    Invocation,
)
from vgi.log import Level, Message
from vgi.table_function import (
    CardinalityInfo,
    GlobalStateInitInput,
    OutputSpec,
)


class TestArguments:
    """Tests for Arguments encoding and decoding."""

    def test_empty_arguments(self) -> None:
        """Empty arguments should round-trip correctly."""
        args = Arguments()
        encoded = args.encoded_dict()
        assert encoded == {}

        # Decode from empty struct
        schema = pa.schema([pa.field("args", pa.struct([]))])
        batch = pa.RecordBatch.from_pylist([{"args": {}}], schema=schema)
        decoded = Arguments.decode(batch.column("args")[0])
        assert decoded.positional == ()
        assert decoded.named is None

    def test_positional_arguments_only(self) -> None:
        """Positional-only arguments should encode/decode correctly."""
        args = Arguments(
            positional=(pa.scalar(42), pa.scalar("hello"), pa.scalar(3.14)),
            named={},
        )
        encoded = args.encoded_dict()
        assert "positional_0" in encoded
        assert "positional_1" in encoded
        assert "positional_2" in encoded

        # Round-trip via RecordBatch
        batch = pa.RecordBatch.from_pylist([encoded])
        struct_array = pa.StructArray.from_arrays(
            [batch.column(name) for name in batch.schema.names],
            names=batch.schema.names,
        )
        decoded = Arguments.decode(struct_array[0])

        assert len(decoded.positional) == 3
        assert decoded.positional[0] is not None
        assert decoded.positional[0].as_py() == 42
        assert decoded.positional[1] is not None
        assert decoded.positional[1].as_py() == "hello"
        assert decoded.positional[2] is not None
        assert decoded.positional[2].as_py() == 3.14
        assert decoded.named is None

    def test_named_arguments_only(self) -> None:
        """Named-only arguments should encode/decode correctly."""
        args = Arguments(
            positional=(),
            named={
                "count": pa.scalar(10),
                "name": pa.scalar("test"),
            },
        )
        encoded = args.encoded_dict()
        assert "named_count" in encoded
        assert "named_name" in encoded

        # Round-trip via RecordBatch
        batch = pa.RecordBatch.from_pylist([encoded])
        struct_array = pa.StructArray.from_arrays(
            [batch.column(name) for name in batch.schema.names],
            names=batch.schema.names,
        )
        decoded = Arguments.decode(struct_array[0])

        assert decoded.positional == ()
        assert decoded.named is not None
        assert len(decoded.named) == 2
        assert decoded.named["count"].as_py() == 10
        assert decoded.named["name"].as_py() == "test"

    def test_mixed_arguments(self) -> None:
        """Mixed positional and named arguments should encode/decode correctly."""
        args = Arguments(
            positional=tuple([pa.scalar(1), pa.scalar(2)]),
            named={"key": pa.scalar("value")},
        )
        encoded = args.encoded_dict()
        assert "positional_0" in encoded
        assert "positional_1" in encoded
        assert "named_key" in encoded

        batch = pa.RecordBatch.from_pylist([encoded])
        struct_array = pa.StructArray.from_arrays(
            [batch.column(name) for name in batch.schema.names],
            names=batch.schema.names,
        )
        decoded = Arguments.decode(struct_array[0])

        assert len(decoded.positional) == 2
        assert decoded.positional[0] is not None
        assert decoded.positional[0].as_py() == 1
        assert decoded.positional[1] is not None
        assert decoded.positional[1].as_py() == 2
        assert decoded.named is not None
        assert decoded.named["key"].as_py() == "value"

    def test_null_positional_argument(self) -> None:
        """Null values in positional arguments should be preserved."""
        args = Arguments(positional=(None, pa.scalar(42), None), named={})
        encoded = args.encoded_dict()

        assert encoded["positional_0"] is None
        pos_1 = encoded["positional_1"]
        assert pos_1 is not None
        assert pos_1.as_py() == 42
        assert encoded["positional_2"] is None

    def test_schema_generation(self) -> None:
        """Arguments.schema() should produce valid Arrow schema."""
        args = Arguments(
            positional=(pa.scalar(42), pa.scalar("text")),
            named={"flag": pa.scalar(True)},
        )
        schema = args.schema()

        assert "positional_0" in schema.names
        assert "positional_1" in schema.names
        assert "named_flag" in schema.names
        assert schema.field("positional_0").type == pa.int64()
        assert schema.field("positional_1").type == pa.string()
        assert schema.field("named_flag").type == pa.bool_()


class TestInvocation:
    """Tests for Invocation serialization and deserialization."""

    def test_basic_round_trip(self) -> None:
        """Basic Invocation should serialize and deserialize correctly."""
        original = Invocation(
            function_name="test_function",
            arguments=Arguments(positional=(pa.scalar(42),), named={}),
            in_out_function_input_schema=pa.schema([pa.field("col1", pa.int64())]),
            correlation_id="test-123",
            invocation_id=b"bind-id-bytes",
        )

        serialized = original.serialize()
        assert isinstance(serialized, bytes)
        assert len(serialized) > 0

        # Deserialize
        from pyarrow import ipc

        reader = ipc.open_stream(serialized)
        batch = reader.read_next_batch()
        deserialized = Invocation.deserialize(batch)

        assert deserialized.function_name == original.function_name
        assert deserialized.correlation_id == original.correlation_id
        assert deserialized.invocation_id == original.invocation_id
        assert (
            deserialized.in_out_function_input_schema
            == original.in_out_function_input_schema
        )
        assert len(deserialized.arguments.positional) == 1
        assert deserialized.arguments.positional[0] is not None
        assert deserialized.arguments.positional[0].as_py() == 42

    def test_null_schema(self) -> None:
        """Invocation with null input schema should round-trip correctly."""
        original = Invocation(
            function_name="scalar_function",
            in_out_function_input_schema=None,
            correlation_id="",
            invocation_id=None,
        )

        serialized = original.serialize()
        from pyarrow import ipc

        reader = ipc.open_stream(serialized)
        batch = reader.read_next_batch()
        deserialized = Invocation.deserialize(batch)

        assert deserialized.function_name == "scalar_function"
        assert deserialized.in_out_function_input_schema is None
        assert deserialized.invocation_id is None

    def test_complex_schema(self) -> None:
        """Invocation with complex schema should round-trip correctly."""
        complex_schema = pa.schema(
            [
                pa.field("int_col", pa.int32()),
                pa.field("float_col", pa.float64()),
                pa.field("string_col", pa.string()),
                pa.field("list_col", pa.list_(pa.int64())),
                pa.field("struct_col", pa.struct([pa.field("nested", pa.string())])),
            ]
        )

        original = Invocation(
            function_name="complex_function",
            in_out_function_input_schema=complex_schema,
            correlation_id="complex-test",
            invocation_id=b"complex-bind",
        )

        serialized = original.serialize()
        from pyarrow import ipc

        reader = ipc.open_stream(serialized)
        batch = reader.read_next_batch()
        deserialized = Invocation.deserialize(batch)

        assert deserialized.in_out_function_input_schema == complex_schema

    def test_deserialize_empty_batch_raises(self) -> None:
        """Deserializing empty batch should raise ValueError."""
        empty_batch = pa.RecordBatch.from_pylist(
            [],
            schema=pa.schema(
                [
                    pa.field("function_name", pa.string()),
                    pa.field("arguments", pa.struct([])),
                    pa.field("in_out_function_input_schema", pa.binary()),
                    pa.field("invocation_id", pa.binary()),
                    pa.field("correlation_id", pa.string()),
                ]
            ),
        )

        with pytest.raises(ValueError, match="empty RecordBatch"):
            Invocation.deserialize(empty_batch)

    def test_deserialize_multi_row_batch_raises(self) -> None:
        """Deserializing multi-row batch should raise ValueError."""
        multi_row_batch = pa.RecordBatch.from_pylist(
            [
                {
                    "function_name": "fn1",
                    "arguments": {},
                    "in_out_function_input_schema": None,
                    "invocation_id": None,
                    "correlation_id": "",
                },
                {
                    "function_name": "fn2",
                    "arguments": {},
                    "in_out_function_input_schema": None,
                    "invocation_id": None,
                    "correlation_id": "",
                },
            ]
        )

        with pytest.raises(ValueError, match="single-row"):
            Invocation.deserialize(multi_row_batch)

    def test_with_global_init_identifier(self) -> None:
        """Test that with_global_init_identifier creates a new Invocation."""
        original = Invocation(
            function_name="test",
            in_out_function_input_schema=None,
            correlation_id="test",
            invocation_id=None,
            global_init_identifier=None,
        )

        init_result = GlobalInitResult(global_init_identifier=b"init-data")
        updated = original.with_global_init_identifier(init_result)

        assert updated.function_name == original.function_name
        assert updated.global_init_identifier == init_result
        assert original.global_init_identifier is None  # Original unchanged


class TestGlobalInitResult:
    """Tests for GlobalInitResult serialization."""

    def test_basic_round_trip(self) -> None:
        """GlobalInitResult should serialize and deserialize correctly."""
        original = GlobalInitResult(global_init_identifier=b"test-init-id")

        serialized = original.serialize()
        assert isinstance(serialized, bytes)

        from pyarrow import ipc

        reader = ipc.open_stream(serialized)
        batch = reader.read_next_batch()
        deserialized = GlobalInitResult.deserialize(batch)

        assert deserialized.global_init_identifier == b"test-init-id"

    def test_null_identifier(self) -> None:
        """GlobalInitResult with null identifier should round-trip correctly."""
        original = GlobalInitResult(global_init_identifier=None)

        serialized = original.serialize()
        from pyarrow import ipc

        reader = ipc.open_stream(serialized)
        batch = reader.read_next_batch()
        deserialized = GlobalInitResult.deserialize(batch)

        assert deserialized.global_init_identifier is None

    def test_has_identifier_true(self) -> None:
        """has_identifier should return True when field exists."""
        batch = pa.RecordBatch.from_pylist(
            [{"global_init_identifier": b"some-id"}],
            schema=pa.schema(
                [pa.field("global_init_identifier", pa.binary(), nullable=True)]
            ),
        )
        assert GlobalInitResult.has_identifier(batch) is True

    def test_has_identifier_false(self) -> None:
        """has_identifier should return False when field doesn't exist."""
        batch = pa.RecordBatch.from_pylist(
            [{"other_field": "value"}],
            schema=pa.schema([pa.field("other_field", pa.string())]),
        )
        assert GlobalInitResult.has_identifier(batch) is False

    def test_deserialize_empty_batch_raises(self) -> None:
        """Deserializing empty batch should raise ValueError."""
        empty_batch = pa.RecordBatch.from_pylist(
            [],
            schema=pa.schema(
                [pa.field("global_init_identifier", pa.binary(), nullable=True)]
            ),
        )

        with pytest.raises(ValueError, match="empty RecordBatch"):
            GlobalInitResult.deserialize(empty_batch)

    def test_deserialize_multi_row_batch_raises(self) -> None:
        """Deserializing multi-row batch should raise ValueError."""
        multi_row_batch = pa.RecordBatch.from_pylist(
            [
                {"global_init_identifier": b"id1"},
                {"global_init_identifier": b"id2"},
            ],
            schema=pa.schema(
                [pa.field("global_init_identifier", pa.binary(), nullable=True)]
            ),
        )

        with pytest.raises(ValueError, match="single-row"):
            GlobalInitResult.deserialize(multi_row_batch)

    def test_schema(self) -> None:
        """schema() should return correct Arrow schema."""
        result = GlobalInitResult(global_init_identifier=b"test")
        schema = result.schema()

        assert len(schema) == 1
        assert schema.field("global_init_identifier").type == pa.binary()
        assert schema.field("global_init_identifier").nullable is True


class TestMessage:
    """Tests for Message convenience methods."""

    def test_exception_method(self) -> None:
        """Message.exception() should create EXCEPTION level message."""
        msg = Message.exception("Something failed", code=500)
        assert msg.level == Level.EXCEPTION
        assert msg.message == "Something failed"
        assert msg.extra == {"code": 500}

    def test_error_method(self) -> None:
        """Message.error() should create ERROR level message."""
        msg = Message.error("An error occurred")
        assert msg.level == Level.ERROR
        assert msg.message == "An error occurred"
        assert msg.extra is None

    def test_warn_method(self) -> None:
        """Message.warn() should create WARN level message."""
        msg = Message.warn("Warning message", count=5)
        assert msg.level == Level.WARN
        assert msg.message == "Warning message"
        assert msg.extra == {"count": 5}

    def test_info_method(self) -> None:
        """Message.info() should create INFO level message."""
        msg = Message.info("Info message")
        assert msg.level == Level.INFO
        assert msg.message == "Info message"

    def test_debug_method(self) -> None:
        """Message.debug() should create DEBUG level message."""
        msg = Message.debug("Debug details", var="value")
        assert msg.level == Level.DEBUG
        assert msg.message == "Debug details"
        assert msg.extra == {"var": "value"}

    def test_trace_method(self) -> None:
        """Message.trace() should create TRACE level message."""
        msg = Message.trace("Trace info")
        assert msg.level == Level.TRACE
        assert msg.message == "Trace info"

    def test_from_exception(self) -> None:
        """Message.from_exception() should capture exception details."""
        try:
            raise ValueError("Test error message")
        except ValueError as e:
            msg = Message.from_exception(e)

        assert msg.level == Level.EXCEPTION
        assert "ValueError" in msg.message
        assert "Test error message" in msg.message

    def test_equality(self) -> None:
        """Message equality should compare all fields."""
        msg1 = Message(Level.INFO, "test", key="value")
        msg2 = Message(Level.INFO, "test", key="value")
        msg3 = Message(Level.INFO, "test", key="other")

        assert msg1 == msg2
        assert msg1 != msg3

    def test_repr(self) -> None:
        """Message repr should be informative."""
        msg = Message(Level.INFO, "test message", extra_key="extra_value")
        repr_str = repr(msg)

        assert "Message" in repr_str
        assert "INFO" in repr_str
        assert "test message" in repr_str


class TestCardinalityInfo:
    """Tests for CardinalityInfo dataclass."""

    def test_basic_creation(self) -> None:
        """CardinalityInfo should store estimate and max values."""
        info = CardinalityInfo(estimate=100, max=1000)
        assert info.estimate == 100
        assert info.max == 1000

    def test_null_values(self) -> None:
        """CardinalityInfo should allow null estimate and max."""
        info = CardinalityInfo(estimate=None, max=None)
        assert info.estimate is None
        assert info.max is None

    def test_partial_values(self) -> None:
        """CardinalityInfo should allow partial information."""
        estimate_only = CardinalityInfo(estimate=50, max=None)
        assert estimate_only.estimate == 50
        assert estimate_only.max is None

        max_only = CardinalityInfo(estimate=None, max=100)
        assert max_only.estimate is None
        assert max_only.max == 100

    def test_exact_cardinality(self) -> None:
        """CardinalityInfo with equal estimate and max indicates exact count."""
        exact = CardinalityInfo(estimate=1, max=1)
        assert exact.estimate == exact.max == 1

    def test_frozen(self) -> None:
        """CardinalityInfo should be immutable (frozen dataclass)."""
        info = CardinalityInfo(estimate=100, max=1000)
        with pytest.raises(AttributeError):
            info.estimate = 200  # type: ignore[misc]


class TestGlobalStateInitInput:
    """Tests for GlobalStateInitInput serialization."""

    def test_basic_round_trip(self) -> None:
        """GlobalStateInitInput should serialize and deserialize correctly."""
        original = GlobalStateInitInput(projection_ids=[0, 2, 4])

        serialized = original.serialize()
        assert isinstance(serialized, bytes)

        from pyarrow import ipc

        reader = ipc.open_stream(serialized)
        batch = reader.read_next_batch()
        deserialized = GlobalStateInitInput.deserialize(batch)

        assert deserialized.projection_ids == [0, 2, 4]

    def test_null_projection_ids(self) -> None:
        """GlobalStateInitInput with null projection_ids should round-trip."""
        original = GlobalStateInitInput(projection_ids=None)

        serialized = original.serialize()
        from pyarrow import ipc

        reader = ipc.open_stream(serialized)
        batch = reader.read_next_batch()
        deserialized = GlobalStateInitInput.deserialize(batch)

        assert deserialized.projection_ids is None

    def test_empty_projection_ids(self) -> None:
        """GlobalStateInitInput with empty list should round-trip."""
        original = GlobalStateInitInput(projection_ids=[])

        serialized = original.serialize()
        from pyarrow import ipc

        reader = ipc.open_stream(serialized)
        batch = reader.read_next_batch()
        deserialized = GlobalStateInitInput.deserialize(batch)

        assert deserialized.projection_ids == []

    def test_default_value(self) -> None:
        """GlobalStateInitInput default should have None projection_ids."""
        default = GlobalStateInitInput()
        assert default.projection_ids is None


class TestTableOutputSpec:
    """Tests for table_function.OutputSpec with cardinality."""

    def test_serialization_with_cardinality(self) -> None:
        """OutputSpec with cardinality should serialize correctly."""
        spec = OutputSpec(
            output_schema=pa.schema([pa.field("col1", pa.int64())]),
            max_processes=4,
            invocation_id=b"test-id",
            cardinality=CardinalityInfo(estimate=100, max=1000),
        )

        serialized = spec.serialize()
        assert isinstance(serialized, bytes)
        assert len(serialized) > 0

    def test_serialization_without_cardinality(self) -> None:
        """OutputSpec without cardinality should serialize correctly."""
        spec = OutputSpec(
            output_schema=pa.schema([pa.field("col1", pa.int64())]),
            max_processes=1,
            invocation_id=b"test-id",
            cardinality=None,
        )

        serialized = spec.serialize()
        assert isinstance(serialized, bytes)

    def test_serialize_schema_includes_cardinality_fields(self) -> None:
        """Serialize schema should include cardinality fields."""
        spec = OutputSpec(
            output_schema=pa.schema([pa.field("col1", pa.int64())]),
            max_processes=1,
            invocation_id=b"test-id",
            cardinality=CardinalityInfo(estimate=50, max=100),
        )

        schema = spec.serialize_schema()
        assert "cardinality_estimated" in schema.names
        assert "cardinality_max" in schema.names

    def test_serialize_dict_includes_cardinality_values(self) -> None:
        """Serialize dict should include cardinality values."""
        spec = OutputSpec(
            output_schema=pa.schema([pa.field("col1", pa.int64())]),
            max_processes=1,
            invocation_id=b"test-id",
            cardinality=CardinalityInfo(estimate=50, max=100),
        )

        data = spec.serialize_dict()
        assert data["cardinality_estimated"] == 50
        assert data["cardinality_max"] == 100

    def test_serialize_dict_null_cardinality(self) -> None:
        """Serialize dict should handle null cardinality."""
        spec = OutputSpec(
            output_schema=pa.schema([pa.field("col1", pa.int64())]),
            max_processes=1,
            invocation_id=b"test-id",
            cardinality=None,
        )

        data = spec.serialize_dict()
        assert data["cardinality_estimated"] is None
        assert data["cardinality_max"] is None


class TestArg:
    """Tests for the Arg descriptor for declarative argument parsing."""

    def test_positional_required(self) -> None:
        """Arg should parse required positional arguments."""

        class MyClass:
            arguments = Arguments(positional=(pa.scalar(42),))
            value = Arg[int](0)

        obj = MyClass()
        assert obj.value == 42

    def test_positional_with_default(self) -> None:
        """Arg should use default when positional argument is missing."""

        class MyClass:
            arguments = Arguments(positional=())
            value = Arg[int](0, default=99)

        obj = MyClass()
        assert obj.value == 99

    def test_named_required(self) -> None:
        """Arg should parse required named arguments."""

        class MyClass:
            arguments = Arguments(named={"name": pa.scalar("hello")})
            name = Arg[str]("name")

        obj = MyClass()
        assert obj.name == "hello"

    def test_named_with_default(self) -> None:
        """Arg should use default when named argument is missing."""

        class MyClass:
            arguments = Arguments(named={})
            separator = Arg[str]("sep", default=",")

        obj = MyClass()
        assert obj.separator == ","

    def test_multiple_args(self) -> None:
        """Arg should work with multiple arguments on same class."""

        class MyClass:
            arguments = Arguments(
                positional=(pa.scalar(10), pa.scalar(20)),
                named={"format": pa.scalar("json")},
            )
            first = Arg[int](0)
            second = Arg[int](1)
            fmt = Arg[str]("format")

        obj = MyClass()
        assert obj.first == 10
        assert obj.second == 20
        assert obj.fmt == "json"

    def test_class_level_access_returns_descriptor(self) -> None:
        """Accessing Arg on class should return the descriptor."""

        class MyClass:
            arguments = Arguments(positional=(pa.scalar(42),))
            value = Arg[int](0)

        assert isinstance(MyClass.value, Arg)
        assert MyClass.value.position == 0

    def test_value_is_cached(self) -> None:
        """Arg should cache the resolved value."""

        class MyClass:
            arguments = Arguments(positional=(pa.scalar(42),))
            value = Arg[int](0)

        obj = MyClass()
        # First access
        _ = obj.value
        # Verify it's cached in __dict__
        assert "value" in obj.__dict__
        assert obj.__dict__["value"] == 42
        # Second access should return cached value
        assert obj.value == 42

    def test_missing_required_raises(self) -> None:
        """Arg should raise when required argument is missing."""

        class MyClass:
            arguments = Arguments(positional=())
            value = Arg[int](0)

        obj = MyClass()
        with pytest.raises(IndexError, match="index out of range"):
            _ = obj.value

    def test_missing_named_required_raises(self) -> None:
        """Arg should raise when required named argument is missing."""

        class MyClass:
            arguments = Arguments(named={})
            name = Arg[str]("name")

        obj = MyClass()
        with pytest.raises(KeyError, match="not found"):
            _ = obj.name

    def test_repr(self) -> None:
        """Arg should have a useful repr."""
        arg1 = Arg[int](0)
        assert repr(arg1) == "Arg(0)"

        arg2 = Arg[int](1, default=10)
        assert repr(arg2) == "Arg(1, default=10)"

        arg3 = Arg[str]("name", default="test", doc="A name")
        assert repr(arg3) == "Arg('name', default='test', doc='A name')"

    def test_with_none_arguments(self) -> None:
        """Arg should handle None named arguments dict."""

        class MyClass:
            arguments = Arguments(positional=(), named=None)
            value = Arg[str]("key", default="default")

        obj = MyClass()
        assert obj.value == "default"

    def test_null_scalar_with_default(self) -> None:
        """Arg should use default when scalar is null."""

        class MyClass:
            arguments = Arguments(positional=(None,))
            value = Arg[int](0, default=99)

        obj = MyClass()
        assert obj.value == 99

    def test_different_instances_independent(self) -> None:
        """Different instances should have independent cached values."""

        class MyClass:
            value = Arg[int](0)

            def __init__(self, args: Arguments):
                self.arguments = args

        obj1 = MyClass(Arguments(positional=(pa.scalar(1),)))
        obj2 = MyClass(Arguments(positional=(pa.scalar(2),)))

        assert obj1.value == 1
        assert obj2.value == 2


class TestArgValidation:
    """Tests for Arg descriptor validation features."""

    def test_ge_validation_pass(self) -> None:
        """Arg ge validation should pass when value >= threshold."""

        class MyClass:
            arguments = Arguments(positional=(pa.scalar(10),))
            value = Arg[int](0, ge=5)

        obj = MyClass()
        assert obj.value == 10

    def test_ge_validation_fail(self) -> None:
        """Arg ge validation should fail when value < threshold."""

        class MyClass:
            arguments = Arguments(positional=(pa.scalar(3),))
            value = Arg[int](0, ge=5)

        obj = MyClass()
        with pytest.raises(ArgumentValidationError, match="must be >= 5"):
            _ = obj.value

    def test_le_validation_pass(self) -> None:
        """Arg le validation should pass when value <= threshold."""

        class MyClass:
            arguments = Arguments(positional=(pa.scalar(10),))
            value = Arg[int](0, le=100)

        obj = MyClass()
        assert obj.value == 10

    def test_le_validation_fail(self) -> None:
        """Arg le validation should fail when value > threshold."""

        class MyClass:
            arguments = Arguments(positional=(pa.scalar(150),))
            value = Arg[int](0, le=100)

        obj = MyClass()
        with pytest.raises(ArgumentValidationError, match="must be <= 100"):
            _ = obj.value

    def test_gt_validation_pass(self) -> None:
        """Arg gt validation should pass when value > threshold."""

        class MyClass:
            arguments = Arguments(positional=(pa.scalar(0.5),))
            value = Arg[float](0, gt=0.0)

        obj = MyClass()
        assert obj.value == 0.5

    def test_gt_validation_fail(self) -> None:
        """Arg gt validation should fail when value <= threshold."""

        class MyClass:
            arguments = Arguments(positional=(pa.scalar(0.0),))
            value = Arg[float](0, gt=0.0)

        obj = MyClass()
        with pytest.raises(ArgumentValidationError, match="must be > 0.0"):
            _ = obj.value

    def test_lt_validation_pass(self) -> None:
        """Arg lt validation should pass when value < threshold."""

        class MyClass:
            arguments = Arguments(positional=(pa.scalar(0.5),))
            value = Arg[float](0, lt=1.0)

        obj = MyClass()
        assert obj.value == 0.5

    def test_lt_validation_fail(self) -> None:
        """Arg lt validation should fail when value >= threshold."""

        class MyClass:
            arguments = Arguments(positional=(pa.scalar(1.0),))
            value = Arg[float](0, lt=1.0)

        obj = MyClass()
        with pytest.raises(ArgumentValidationError, match="must be < 1.0"):
            _ = obj.value

    def test_range_validation(self) -> None:
        """Arg should support combined ge and le for range validation."""

        class MyClass:
            arguments = Arguments(positional=(pa.scalar(50),))
            value = Arg[int](0, ge=1, le=100)

        obj = MyClass()
        assert obj.value == 50

    def test_choices_validation_pass(self) -> None:
        """Arg choices validation should pass when value in choices."""

        class MyClass:
            arguments = Arguments(positional=(pa.scalar("fast"),))
            mode = Arg[str](0, choices=["fast", "slow", "auto"])

        obj = MyClass()
        assert obj.mode == "fast"

    def test_choices_validation_fail(self) -> None:
        """Arg choices validation should fail when value not in choices."""

        class MyClass:
            arguments = Arguments(positional=(pa.scalar("invalid"),))
            mode = Arg[str](0, choices=["fast", "slow", "auto"])

        obj = MyClass()
        with pytest.raises(
            ArgumentValidationError, match="must be one of: 'fast', 'slow', 'auto'"
        ):
            _ = obj.mode

    def test_pattern_validation_pass(self) -> None:
        """Arg pattern validation should pass when value matches pattern."""

        class MyClass:
            arguments = Arguments(positional=(pa.scalar("my_variable"),))
            name = Arg[str](0, pattern=r"^[a-z_][a-z0-9_]*$")

        obj = MyClass()
        assert obj.name == "my_variable"

    def test_pattern_validation_fail(self) -> None:
        """Arg pattern validation should fail when value doesn't match."""

        class MyClass:
            arguments = Arguments(positional=(pa.scalar("123invalid"),))
            name = Arg[str](0, pattern=r"^[a-z_][a-z0-9_]*$")

        obj = MyClass()
        with pytest.raises(ArgumentValidationError, match="does not match pattern"):
            _ = obj.name

    def test_pattern_validation_requires_string(self) -> None:
        """Arg pattern validation should fail for non-string types."""

        class MyClass:
            arguments = Arguments(positional=(pa.scalar(123),))
            value = Arg[int](0, pattern=r".*")

        obj = MyClass()
        with pytest.raises(ArgumentValidationError, match="requires string"):
            _ = obj.value

    def test_conflicting_ge_gt_raises(self) -> None:
        """Arg should raise when both ge and gt are specified."""
        with pytest.raises(ValueError, match="Cannot specify both 'ge' and 'gt'"):
            Arg[int](0, ge=1, gt=0)

    def test_conflicting_le_lt_raises(self) -> None:
        """Arg should raise when both le and lt are specified."""
        with pytest.raises(ValueError, match="Cannot specify both 'le' and 'lt'"):
            Arg[int](0, le=10, lt=5)

    def test_validation_with_default(self) -> None:
        """Default values should also be validated."""

        class MyClass:
            arguments = Arguments(positional=())
            value = Arg[int](0, default=50, ge=1, le=100)

        obj = MyClass()
        assert obj.value == 50

    def test_repr_with_validation(self) -> None:
        """Arg repr should include validation parameters."""
        arg = Arg[int](0, ge=1, le=100, choices=[1, 2, 3], pattern=".*")
        repr_str = repr(arg)

        assert "ge=1" in repr_str
        assert "le=100" in repr_str
        assert "choices=" in repr_str
        assert "pattern=" in repr_str
