# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Emit Java's nested-binary conformance test (vgi docs/large-values-3.4.md §3.4).

A JUnit 5 class for vgi-java that runs the spec's generator conformance over the
checked-in corpus ``conformance/nested-binary/`` (vgi.codegen.nested_binary_fixtures):

1. **Byte identity:** decode each ``*-noncanonical.arrow`` as its generated
   record; re-encode without touching ``bind_call`` and compare the re-encoded
   ``bind_call`` column with ``bind_call.bin``; then access it, compare its fields
   with ``expected.json``, and re-encode again; same bytes.
2. **Typed construction:** build a ``BindRequest`` from typed fields, wrap it
   with ``Nested.of``, embed it in an ``InitRequest``, encode, decode, access:
   fields equal; encoding twice gives identical bytes.
3. **Null arguments:** ``bind-arguments-null.arrow`` decodes with ``arguments``
   null (asserted explicitly: ``byte[]`` accepts null silently) and the location
   set as the manifest says.

Test 3's worker refusal of ``bind-both-*`` and test 4 (re-encode after opening
opaque data) exercise vgi-java's request handling, not the generated types, and
belong to vgi-java's own suite.

The constructor calls in test 2 are generated from the records' ``ARROW_SCHEMA``
through `vgi.codegen.java_types`, so the test tracks the generated records.
"""

from __future__ import annotations

import io
import sys
from typing import TYPE_CHECKING

from vgi.codegen import java_types
from vgi.codegen._common import provenance_comment
from vgi.protocol import BindRequest, InitRequest

if TYPE_CHECKING:
    from typing import TextIO

GENERATOR_VERSION = "1"

#: The repository this test is checked into, and its path there.
REPO = "vgi-java"
TARGET = "vgi/src/test/java/farm/query/vgi/generated/NestedBinaryConformanceTest.java"

#: Values for the typed-construction records; every other component gets null
#: (nullable) or its type's empty value.
_BIND_VALUES = {
    "function_name": '"typed_construction"',
    "arguments": "new byte[] {1, 2, 3}",
    "function_type": '"TABLE"',
    "attach_opaque_data": "new byte[] {0, (byte) 0xff}",
    "resolved_secrets_provided": "true",
    "at_unit": '"TIMESTAMP"',
    "copy_to": 'new CopyToContext("parquet", "out.parquet")',
    "schema_path": 'List.of("main", "typed")',
    "argument_names": 'Arrays.asList("a", null)',
}
_INIT_VALUES = {
    "bind_call": "wrapped",
    "output_schema": "new byte[] {9, 8, 7}",
    "row_limit": "5L",
    "phase": '"INPUT"',
}

_EMPTY = {
    "long": "0L",
    "boolean": "false",
    "double": "0.0",
    "String": '""',
    "byte[]": "new byte[0]",
}


def _arguments(cls: type, values: dict[str, str]) -> str:
    """The positional constructor arguments of the generated record for *cls*."""
    schema = cls.ARROW_SCHEMA  # type: ignore[attr-defined]
    hints = java_types._hints(cls)
    out = []
    for field in schema:
        component = java_types._component(field, cls.__name__, hints.get(field.name))
        if field.name in values:
            out.append(values[field.name])
        elif "@Nullable" in component.annotations:
            out.append("null")
        elif component.java_type.startswith("List<"):
            out.append("List.of()")
        elif component.java_type.startswith("Map<"):
            out.append("Map.of()")
        else:
            out.append(_EMPTY[component.java_type])
    return ",\n                ".join(out)


_TEMPLATE = """\
// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.generated;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.CopyToContext;
import farm.query.vgi.protocol.InitRequest;
import farm.query.vgi.protocol.Nested;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import java.io.ByteArrayInputStream;
import java.lang.reflect.RecordComponent;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The nested-binary conformance of the generated records (vgi large-values spec
 * §3.4 tests 1-3), over {@code conformance/nested-binary/}: a {@code bind_call}
 * re-encodes as exactly the bytes it was decoded from.
 */
class NestedBinaryConformanceTest {

    private static final Path CORPUS = Path.of("../conformance/nested-binary");
    private static final HexFormat HEX = HexFormat.of();

    private static byte[] file(String name) throws Exception {
        return Files.readAllBytes(CORPUS.resolve(name));
    }

    private static JsonObject json(String name) throws Exception {
        return JsonParser.parseString(Files.readString(CORPUS.resolve(name))).getAsJsonObject();
    }

    static Stream<String> embeddingFixtures() throws Exception {
        List<String> names = new ArrayList<>();
        for (JsonElement e : json("manifest.json").getAsJsonArray("embedding")) {
            names.add(e.getAsJsonObject().get("file").getAsString());
        }
        return names.stream();
    }

    private static JsonObject embeddingEntry(String file) throws Exception {
        for (JsonElement e : json("manifest.json").getAsJsonArray("embedding")) {
            if (e.getAsJsonObject().get("file").getAsString().equals(file)) return e.getAsJsonObject();
        }
        throw new AssertionError("no manifest entry for " + file);
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends ArrowSerializableRecord> record(String name) throws Exception {
        return (Class<? extends ArrowSerializableRecord>) Class.forName("farm.query.vgi.protocol." + name);
    }

    private static Object component(Object record, String name) throws Exception {
        for (RecordComponent rc : record.getClass().getRecordComponents()) {
            if (rc.getName().equals(name)) return rc.getAccessor().invoke(record);
        }
        throw new AssertionError(record.getClass().getSimpleName() + " has no component " + name);
    }

    /** The raw value of a binary column in a one-row IPC stream, read with Arrow directly. */
    private static byte[] column(byte[] ipc, String name) throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            assertTrue(reader.loadNextBatch(), "no batch");
            VarBinaryVector vector = (VarBinaryVector) reader.getVectorSchemaRoot().getVector(name);
            return vector.getObject(0);
        }
    }

    /** Compare one decoded value with its corpus wire value (see expected.json's value_encoding). */
    private static void assertWire(String path, JsonElement expected, Object actual) throws Exception {
        if (expected == null || expected.isJsonNull()) {
            assertNull(actual, path);
        } else if (expected.isJsonObject() && expected.getAsJsonObject().has("hex")
                && expected.getAsJsonObject().size() == 1) {
            assertInstanceOf(byte[].class, actual, path);
            assertEquals(expected.getAsJsonObject().get("hex").getAsString(), HEX.formatHex((byte[]) actual), path);
        } else if (expected.isJsonObject()) {
            assertNotNull(actual, path);
            for (Map.Entry<String, JsonElement> e : expected.getAsJsonObject().entrySet()) {
                assertWire(path + "." + e.getKey(), e.getValue(), component(actual, e.getKey()));
            }
        } else if (expected.isJsonArray()) {
            JsonArray items = expected.getAsJsonArray();
            List<?> list = assertInstanceOf(List.class, actual, path);
            assertEquals(items.size(), list.size(), path);
            for (int i = 0; i < items.size(); i++) assertWire(path + "[" + i + "]", items.get(i), list.get(i));
        } else {
            JsonPrimitive p = expected.getAsJsonPrimitive();
            if (p.isBoolean()) {
                assertEquals(p.getAsBoolean(), actual, path);
            } else if (p.isNumber()) {
                assertEquals(p.getAsLong(), ((Number) actual).longValue(), path);
            } else {
                assertEquals(p.getAsString(), actual instanceof Enum<?> e ? e.name() : actual, path);
            }
        }
    }

    private static void assertFields(String path, JsonObject expected, Object record) throws Exception {
        for (Map.Entry<String, JsonElement> e : expected.entrySet()) {
            assertWire(path + "." + e.getKey(), e.getValue(), component(record, e.getKey()));
        }
    }

    /** Component-wise equality of two records, comparing arrays by content. */
    private static boolean same(Object a, Object b) throws Exception {
        if (a == null || b == null) return a == b;
        if (a instanceof byte[] x && b instanceof byte[] y) return Arrays.equals(x, y);
        if (a instanceof List<?> x && b instanceof List<?> y) {
            if (x.size() != y.size()) return false;
            for (int i = 0; i < x.size(); i++) if (!same(x.get(i), y.get(i))) return false;
            return true;
        }
        if (a.getClass().isRecord() && a.getClass() == b.getClass()) {
            for (RecordComponent rc : a.getClass().getRecordComponents()) {
                if (!same(rc.getAccessor().invoke(a), rc.getAccessor().invoke(b))) return false;
            }
            return true;
        }
        return Objects.equals(a, b);
    }

    // -- 1. byte identity ---------------------------------------------------

    @ParameterizedTest
    @MethodSource("embeddingFixtures")
    void reEncodesTheReceivedBindCallWithoutAccess(String fixture) throws Exception {
        JsonObject entry = embeddingEntry(fixture);
        byte[] expected = file(entry.get("field_bytes").getAsString());
        Object request = RecordCodec.deserializeFromBytes(file(fixture), record(entry.get("record").getAsString()));
        Nested<?> wrapper = assertInstanceOf(Nested.class, component(request, "bind_call"));
        assertArrayEquals(expected, wrapper.bytes());
        byte[] reEncoded = RecordCodec.serializeToBytes((ArrowSerializableRecord) request);
        assertArrayEquals(expected, column(reEncoded, "bind_call"), fixture + ": re-encoded bind_call");
    }

    @ParameterizedTest
    @MethodSource("embeddingFixtures")
    void reEncodesTheReceivedBindCallAfterAccess(String fixture) throws Exception {
        JsonObject entry = embeddingEntry(fixture);
        byte[] expected = file(entry.get("field_bytes").getAsString());
        JsonObject fields = json(entry.get("field_expected").getAsString()).getAsJsonObject("fields");
        Object request = RecordCodec.deserializeFromBytes(file(fixture), record(entry.get("record").getAsString()));
        Nested<?> wrapper = (Nested<?>) component(request, "bind_call");
        BindRequest bind = assertInstanceOf(BindRequest.class, wrapper.get());
        assertFields(entry.get("record").getAsString() + ".bind_call", fields, bind);
        assertTrue(wrapper.get() == bind, "the decoded value is cached");
        byte[] reEncoded = RecordCodec.serializeToBytes((ArrowSerializableRecord) request);
        assertArrayEquals(expected, column(reEncoded, "bind_call"), fixture + ": re-encoded bind_call");
        // Re-serializing the typed value would not reproduce the received bytes;
        // that is what makes this corpus a test of raw preservation.
        assertFalse(Arrays.equals(expected, RecordCodec.serializeToBytes(bind)));
    }

    @Test
    void wrapperEqualityAndPrintingUseTheHeldBytes() throws Exception {
        byte[] bytes = file("bind_call.bin");
        Nested<BindRequest> a = Nested.fromBytes(bytes, BindRequest.class);
        bytes[0] ^= 1;  // fromBytes copied: the caller's array is not the held one
        Nested<BindRequest> b = Nested.fromBytes(file("bind_call.bin"), BindRequest.class);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertEquals("Nested<BindRequest>[" + a.size() + " bytes]", a.toString());
        a.bytes()[0] ^= 1;  // bytes() is a copy
        assertArrayEquals(file("bind_call.bin"), a.bytes());
    }

    @Test
    void decodeErrorsSurfaceAtAccessNamingTheField() throws Exception {
        InitRequest init = RecordCodec.deserializeFromBytes(file("init-noncanonical.arrow"), InitRequest.class);
        InitRequest broken = RecordCodec.deserializeFromBytes(
                RecordCodec.serializeToBytes(withBindCall(init, new byte[] {1, 2, 3})), InitRequest.class);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> broken.bind_call().get());
        assertTrue(e.getMessage().startsWith("InitRequest.bind_call: "), e.getMessage());
        InitRequest empty = RecordCodec.deserializeFromBytes(
                RecordCodec.serializeToBytes(withBindCall(init, new byte[0])), InitRequest.class);
        assertThrows(IllegalArgumentException.class, () -> empty.bind_call().get());
    }

    private static InitRequest withBindCall(InitRequest init, byte[] bytes) {
        return new InitRequest(
                Nested.fromBytes(bytes, BindRequest.class),
                init.output_schema(), init.bind_opaque_data(), init.projection_ids(), init.pushdown_filters(),
                init.join_keys(), init.split_tokens(), init.row_limit(), init.phase(), init.finalize_state_id(),
                init.execution_id(), init.init_opaque_data(), init.substream_id(), init.order_by_column_name(),
                init.order_by_direction(), init.order_by_null_order(), init.order_by_limit(),
                init.tablesample_percentage(), init.tablesample_seed());
    }

    // -- 2. typed construction ----------------------------------------------

    @Test
    void typedConstructionRoundTrips() throws Exception {
        BindRequest bind = new BindRequest(
                @@BIND@@);
        Nested<BindRequest> wrapped = Nested.of(bind);
        InitRequest init = new InitRequest(
                @@INIT@@);
        byte[] encoded = RecordCodec.serializeToBytes(init);
        assertArrayEquals(encoded, RecordCodec.serializeToBytes(init), "the same wrapper encodes identically");
        InitRequest decoded = RecordCodec.deserializeFromBytes(encoded, InitRequest.class);
        assertArrayEquals(wrapped.bytes(), column(encoded, "bind_call"));
        assertEquals(wrapped, decoded.bind_call());
        assertTrue(same(bind, decoded.bind_call().get()), "decoded fields equal the typed record");
        assertTrue(same(bind, wrapped.get()), "Nested.of behaves like a wrapper decoded from its bytes");
        assertNotEquals(wrapped, Nested.of(new BindRequest(
                @@BIND_OTHER@@)));
    }

    // -- 3. null arguments --------------------------------------------------

    @Test
    void nullArgumentsDecodeWithTheLocationSet() throws Exception {
        JsonObject entry = null;
        for (JsonElement e : json("manifest.json").getAsJsonArray("binds")) {
            if (e.getAsJsonObject().get("file").getAsString().equals("bind-arguments-null.arrow")) {
                entry = e.getAsJsonObject();
            }
        }
        assertNotNull(entry);
        BindRequest bind = RecordCodec.deserializeFromBytes(file("bind-arguments-null.arrow"), BindRequest.class);
        assertNull(bind.arguments(), "arguments must decode as null, not an empty array");
        assertNotNull(bind.arguments_location());
        assertWire("BindRequest.arguments_location", entry.get("arguments_location"), bind.arguments_location());
        assertFields("BindRequest", entry.getAsJsonObject("fields"), bind);
    }

    @Test
    void invalidCombinationsStillDecodeAsTypes() throws Exception {
        BindRequest bothNull = RecordCodec.deserializeFromBytes(file("bind-both-null.arrow"), BindRequest.class);
        assertNull(bothNull.arguments());
        assertNull(bothNull.arguments_location());
        BindRequest bothSet = RecordCodec.deserializeFromBytes(file("bind-both-set.arrow"), BindRequest.class);
        assertNotNull(bothSet.arguments());
        assertNotNull(bothSet.arguments_location());
    }
}
"""


def _body() -> str:
    bind = _arguments(BindRequest, _BIND_VALUES)
    other = _arguments(BindRequest, {**_BIND_VALUES, "function_name": '"typed_construction_other"'})
    init = _arguments(InitRequest, _INIT_VALUES)
    return _TEMPLATE.replace("@@BIND@@", bind).replace("@@BIND_OTHER@@", other).replace("@@INIT@@", init)


def emit(out: TextIO) -> None:
    """Emit the test."""
    body = _body()
    out.write(
        provenance_comment(
            generator_module="vgi.codegen.java_nested_test",
            generator_command="python -m vgi.codegen.java_nested_test",
            generator_version=GENERATOR_VERSION,
            regen_command_lines=["uv run --project ~/Development/vgi-python python scripts/regen_generated.py"],
            body=body,
        )
    )
    out.write("\n")
    out.write(body)


def render() -> str:
    """The full text of the generated test."""
    buf = io.StringIO()
    emit(buf)
    return buf.getvalue()


def main() -> None:
    """Console-script entrypoint — write the test to stdout."""
    emit(sys.stdout)


if __name__ == "__main__":
    main()
