# Copyright 2025, 2026 Query Farm LLC - https://query.farm

# ruff: noqa: E501 - the C# test body is a template; its lines follow C# layout, not Python's.

"""Emit C#'s nested-binary conformance test (vgi docs/large-values-3.4.md §3.4).

An xUnit test class for vgi-csharp's test project that reads the corpus
`vgi.codegen.nested_binary_fixtures` checks into ``conformance/nested-binary/``
and proves the generated ``Nested<T>`` wrapper (`vgi.codegen.csharp_types`):

1. byte identity: every embedding fixture decodes, its ``bind_call`` decodes to
   ``expected.json``, and re-encoding writes ``bind_call.bin`` byte for byte,
   both after and without accessing the value;
2. typed construction: ``BindRequest`` -> ``Nested.Of`` -> ``InitRequest`` ->
   encode -> decode -> access gives the same fields, and encoding twice gives
   identical bytes;
3. null arguments: ``bind-arguments-null.arrow`` decodes with ``Arguments``
   null and the location set. The worker's refusal of ``bind-both-null`` /
   ``bind-both-set`` (``invalid_bind_request``) is runtime behaviour outside the
   generated types, as is test 4 (opaque opening, ``OpaqueSealing``).

Plus the wrapper's own rules: lazy decode errors name the field path, one
decode under concurrent access, equality over bytes, no decoding in
``ToString``.

The embedding requests and their nested fields come from the records
themselves (`vgi.codegen._common.nested_binary_record`), and the fixture files
from the corpus manifest, so a new embedding request needs no edit here.
"""

from __future__ import annotations

import io
import json
import sys
from typing import TYPE_CHECKING

from vgi.codegen import nested_binary_fixtures
from vgi.codegen._common import SHARED_CONTRACT_TYPES, nested_binary_record, provenance_comment
from vgi.codegen.csharp_types import _hints, _pascal, csharp_name

if TYPE_CHECKING:
    from typing import TextIO

GENERATOR_VERSION = "1"

#: The repository this test is checked into, and its path there.
REPO = "vgi-csharp"
TARGET = "test/QueryFarm.Vgi.Tests/Generated/NestedBinaryConformanceTests.g.cs"


def embedding_fields() -> dict[str, tuple[str, str, str]]:
    """``{python record: (C# record, C# property, nested C# record)}`` for every nested-binary field."""
    out: dict[str, tuple[str, str, str]] = {}
    for cls in SHARED_CONTRACT_TYPES:
        hints = _hints(cls)
        for f in cls.ARROW_SCHEMA:  # type: ignore[attr-defined]
            nested = nested_binary_record(f.type, hints.get(f.name))
            if nested is not None:
                if cls.__name__ in out:
                    raise ValueError(f"{cls.__name__} has two nested-binary fields; extend csharp_nested_test")
                out[cls.__name__] = (csharp_name(cls.__name__), _pascal(f.name), csharp_name(nested.__name__))
    return out


_BODY = """\
using System.Collections;
using System.Reflection;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Attributes;
using Xunit;

namespace QueryFarm.Vgi.Tests.Generated;

public class NestedBinaryConformanceTests
{
    private static readonly string Corpus = FindCorpus();

    private static string FindCorpus()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null &&
               !File.Exists(Path.Combine(directory.FullName, "conformance", "nested-binary", "manifest.json")))
            directory = directory.Parent;
        Assert.NotNull(directory);
        return Path.Combine(directory.FullName, "conformance", "nested-binary");
    }

    private static byte[] Read(string name) => File.ReadAllBytes(Path.Combine(Corpus, name));

    private static JsonElement Json(string name) => JsonDocument.Parse(File.ReadAllText(Path.Combine(Corpus, name))).RootElement;

    private static readonly JsonElement Manifest = Json("manifest.json");

    private static readonly JsonElement ExpectedBind = Json("expected.json").GetProperty("fields");

    public static TheoryData<string> EmbeddingFixtures()
    {
        var data = new TheoryData<string>();
        foreach (var entry in Manifest.GetProperty("embedding").EnumerateArray()) data.Add(entry.GetProperty("file").GetString()!);
        return data;
    }

    private static JsonElement Entry(string file) =>
        Manifest.GetProperty("embedding").EnumerateArray().Single(e => e.GetProperty("file").GetString() == file);

    /// <summary>The single binary column <paramref name="name"/> of a one-row IPC stream.</summary>
    private static byte[] Column(byte[] ipc, string name)
    {
        using var reader = new ArrowStreamReader(new MemoryStream(ipc));
        using var batch = reader.ReadNextRecordBatch();
        var column = (BinaryArray)batch.Column(batch.Schema.GetFieldIndex(name));
        return column.GetBytes(0).ToArray();
    }

    private static void Check<T>(string file, string field, bool access, Func<T, Nested<BindRequest>> nested) where T : class, new()
    {
        var expected = Read(Entry(file).GetProperty("field_bytes").GetString()!);
        var request = EmbeddedIpc.Decode<T>(Read(file));
        var wrapper = nested(request);
        Assert.Equal(expected, wrapper.Bytes.ToArray());
        if (access) AssertWire(ExpectedBind, wrapper.Value, "bind_call");
        Assert.Equal(expected, Column(EmbeddedIpc.Encode(request), field));
        if (access) AssertWire(ExpectedBind, wrapper.Value, "bind_call");
    }

    private static void Dispatch(string file, bool access)
    {
        var entry = Entry(file);
        var field = entry.GetProperty("field").GetString()!;
        switch (entry.GetProperty("record").GetString())
        {
@@CASES@@
            default:
                Assert.Fail($"{file}: no generated record for {entry.GetProperty("record").GetString()}");
                break;
        }
    }

    [Theory]
    [MemberData(nameof(EmbeddingFixtures))]
    public void ByteIdentityAfterAccess(string file) => Dispatch(file, access: true);

    [Theory]
    [MemberData(nameof(EmbeddingFixtures))]
    public void ByteIdentityWithoutAccess(string file) => Dispatch(file, access: false);

    [Fact]
    public void TypedConstruction()
    {
        var received = EmbeddedIpc.Decode<InitRequest>(Read("init-noncanonical.arrow"));
        var source = received.BindCall.Value;
        var bind = new BindRequest
        {
            FunctionName = source.FunctionName,
            Arguments = source.Arguments,
            FunctionType = source.FunctionType,
            Settings = source.Settings,
            AttachOpaqueData = source.AttachOpaqueData,
            TransactionOpaqueData = source.TransactionOpaqueData,
            ResolvedSecretsProvided = source.ResolvedSecretsProvided,
            AtUnit = source.AtUnit,
            AtValue = source.AtValue,
            CopyFrom = source.CopyFrom,
            SchemaPath = source.SchemaPath,
            ArgumentNames = source.ArgumentNames,
        };
        var wrapper = Nested<BindRequest>.Of(bind);
        // Typed construction is canonical: the received bytes carried extra schema metadata.
        Assert.NotEqual(Read("bind_call.bin"), wrapper.Bytes.ToArray());
        var init = new InitRequest { BindCall = wrapper, OutputSchema = received.OutputSchema };
        var first = EmbeddedIpc.Encode(init);
        var second = EmbeddedIpc.Encode(init);
        Assert.Equal(first, second);
        Assert.Equal(wrapper.Bytes.ToArray(), Column(first, "bind_call"));
        var decoded = EmbeddedIpc.Decode<InitRequest>(first);
        Assert.Equal(wrapper, decoded.BindCall);
        AssertWire(ExpectedBind, decoded.BindCall.Value, "bind_call");
        Assert.Equal(wrapper.Bytes.ToArray(), Nested<BindRequest>.Of(bind).Bytes.ToArray());
    }

    [Fact]
    public void NullArgumentsDecode()
    {
        var entry = Manifest.GetProperty("binds").EnumerateArray().Single(e => e.GetProperty("file").GetString() == "bind-arguments-null.arrow");
        var bind = EmbeddedIpc.Decode<BindRequest>(Read("bind-arguments-null.arrow"));
        Assert.Null(bind.Arguments);
        Assert.NotNull(bind.ArgumentsLocation);
        AssertWire(entry.GetProperty("arguments_location"), bind.ArgumentsLocation, "arguments_location");
        AssertWire(entry.GetProperty("fields"), bind, "bind");
    }

    [Fact]
    public void InvalidCombinationsDecodeForTheWorkerToRefuse()
    {
        // The generated types carry the wire; refusing these with invalid_bind_request is the worker's job.
        var bothNull = EmbeddedIpc.Decode<BindRequest>(Read("bind-both-null.arrow"));
        Assert.Null(bothNull.Arguments);
        Assert.Null(bothNull.ArgumentsLocation);
        var bothSet = EmbeddedIpc.Decode<BindRequest>(Read("bind-both-set.arrow"));
        Assert.NotNull(bothSet.Arguments);
        Assert.NotNull(bothSet.ArgumentsLocation);
    }

    [Fact]
    public void MalformedNestedValueFailsAtAccessNamingTheField()
    {
        var init = new InitRequest { BindCall = Nested<BindRequest>.FromBytes([1, 2, 3]), OutputSchema = [] };
        var decoded = EmbeddedIpc.Decode<InitRequest>(EmbeddedIpc.Encode(init));
        Assert.Equal([1, 2, 3], decoded.BindCall.Bytes.ToArray());
        var error = Assert.Throws<InvalidDataException>(() => decoded.BindCall.Value);
        Assert.StartsWith("InitRequest.bind_call: ", error.Message);
        var empty = Assert.Throws<InvalidDataException>(() => new InitRequest().BindCall.Value);
        Assert.StartsWith("InitRequest.bind_call: ", empty.Message);
    }

    [Fact]
    public void WrapperDecodesOnceComparesBytesAndNeverDecodesToPrint()
    {
        var bytes = Read("bind_call.bin");
        var wrapper = Nested<BindRequest>.FromBytes(bytes);
        Assert.Equal($"Nested<BindRequest>({bytes.Length} bytes)", wrapper.ToString());
        var values = new BindRequest[16];
        Parallel.For(0, values.Length, i => values[i] = wrapper.Value);
        Assert.All(values, v => Assert.Same(values[0], v));
        Assert.Equal(wrapper, Nested<BindRequest>.FromBytes(bytes));
        Assert.Equal(wrapper.GetHashCode(), Nested<BindRequest>.FromBytes(bytes).GetHashCode());
        Assert.NotSame(wrapper.Value, wrapper.DecodeCopy());
        Assert.Equal(wrapper, wrapper.WithPath("Elsewhere.bind_call"));
        Assert.Equal(bytes, wrapper.Bytes.ToArray());
    }

    // ---- expected.json comparison (value_encoding in the corpus) ----

    private static void AssertWire(JsonElement expected, object? actual, string path)
    {
        if (expected.ValueKind == JsonValueKind.Null)
        {
            Assert.True(actual is null, $"{path}: expected null, got {actual}");
            return;
        }

        Assert.True(actual is not null, $"{path}: expected {expected}, got null");
        switch (expected.ValueKind)
        {
            case JsonValueKind.Object when expected.TryGetProperty("hex", out var hex) && actual is byte[] bytes:
                Assert.True(hex.GetString() == Convert.ToHexString(bytes).ToLowerInvariant(), $"{path}: bytes differ");
                break;
            case JsonValueKind.Object:
                foreach (var property in expected.EnumerateObject())
                {
                    var clr = actual!.GetType().GetProperty(Pascal(property.Name))
                        ?? throw new Xunit.Sdk.XunitException($"{path}: {actual.GetType().Name} has no property for '{property.Name}'");
                    AssertWire(property.Value, clr.GetValue(actual), $"{path}.{property.Name}");
                }

                break;
            case JsonValueKind.Array:
                var items = ((IEnumerable)actual!).Cast<object?>().ToList();
                var wanted = expected.EnumerateArray().ToList();
                Assert.True(wanted.Count == items.Count, $"{path}: expected {wanted.Count} items, got {items.Count}");
                for (var i = 0; i < items.Count; i++) AssertWire(wanted[i], items[i], $"{path}[{i}]");
                break;
            case JsonValueKind.String when actual is Enum member:
                Assert.Equal(expected.GetString(), WireName(member));
                break;
            case JsonValueKind.String:
                Assert.Equal(expected.GetString(), actual);
                break;
            case JsonValueKind.True or JsonValueKind.False:
                Assert.Equal(expected.GetBoolean(), actual);
                break;
            case JsonValueKind.Number:
                Assert.Equal(expected.GetInt64(), Convert.ToInt64(actual));
                break;
            default:
                Assert.Fail($"{path}: unexpected JSON {expected.ValueKind}");
                break;
        }
    }

    private static string Pascal(string snake) =>
        string.Concat(snake.Split('_', StringSplitOptions.RemoveEmptyEntries).Select(p => char.ToUpperInvariant(p[0]) + p[1..].ToLowerInvariant()));

    private static string WireName(Enum member)
    {
        var name = member.ToString();
        var field = member.GetType().GetField(name)!;
        if (field.GetCustomAttribute<RpcNameAttribute>() is { } rpc) return rpc.WireName;
        return string.Concat(name.Select((c, i) => i > 0 && char.IsUpper(c) ? "_" + c : c.ToString())).ToUpperInvariant();
    }
}
"""


def _cases() -> str:
    records = {e["record"] for e in json.loads(nested_binary_fixtures.emit_bytes(record="manifest.json"))["embedding"]}
    fields = embedding_fields()
    missing = sorted(records - set(fields))
    if missing:
        raise ValueError(f"corpus records without a generated nested-binary field: {missing}")
    lines = []
    for py in sorted(records):
        cs, prop, nested = fields[py]
        if nested != "BindRequest":
            raise ValueError(f"{py}: csharp_nested_test compares nested values against expected.json's BindRequest")
        lines.append(f'            case "{py}":')
        lines.append(f"                Check<{cs}>(file, field, access, r => r.{prop});")
        lines.append("                break;")
    return "\n".join(lines)


def emit(out: TextIO) -> None:
    """Emit the test."""
    body = io.StringIO()
    body.write("// Copyright 2025, 2026 Query Farm LLC - https://query.farm\n")
    body.write("// <auto-generated/>\n\n#nullable enable\n\n")
    body.write(_BODY.replace("@@CASES@@", _cases()))
    out.write(
        provenance_comment(
            generator_module="vgi.codegen.csharp_nested_test",
            generator_command="python -m vgi.codegen.csharp_nested_test",
            generator_version=GENERATOR_VERSION,
            regen_command_lines=["uv run --project ~/Development/vgi-python python scripts/regen_generated.py"],
            body=body.getvalue(),
        )
    )
    out.write("\n")
    out.write(body.getvalue())


def main() -> None:
    """Write the test to stdout."""
    emit(sys.stdout)


if __name__ == "__main__":
    main()
