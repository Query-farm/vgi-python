# Copyright 2026 Query Farm LLC - https://query.farm
"""Nanoarrow rendering for the shared native protocol-record model.

Only runtime syntax differs; types, fields, presence and wire identities come
from cpp_records.build_model, shared with the Arrow C++ renderer.
"""

from __future__ import annotations

import io

from vgi.codegen._common import close_namespace, open_namespace
from vgi.codegen.cpp_records import HEADER_NAME, _dependency_order, _doc, _factory_names, _method_rows, _Model
from vgi.codegen.cpp_schemas import _na_emit_field


def _render_header(model: _Model, namespace: list[str]) -> str:
    out = io.StringIO()
    out.write("#pragma once\n\n")
    out.write("// Typed VGI protocol records. Encode/decode with ToBatch()/FromBatch()\n")
    out.write("// (vgi_protocol_records.cpp), over the nanoarrow-backed types of vgi_arrow.hpp.\n\n")
    out.write('#include "vgi_arrow.hpp"\n\n')
    for inc in ("cstdint", "map", "memory", "optional", "string", "utility", "vector"):
        out.write(f"#include <{inc}>\n")
    out.write("\n")
    out.write(open_namespace(namespace))
    out.write("\n")
    out.write(
        "using ::duckdb::vgi::VgiBatchPtr;\nusing ::duckdb::vgi::VgiBufferPtr;\nusing ::duckdb::vgi::VgiSchemaPtr;\n\n"
    )
    out.write("using Bytes = std::vector<uint8_t>;\n")
    out.write("//! A large_binary value, shared rather than copied (filter and join-key payloads can be large).\n")
    out.write("using LargeBytes = VgiBufferPtr;\n")
    out.write("//! timestamp[us, tz=UTC]: microseconds since the Unix epoch.\n")
    out.write("using TimestampMicros = int64_t;\n\n")

    out.write("// " + "-" * 76 + "\n// Enums. Enumerator order is the wire dictionary order.\n// " + "-" * 76 + "\n\n")
    for e in model.enums.values():
        out.write("\n".join(_doc(e.doc, "")) + ("\n" if e.doc else ""))
        out.write(f"enum class {e.name} : int16_t {{\n")
        for em in e.members:
            out.write(f"\t{em.ident},\n")
        out.write("};\n")
        out.write(f"//! The wire name of a {e.name} value.\n")
        out.write(f"const char *ToWire({e.name} value);\n")
        out.write(f"//! The {e.name} with this wire name, or std::nullopt.\n")
        out.write(f"std::optional<{e.name}> {e.name}FromWire(const std::string &wire);\n\n")

    out.write("// " + "-" * 76 + "\n// Records.\n// " + "-" * 76 + "\n\n")
    # Forward-declare so a record can hold one declared later.
    for r in model.records.values():
        out.write(f"struct {r.name};\n")
    out.write("\n")
    for r in _dependency_order(model):
        out.write("\n".join(_doc(r.doc, "")) + ("\n" if r.doc else ""))
        out.write(f"//! Protocol type: {', '.join(sorted(set(r.origins)))}.\n")
        out.write(f"struct {r.name} {{\n")
        for m in r.members:
            for line in _doc(m.doc, "\t"):
                out.write(line + "\n")
            init = f" = {m.initializer}" if m.initializer else "{}"
            out.write(f"\t{m.declared_type} {m.ident}{init};\n")
        out.write("};\n")
        out.write(f"bool operator==(const {r.name} &a, const {r.name} &b);\n")
        out.write(f"inline bool operator!=(const {r.name} &a, const {r.name} &b) {{\n\treturn !(a == b);\n}}\n")
        out.write(f"//! A one-row batch with {r.name}'s exact protocol schema.\n")
        out.write(f"VgiBatchPtr ToBatch(const {r.name} &value);\n")
        out.write(
            f"//! Decode row `row` of `batch` (protocol errors name `worker_path`).\n"
            f"void FromBatch(const VgiBatchPtr &batch, int64_t row, {r.name} &out, const std::string &worker_path);\n\n"
        )

    out.write("// " + "-" * 76 + "\n// Methods.\n// " + "-" * 76 + "\n\n")
    out.write("//! One VgiProtocol method's wire schemas (vgi_protocol_schemas.hpp factories).\n")
    out.write("struct MethodSchemas {\n")
    out.write("\tconst char *method;\n")
    out.write("\t//! The outer request batch (`<Method>ParamsSchema`).\n")
    out.write("\tVgiSchemaPtr params;\n")
    out.write("\t//! The record a unary result carries (`<Method>ResultSchema`); null when the method\n")
    out.write("\t//! returns nothing, raw bytes, or is a stream.\n")
    out.write("\tVgiSchemaPtr result;\n")
    out.write("\t//! False for a unary method that returns None.\n")
    out.write("\tbool returns_value;\n")
    out.write("};\n")
    out.write("//! Every VgiProtocol method, by name order.\n")
    out.write("const std::vector<MethodSchemas> &AllMethodSchemas();\n\n")
    out.write("// Test hooks (test/cpp/test_protocol_records.cpp).\n")
    out.write("//! Every record's name with a one-row batch of a default-constructed instance.\n")
    out.write("std::vector<std::pair<std::string, VgiBatchPtr>> DefaultRecordBatchesForTesting();\n")
    out.write("//! Records that have a vgi_protocol_schemas.hpp factory: name, record schema, factory schema.\n")
    out.write("struct RecordSchemaPair {\n\tstd::string name;\n\tVgiSchemaPtr record;\n")
    out.write("\tVgiSchemaPtr factory;\n};\n")
    out.write("std::vector<RecordSchemaPair> RecordSchemaPairsForTesting();\n\n")
    out.write("//! Decode row `row` of `batch` as a T (any record above).\n")
    out.write("template <class T>\n")
    out.write("T Decode(const VgiBatchPtr &batch, int64_t row, const std::string &worker_path) {\n")
    out.write("\tT out;\n\tFromBatch(batch, row, out, worker_path);\n\treturn out;\n}\n\n")
    out.write(close_namespace(namespace))
    return out.getvalue()


def _render_source(model: _Model, namespace: list[str]) -> str:
    out = io.StringIO()
    out.write(f'#include "{HEADER_NAME}"\n\n')
    out.write('#include "vgi_protocol_schemas.hpp"\n')
    out.write('#include "vgi_secret_protocol_schemas.hpp"\n')
    out.write('#include "vgi_wire_codec.hpp"\n\n')
    out.write(open_namespace(namespace))
    out.write("\nnamespace wire = ::duckdb::vgi::wire;\n")
    out.write(
        "using ::duckdb::vgi::MakeSchema;\nusing ::duckdb::vgi::VgiBatchBuilder;\nusing ::duckdb::vgi::VgiColumn;\n"
    )
    out.write("namespace type = ::duckdb::vgi::type;\n")
    out.write("// The codec's overloads join the generated ones (records and enums find each other by ADL).\n")
    out.write("using wire::ReadValue;\nusing wire::WriteValue;\n\n")

    # Enums.
    for e in model.enums.values():
        wires = ", ".join(f'"{m.wire_name}"' for m in e.members)
        out.write(f"static const char *const k{e.name}Wire[] = {{{wires}}};\n")
        out.write(f"static constexpr int16_t k{e.name}Count = {len(e.members)};\n\n")
        out.write(f"const char *ToWire({e.name} value) {{\n")
        out.write("\tauto i = static_cast<int16_t>(value);\n")
        out.write(f'\treturn i >= 0 && i < k{e.name}Count ? k{e.name}Wire[i] : "";\n}}\n\n')
        out.write(f"std::optional<{e.name}> {e.name}FromWire(const std::string &wire) {{\n")
        out.write(f"\tfor (int16_t i = 0; i < k{e.name}Count; i++) {{\n")
        out.write(f"\t\tif (wire == k{e.name}Wire[i]) {{\n\t\t\treturn static_cast<{e.name}>(i);\n\t\t}}\n\t}}\n")
        out.write("\treturn std::nullopt;\n}\n\n")
        out.write(f"void ReadValue(const VgiColumn &col, int64_t i, {e.name} &out, const wire::Ctx &ctx) {{\n")
        out.write("\tstd::string s;\n\twire::ReadValue(col, i, s, ctx);\n")
        out.write(f"\tauto v = {e.name}FromWire(s);\n")
        out.write(f'\tif (!v) {{\n\t\twire::ThrowBadEnum(ctx, "{e.name}", s);\n\t}}\n')
        out.write("\tout = *v;\n}\n\n")
        out.write(f"void WriteValue(ArrowArray *array, {e.name} value) {{\n")
        out.write(f"\twire::WriteEnum(array, static_cast<int16_t>(value), k{e.name}Wire, k{e.name}Count);\n}}\n\n")

    # Records: schema factories, nested-struct codecs (declared first so any order works), batch codecs.
    out.write("// Nested-struct codecs, declared up front so records can nest in any order.\n")
    for r in model.records.values():
        out.write(f"void ReadValue(const VgiColumn &col, int64_t i, {r.name} &out, const wire::Ctx &ctx);\n")
        out.write(f"void WriteValue(ArrowArray *array, const {r.name} &value);\n")
    out.write("\n")

    for r in model.records.values():
        fields = ",\n\t    ".join(_na_emit_field(f, origin=f"{r.name}.{f.name}") for f in r.schema)
        out.write(f"static const VgiSchemaPtr &{r.name}RecordSchema() {{\n")
        out.write(
            f"\tstatic const auto schema = MakeSchema({{\n\t    {fields},\n\t}});\n"
            if r.members
            else "\tstatic const auto schema = MakeSchema({});\n"
        )
        out.write("\treturn schema;\n}\n\n")

        out.write(f"bool operator==(const {r.name} &a, const {r.name} &b) {{\n")
        if r.members:
            conds = " &&\n\t       ".join(f"wire::Equal(a.{m.ident}, b.{m.ident})" for m in r.members)
            out.write(f"\treturn {conds};\n}}\n\n")
        else:
            out.write("\t(void)a;\n\t(void)b;\n\treturn true;\n}\n\n")

        out.write(f"template <class Reader>\nstatic void Read{r.name}(Reader &r, {r.name} &out) {{\n")
        if not r.members:
            out.write("\t(void)r;\n\t(void)out;\n")
        for m in r.members:
            out.write(f'\tr.{m.presence.value}("{m.wire_name}", out.{m.ident});\n')
        out.write("}\n\n")

        out.write(f"void ReadValue(const VgiColumn &col, int64_t i, {r.name} &out, const wire::Ctx &ctx) {{\n")
        out.write(f'\twire::StructReader r(col, i, ctx, "{r.name}");\n\tRead{r.name}(r, out);\n}}\n\n')
        out.write(f"void WriteValue(ArrowArray *array, const {r.name} &value) {{\n")
        if not r.members:
            out.write("\t(void)value;\n")
        for idx, m in enumerate(r.members):
            out.write(f"\tWriteValue(array->children[{idx}], value.{m.ident});\n")
        out.write("\t::duckdb::vgi::append::FinishElement(array);\n}\n\n")

        out.write(f"VgiBatchPtr ToBatch(const {r.name} &value) {{\n")
        out.write(f"\tVgiBatchBuilder b({r.name}RecordSchema());\n")
        if not r.members:
            out.write("\t(void)value;\n")
        for idx, m in enumerate(r.members):
            out.write(f"\tWriteValue(b.column({idx}), value.{m.ident});\n")
        out.write("\tb.FinishRow();\n\treturn b.Finish();\n}\n\n")
        out.write(
            f"void FromBatch(const VgiBatchPtr &batch, int64_t row, {r.name} &out, const std::string &worker_path) {{\n"
        )
        out.write(f'\twire::RowReader r(batch, row, "{r.name}", worker_path);\n')
        out.write("\twire::CurrentBatchScope scope(batch.get());\n")
        out.write(f"\tRead{r.name}(r, out);\n}}\n\n")

    out.write("const std::vector<MethodSchemas> &AllMethodSchemas() {\n")
    out.write("\tstatic const std::vector<MethodSchemas> methods = {\n")
    for method, params, result, returns in _method_rows():
        result_expr = f"{result}()" if result else "nullptr"
        out.write(f'\t    {{"{method}", {params}(), {result_expr}, {"true" if returns else "false"}}},\n')
    out.write("\t};\n\treturn methods;\n}\n\n")

    factories = _factory_names()
    out.write("std::vector<std::pair<std::string, VgiBatchPtr>> DefaultRecordBatchesForTesting() {\n")
    out.write("\treturn {\n")
    for r in model.records.values():
        out.write(f'\t    {{"{r.name}", ToBatch({r.name} {{}})}},\n')
    out.write("\t};\n}\n\n")
    out.write("std::vector<RecordSchemaPair> RecordSchemaPairsForTesting() {\n\treturn {\n")
    for r in model.records.values():
        if r.name in factories:
            out.write(f'\t    {{"{r.name}", {r.name}RecordSchema(), {r.name}Schema()}},\n')
    out.write("\t};\n}\n\n")
    out.write(close_namespace(namespace))
    return out.getvalue()
