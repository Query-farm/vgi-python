# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Emit TypeScript's nested-binary conformance test (vgi docs/large-values-3.4.md §3.4).

The test reads the corpus `vgi.codegen.nested_binary_fixtures` writes to
``conformance/nested-binary/`` and checks vgi-typescript's generated
``Nested<T>`` wrapper and the codecs of the six requests embedding
``bind_call`` (``src/generated/vgi-protocol-types.ts``, from
`vgi.codegen.ts_types`):

1. byte identity: each embedding fixture decodes, its ``bind_call`` decodes to
   ``expected.json``, and re-encoding (after access and without access) writes
   exactly ``bind_call.bin``;
2. typed construction: a ``BindRequest`` built from fields, wrapped, embedded
   in an ``InitRequest``, encoded and decoded, has the same fields, and the
   same wrapper encodes to the same bytes twice;
3. ``bind-arguments-null.arrow`` decodes with ``arguments`` null and the
   location set;

plus the wrapper rules a generated decoder alone can check (errors at access,
read-only value, printing never decodes). Refusing ``bind-both-null`` /
``bind-both-set`` with ``invalid_bind_request`` and test 4 (re-encode after
``openOpaque``, rule 11) exercise worker code, so they belong to
vgi-typescript's own suite.

.. code-block:: bash

   uv run --project ~/Development/vgi-python python scripts/regen_generated.py
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from vgi.codegen._common import provenance_comment

if TYPE_CHECKING:
    from typing import TextIO

GENERATOR_VERSION = "1"

#: The repository this test is checked into, and its path there.
REPO = "vgi-typescript"
TARGET = "src/generated/__tests__/nested-binary.test.ts"

_BODY = """\
// Copyright 2025, 2026 Query Farm LLC - https://query.farm

import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { decodeASD, encodeASD } from "../../codec/asd.js";
import * as schemas from "../vgi-protocol-schemas.js";
import {
  Nested,
  buildBindRequest,
  buildInitRequest,
  decodeBindRequest,
  decodeInitRequest,
  decodeTableFunctionCardinalityRequest,
  decodeTableFunctionDynamicToStringRequest,
  decodeTableFunctionPlanRequest,
  decodeTableFunctionProjectionRequest,
  decodeTableFunctionStatisticsRequest,
  encodeInitRequest,
  encodeTableFunctionCardinalityRequest,
  encodeTableFunctionDynamicToStringRequest,
  encodeTableFunctionPlanRequest,
  encodeTableFunctionProjectionRequest,
  encodeTableFunctionStatisticsRequest,
  nestedBindRequest,
  type BindRequest,
} from "../vgi-protocol-types.js";

const corpus = new URL("../../../conformance/nested-binary/", import.meta.url);
const file = (name: string): Uint8Array => new Uint8Array(readFileSync(new URL(name, corpus)));
const json = (name: string): any => JSON.parse(readFileSync(new URL(name, corpus), "utf8"));

const manifest = json("manifest.json");
const expected = json("expected.json");
const bindCallBin = file("bind_call.bin");

/** A decoded value in the corpus's wire JSON form (see expected.json `value_encoding`). */
function wire(value: unknown): unknown {
  if (value === null || value === undefined) return null;
  if (value instanceof Uint8Array) return { hex: Buffer.from(value).toString("hex") };
  if (typeof value === "bigint") return Number(value);
  if (Array.isArray(value)) return value.map(wire);
  if (typeof value === "object") {
    return Object.fromEntries(Object.entries(value as object).map(([k, v]) => [k, wire(v)]));
  }
  return value;
}

type Codec = { decode: (b: Uint8Array) => any; encode: (v: any) => Uint8Array; schema: any };
const codecs: Record<string, Codec> = {
  InitRequest: { decode: decodeInitRequest, encode: encodeInitRequest, schema: schemas.InitRequestSchema },
  TableFunctionCardinalityRequest: {
    decode: decodeTableFunctionCardinalityRequest,
    encode: encodeTableFunctionCardinalityRequest,
    schema: schemas.TableFunctionCardinalityRequestSchema,
  },
  TableFunctionStatisticsRequest: {
    decode: decodeTableFunctionStatisticsRequest,
    encode: encodeTableFunctionStatisticsRequest,
    schema: schemas.TableFunctionStatisticsRequestSchema,
  },
  TableFunctionDynamicToStringRequest: {
    decode: decodeTableFunctionDynamicToStringRequest,
    encode: encodeTableFunctionDynamicToStringRequest,
    schema: schemas.TableFunctionDynamicToStringRequestSchema,
  },
  TableFunctionPlanRequest: {
    decode: decodeTableFunctionPlanRequest,
    encode: encodeTableFunctionPlanRequest,
    schema: schemas.TableFunctionPlanRequestSchema,
  },
  TableFunctionProjectionRequest: {
    decode: decodeTableFunctionProjectionRequest,
    encode: encodeTableFunctionProjectionRequest,
    schema: schemas.TableFunctionProjectionRequestSchema,
  },
};

/** The raw `bind_call` column of an encoded embedding request. */
const heldColumn = (codec: Codec, bytes: Uint8Array): Uint8Array =>
  decodeASD<Record<string, any>>(codec.schema, bytes).bind_call as Uint8Array;

describe("nested-binary corpus: byte identity (spec §3.4 test 1)", () => {
  test("the corpus covers the six embedding requests", () => {
    expect(manifest.embedding.map((e: any) => e.record).sort()).toEqual(Object.keys(codecs).sort());
  });

  for (const entry of manifest.embedding) {
    const codec = codecs[entry.record]!;

    test(`${entry.file}: access, then re-encode, writes the received bind_call`, () => {
      const request = codec.decode(file(entry.file));
      expect(request.bind_call).toBeInstanceOf(Nested);
      expect(wire(request.bind_call.get())).toEqual(expected.fields);
      expect(heldColumn(codec, codec.encode(request))).toEqual(bindCallBin);
    });

    test(`${entry.file}: re-encode without access writes the received bind_call`, () => {
      const request = codec.decode(file(entry.file));
      expect(heldColumn(codec, codec.encode(request))).toEqual(bindCallBin);
    });
  }
});

describe("nested-binary corpus: typed construction (spec §3.4 test 2)", () => {
  const record: BindRequest = buildBindRequest({
    function_name: "typed_construction",
    arguments: new Uint8Array(Buffer.from(expected.fields.arguments.hex, "hex")),
    function_type: "TABLE",
    schema_path: ["main"],
    argument_names: ["a", null],
    attach_opaque_data: new Uint8Array([1, 2, 3]),
  });

  test("build, wrap, embed, encode, decode, access: fields equal", () => {
    const wrapped = nestedBindRequest(record);
    const init = buildInitRequest({ bind_call: wrapped, output_schema: new Uint8Array([9, 9]) });
    const decoded = decodeInitRequest(encodeInitRequest(init));
    expect(wire(decoded.bind_call.get())).toEqual(wire(record));
    expect(decoded.bind_call.equals(wrapped)).toBe(true);
  });

  test("the same wrapper encodes to identical bytes twice", () => {
    const init = buildInitRequest({ bind_call: nestedBindRequest(record), output_schema: new Uint8Array([9]) });
    expect(encodeInitRequest(init)).toEqual(encodeInitRequest(init));
    expect(nestedBindRequest(record).bytes).toEqual(nestedBindRequest(record).bytes);
  });
});

describe("nested-binary corpus: null arguments (spec §3.4 test 3)", () => {
  const entry = manifest.binds.find((b: any) => b.file === "bind-arguments-null.arrow");

  test("bind-arguments-null decodes with arguments null and the location set", () => {
    const bind = decodeBindRequest(file(entry.file));
    expect(bind.arguments).toBeNull();
    expect(bind.arguments_location).not.toBeNull();
    expect(wire(bind.arguments_location)).toEqual(entry.arguments_location);
    expect(wire(bind)).toEqual(entry.fields);
  });

  test("the invalid binds still decode as wire records (refusal is the worker's)", () => {
    for (const name of ["bind-both-null.arrow", "bind-both-set.arrow"]) {
      const bind = decodeBindRequest(file(name));
      expect((bind.arguments == null) === (bind.arguments_location == null)).toBe(true);
    }
  });
});

describe("Nested<T> wrapper rules (spec §3.3)", () => {
  const init = file("init-noncanonical.arrow");

  test("a malformed nested value fails at access, naming the field path, not at decode", () => {
    const raw = decodeASD<Record<string, any>>(schemas.InitRequestSchema, init);
    const garbled = encodeASD(schemas.InitRequestSchema, { ...raw, bind_call: new Uint8Array([1, 2, 3]) });
    const bad = decodeInitRequest(garbled);
    expect(() => bad.bind_call.get()).toThrow(/^InitRequest\\.bind_call: /);
    expect(JSON.stringify(bad.bind_call)).toContain("InitRequest.bind_call");
    expect(String(bad.bind_call)).toContain("3 bytes");
  });

  test("a wrapper over zero bytes is a decode error at access", () => {
    expect(() => Nested.fromBytes(new Uint8Array(0), decodeBindRequest, "X.y").get()).toThrow(/^X\\.y: /);
  });

  test("the decoded value is read-only and cannot change the held bytes", () => {
    const request = decodeInitRequest(init);
    const value = request.bind_call.get() as any;
    expect(Object.isFrozen(value)).toBe(true);
    expect(() => {
      value.function_name = "changed";
    }).toThrow();
    value.arguments.fill(0);
    expect(request.bind_call.bytes).toEqual(bindCallBin);
    expect(heldColumn(codecs.InitRequest!, encodeInitRequest(request))).toEqual(bindCallBin);
  });

  test("decoding once caches the value", () => {
    const request = decodeInitRequest(init);
    expect(request.bind_call.get()).toBe(request.bind_call.value);
  });

  test("fromBytes copies the bytes it is given", () => {
    const source = bindCallBin.slice();
    const wrapped = nestedBindRequest(source);
    source.fill(0);
    expect(wrapped.bytes).toEqual(bindCallBin);
  });
});
"""


def emit(out: TextIO) -> None:
    """Emit the test."""
    out.write(
        provenance_comment(
            generator_module="vgi.codegen.ts_nested_test",
            generator_command="python -m vgi.codegen.ts_nested_test",
            generator_version=GENERATOR_VERSION,
            regen_command_lines=["uv run --project ~/Development/vgi-python python scripts/regen_generated.py"],
            body=_BODY,
        )
    )
    out.write("\n")
    out.write(_BODY)


def main() -> None:
    """Console-script entrypoint: write the test to stdout."""
    emit(sys.stdout)


if __name__ == "__main__":
    main()
