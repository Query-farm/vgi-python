# Latest-main integration — October 24, 2026

**Subsequent fix:** the nested-NOT_NULL schema-reconciliation failure recorded
below has been resolved. Native 1.5 now validates and retains nested facts without
making nullable parents NOT NULL. The write regression passes with Python and Go
workers over subprocess and HTTP. The full native 1.5 rerun passed 347 test files,
with 32 skips and zero failures. Current regression and seven-framework evidence
are in `nested_not_null_checkpoint_2026_10_24` in the
[manifest](duckdb-2.0-manifest.json); the original integration results below remain historical.

Published main is now `vgi.v2` / `2.2.0`. The combined successor remains unpublished and
incompatible despite temporarily carrying those development labels. Its final major version,
routing name and coordinated release gates remain unresolved. The native runtime stays pinned
to Haybarn/DuckDB 1.5 commit `105edd31b57fe7e6d32ee648efad562e45a9f908`.

This is an integration checkpoint, not a protocol release, full DuckDB 2 qualification or
implementation of automatic whole-query pushdown. Earlier dated source pins, version values
and test results in the manifest remain historical evidence. The original A0 inventory is
retained separately from the refreshed diagnostic build.

## Exact merged inputs

These are merge inputs, not final merge-commit IDs. Backup ref:
`codex/pre-main-sync-20261024-kmz7l4hp`. The machine-readable copies are in
`main_integration_checkpoint_2026_10_24` in the [manifest](duckdb-2.0-manifest.json).

| Repository | Successor branch before merge | Merged main or published tag |
| --- | --- | --- |
| `vgi` | `e1dc7520d28dec8cc7414bb65e227e565b6ff560` | `454fefa50b11f8bfe3065fb11aebf8f5761d7b3d` |
| `vgi-python` | `7dc87f44bb73c723b3cac13f059af50338fdd284` | `fd4f5b97a41972475d93e627c1688b4e75712f6d` |
| `vgi-go` | `ad5f89701ef0bc20845ae95bf1cffe0f8e6cbed8` | `ca85f8cfef3eb8c17bbbb6a13f8a48bf1956670e` |
| `vgi-rust` | `2f0c343ebf010f1cbf6b37f0d084ab8a7ccca338` | `89f3cf651badf1f9caccc89f43f208fa34770e59` |
| `vgi-typescript` | `977b667f49c924f912127eae2c8957d3d0658fa6` | `412384aa56d96d19fdbc88de42a94d994b70ca88` |
| `vgi-java` | `10613b873996fb8bf755a21c80ffd0bab713ec69` | `074ec2fc13973479504bec12f07fb7a597c5343e` |
| `vgi-csharp` | `d77b0b209a8db9fecb220d7d575932d51b54106d` | `15d3cae296d9082e68ad36c058731cb7d8bde7ff` |
| `vgi-cpp` | `1da952dd328578ee9695f50e3ae719e9e57a511f` | `0c5e24522690119bc002f423130be266c8a010a5` |
| `vgi-rpc-go` | `2921723df8246e8715785ffccae4b1a0912d2550` | `688ba348b67c47125953e98ee74847efc279cd87` |
| `vgi-rpc-java` | `30c84da7fc20b38f7c59aae5d2beb5ee483be6a1` | `3a8a300a10226be1b56e0458e81ffa3212b73f1d` (`v0.33.1`) |
| `vgi-rpc-c++` | `08951ccd7ebd4661655765e733388ce371894a7a` | `25fad89ed4bc7746e2958537752a00fe0a707ecd` (`v0.11.0`) |

The unpublished C# local-main snapshot commit `ce20f46` is covered by upstream `1badcaf`
and subsequent fixes; duplicating it was unnecessary.

## Reconciled behavior

- Six catalog-wide Arrow streams preserve canonical constraints, typed index definitions,
  macro families, schema options and column metadata. `SchemaInfo` attachment state is
  constructor compatibility only. New bulk codecs retain canonical column validation.
- Python capability-projected inventory uses scoped cache/publication keys and etags.
  `SnapshotRequest.capability_projection` is internal. A validator for an unprojected
  inventory cannot short-circuit a projected request. SDKs that publish unprojected records
  retain their existing visibility/version scope; no capability flag or wire field was added.
- Generated C++ codecs and registry integration retain the combined successor methods and
  validation. Identity.v2 timestamp APIs and continued table-in-out output coexist with
  projection acceptance, repeatability and explicit query execution.
- Java's added bulk-index model now uses canonical definition/options fields. The RPC tag
  merge retains annotated String dictionaries alongside upstream binary dictionaries.

## Tested dependencies

| SDK | Dependency used for this qualification |
| --- | --- |
| Python | Published `vgi-rpc 0.52.0` from the lockfile |
| TypeScript | Published `vgi-rpc 0.29.2`, Flechette 2.7.1, Arrow 21.1.1 |
| Rust | Published `vgi-rpc`, client and macros 0.32.0 |
| Go | Declared `vgi-rpc-go 0.35.0`; tested isolated merged source with retained framing patch |
| Java | RPC tag 0.33.1 plus retained annotated String dictionary fix |
| C# | Published `QueryFarm.VgiRpc 0.18.1`; old EC2 sibling source bypassed |
| C++ | RPC tag 0.11.0 plus retained strict-decoding fixes |

Source-backed RPC merges still need coordinated publication/pinning before a release.
The endpoint hash is `41f61f37c7ce42f63f3ca47f8861508f3f356d2422a5b8a0bab48ff92c0857a8`
for 85 methods; it does not fingerprint every nested binary record or semantic metadata rule.

## Bounded EC2 evidence

Artifacts: `logs/main-sync-20261024-kmz7l4hp` on the isolated EC2 task root.

| Lane | Result |
| --- | --- |
| TypeScript | Production build passed; ArrowJS 738 passed/68 skipped, Flechette 739 passed/67 skipped |
| Java | Core 739 passed/2 skipped; examples 61 passed; RPC 681 passed |
| C# | 764 passed against published RPC 0.18.1 |
| Go | 881 passed/1 live-service skip; `go vet` passed |
| Go RPC | 766 passed/17 environment skips; Python-enabled reference subset 36 passed; Python→Go server conformance 1696 passed; `go vet` passed |
| Rust | 664 passed/4 ignored across 45 test binaries |
| C++ | 89/89 CTest cases passed |
| Shared projection/repeatability | 27 cases over subprocess for all six SDKs; HTTP 27 each for Go, TS, Java and C# |
| Native 1.5 contracts | 74 cases/1542 assertions |
| Native 1.5 HTTP, catalog batches, continuation | 8/265, 6/380, 5/473 cases/assertions respectively |
| Native 1.5 discovery/reflection and copy | 8/221 and 2/41 cases/assertions |
| Native 1.5 live references | Repeatability 5, projection 12, explicit-query 18 cases |
| Native six-SDK projection | Each SDK: 5 queries, 42 rows and accepted-plan-copy proof |
| Native 1.5 broad subprocess | 346 passed, 32 skipped, 1 documented pre-existing failure across 379 current test files |

The native static/loadable extensions, shell, unittest, catalog tests and HTTP tests built.
See `native-merge-evidence.json` for native commands and logs. SDK logs are under `sdk/`.
The rebuilt browser bundle SHA256 is
`2b49411f93ff4f538dcbec6c767c663c56026b11f5e5595c0fb749e4743dc6e8`.

**Python: 4602 passed, 129 skipped, no failures** (`python-full-final.log`). Mypy passed
235 source files. The HTTP demo failure exposed a port-allocation race in the test fixture:
it now binds before announcing its port, and the harness checks its own child's liveness.
The 19 affected tests passed, including new collision/concurrent-server regressions, then
passed three repeated parallel runs. All-repository generated-artifact checks pass;
the strict manifest check covers 151 audit rows and 20 contract areas with no unresolved
scope/field reviews. Those coverage checks do not complete the release gates.

**The refreshed 2.0 diagnostic inventory is recorded.** All 110 native C++ sources were
attempted: 54 compiled and 56 failed. Generated `describe.c` also compiled, giving 111
translation units and 55 successful compilations overall. Diagnostics contain 415 errors
under the per-unit capture cap; these are not a count of independent fixes. The two new
native codec sources compile. New `http_params` fails on `FileOpener` overrides, and
`input_dedup` now fails on removed `DataChunk::SetCapacity`. Other existing source statuses
are unchanged. The original 107-source / 53-compiled / 54-failed inventory remains historical.

Evidence: native `docs/duckdb-2.0-a0-main-sync.json` and the appended integration section of
`docs/duckdb-2.0-a0-diagnostics.md`. Neither inventory proves linking or runtime support.
The broad 1.5 run exposed the existing `schema_reconcile.test:40` limitation: the adapter
rejects nested constraint column paths such as the fixture's `nested.a` NOT_NULL. A targeted
rerun with a fresh database reproduced it. Both the guard and fixture facts existed before
the merge, and the SQL test is unchanged; the prior failure is already documented in the
native plan. No guard or fixture was weakened. Other failures were not observed.

The raw broad-run log records 380 files / 347 passed / 32 skipped / 1 failed because it also
ran the retired `parallel_2gb.test_slow` (passed). Main renamed that file to
`parallel_spill_stream.test_slow`; the obsolete copy was archived outside the source tree.
The table reports the 379 current files. Skips require optional services, extensions or
transport-specific fixtures. Logs and the fresh failure reproduction remain available.

Remaining work is the native 2.0 read port, a general SQL provider with shared mutable snapshot
and active-SQL cancellation proof, automatic eligibility/export/replacement, remaining transport
and platform qualification, and P1–P3 release gates. Existing query records remain the planned
carrier; this merge demonstrates no additional wire requirement.
