# CI: the vgi integration suite

[`.github/workflows/integration.yml`](../.github/workflows/integration.yml)
runs the canonical [Query-farm/vgi](https://github.com/Query-farm/vgi)
integration sqllogictest suite against this repo's Python example worker on
every push / PR. The same `.test` files run against the Python, Java, and Go
ports, so a green run here verifies the worker through the native DuckDB
extension over the wire. The native runner and test suite come from the same
VGI commit. Public package releases remain separate from this development check.

(The separate [`ci.yml`](../.github/workflows/ci.yml) covers lint / mypy / unit
+ conformance tests / docs / LocalStack S3 offload.)

## How it works (no C++ build)

CI downloads the native integration runner built by Query-farm/vgi Actions.
It links vgi, httpfs, json and parquet, so the worker's wire contract is checked
against the matching client without waiting for a community extension release.

1. **Install the workers** — `uv sync --all-extras` installs the `vgi-fixtures`
   workspace member, which puts the fixture console scripts
   (`vgi-fixture-worker`, `-versioned-worker`, `-versioned-tables-worker`,
   `-attach-options-worker`, `-bad-protocol-worker`, `-simple-writable-worker`)
   into `.venv/bin`. Every per-catalog worker is the same Python program with a
   different entrypoint; the base `WorkerClass.main()` understands stdin/stdout,
   `--http`, and the launcher's `--unix`, so one install covers every lane.
2. **Checkout the test suite** — `Query-farm/vgi` at a pinned commit; its
   `test/sql/integration/*.test` files are the suite.
3. **Download the runner** — the pinned checkout's `ci_fetch_native_runner.py`
   retrieves its Actions artifact and checks its provenance against that exact
   source commit. It exports `HAYBARN_UNITTEST`, `HAYBARN_CLI` and
   `RUNNER_LINKS_EXTENSIONS=1` for the next step.
4. **Stage** — [`run-integration.sh`](run-integration.sh) copies the selected
   tests unchanged when extensions are linked into the runner. Their normal
   `require` gates load linked extensions; unavailable optional extensions keep
   their normal skip behavior.
5. **Run** — the harness wires the `VGI_*_WORKER` environment variables to the
   `.venv/bin` scripts for the selected transport and executes the suite, with
   `simple_writable` in its own process. Native mode passes `--test-dir` so the
   runner discovers the staged tree instead of its compiled source directory.
   Failed assertions, empty runs, fatal signals and dead shared HTTP workers
   fail the job. The no-error-skip configuration keeps HTTP errors from silently
   skipping tests.

For local checks against the published extension, leave
`RUNNER_LINKS_EXTENSIONS` unset or set it to `0`. This preserves the standalone
runner path: [`preprocess-require.awk`](preprocess-require.awk) converts
`require` gates to signed `INSTALL` and `LOAD` statements, and the harness
warms the community/core extension cache before running.

The env wiring mirrors the vgi repo's Makefile `test_subprocess` / `test_shm` /
`test_launcher` / `test_http` targets — the canonical local way to run this
suite against the Python worker (see the project `CLAUDE.md`).

## Transport lanes

`run-integration.sh` honours `TRANSPORT=stdio|shm|launch|http` and defaults to
`launch` when the variable is omitted. CI runs `launch`, `shm`, and `http` as a
matrix; `stdio` remains available for local compatibility testing:

- **`stdio`** — the local subprocess compatibility lane; the whole suite. It is
  not a CI release gate because its fork-per-connection runtime largely
  duplicates the launcher lane.
  Also boots the versioned and versioned-tables workers as background HTTP
  servers (`VGI_VERSIONED_HTTP_WORKER` / `VGI_VERSIONED_TABLES_HTTP_WORKER`) so
  the `attach/versioned_tables_*_http` and `versioning_http` tests run.
- **`shm`** — the **`launch`** lane plus the POSIX shared-memory side channel
  (`VGI_RPC_SHM_SIZE_BYTES`); the whole suite. vgi-rpc-python attaches the
  segment transparently, so the env var alone flips the side channel on.
  This deliberately differs from the vgi Makefile's `test_shm`, which layers shm
  on raw subprocess: `stdio` already covers the fork-per-connection path and
  spends most of its wall-clock doing it, so paying that cost twice just to
  exercise shm is waste. The side channel is transport-independent — negotiated
  via the `__transport_options__` handshake and carried in POSIX shm rather than
  in the pipe or socket — and engages identically over the launcher (verified:
  the same 18 `[shm]` transfers under `VGI_RPC_SHM_DEBUG=1` on both).
- **`launch`** — the default and primary AF_UNIX launcher transport; the whole suite, with *every*
  `VGI_*_WORKER` fronted by `launch:` so traffic flows through the C++ launcher
  (`ResolveLauncherSocketPath` → AF_UNIX → `UnixSocketWorker`). Mirrors the vgi
  Makefile's `test_launcher`: the point is that the launcher path produces
  identical query results to the subprocess path, which only a full-suite run
  demonstrates. Setting `VGI_REQUIRE_LAUNCHER_TRANSPORT` additionally un-skips
  the launcher-only tests (`launcher/*`), whose options apply solely to the
  `launch:` dispatch path and which the other lanes skip via `require-env`.
  The versioned / versioned-tables *http* worker vars are left unset (as in
  `test_launcher`), so those tests skip here; run the local stdio lane to cover
  them. One deliberate divergence from `test_launcher`: see
  `filter_echo_partitioned.test` below.
- **`http`** — the whole suite over the stateless HTTP transport. The native
  runner links httpfs; standalone mode injects `LOAD httpfs` before worker
  attachments.

The `simple_writable/*.test` write-path tests (INSERT/UPDATE/DELETE/RETURNING)
run on **every** lane against `VGI_SIMPLE_WRITABLE_WORKER`
(`vgi-fixture-simple-writable-worker`), in their own `haybarn-unittest`
invocation so their warm pooled connections don't perturb the immediately
following crash-recovery test. That env var is always a spawned binary — a
`launch:` path on the launch lane, a plain path elsewhere — so these tests run
over subprocess even on the http lane rather than skipping.

The crash / pool-recovery tests (`table_in_out/table_buffering_*`) are gated on
`VGI_TEST_DEDICATED_WORKER`, which is set on the **`stdio` lane only**. Their
`crash_on_process` fixture does `os.kill(getpid(), SIGKILL)`; that is a
recoverable per-DuckDB-process child kill under subprocess transport, but under
any shared-worker transport (`http://`, `launch:`) it kills the one process
serving the whole suite and every later `ATTACH` fails. Because `shm` now rides
the launcher, `stdio` is the only lane that can host them — worth knowing if you
ever narrow the stdio lane.

### Excluded tests

Out of scope on **every** lane:

- `writable/` — the opt-in *generic* writable catalog
  (`VGI_WORKER_ENABLE_WRITABLE`), not modelled by a cross-language fixture (the
  vgi Makefile excludes it too). `simple_writable/` is *not* excluded.
- `nested_type_combinations.test` — segfaults the prebuilt standalone runner (a
  property of that C++ build, not the worker, which passes it against a
  locally-built unittest).

(`bool_in_union.test` is not excluded here — it disables itself with `mode skip`
upstream, being an arch-dependent Haybarn/DuckDB Arrow serialization bug, so the
flag in the test is the single source of truth.)

Dropped on the **http** lane only:

- `projection_pushdown_repro.test` — one POST per two rows; transport-agnostic,
  fully covered by launch.
- `dynamic_filter.test` — Top-N + dynamic-filter continuation terminates early
  over http in the prebuilt binary (a property of that C++ build).
- `partitioned_sequence.test` and `table_in_out/buffer_input/sizes.test` —
  partition-local / input-buffering state that the stateless HTTP transport does
  not preserve across exchanges (the known Python HTTP limitations documented in
  `CLAUDE.md`). `buffer_input/scale.test_slow` is a `.test_slow` file, which the
  harness never stages (it only finds `*.test`).

Nothing is dropped on the **launch** lane. `test_launcher` upstream excludes two
files, and neither applies:

- `test/sql/vgi_worker_pool.test` lives outside `test/sql/integration`, so the
  harness never stages it.
- `table/filter_echo_partitioned.test` is excluded upstream for asserting >1
  distinct `worker_pid`, which AF_UNIX socket pooling cannot satisfy. That
  rationale is stale — the test now counts transport-neutral `conn=` ids instead
  — and it passes over `launch:` against the Python fixture worker. Keeping it
  preserves coverage of precisely what the launcher changes: connection
  multiplexing.

The HTTP-attach / bearer-auth / dynamic-code / schema-reconcile-only tests skip
via their `require-env` gates when we don't set the corresponding worker.
`bad_enum.test` skips on every lane — `VGI_BAD_ENUM_WORKER` is not wired up here
even though the fixture (`vgi-fixture-bad-enum-worker`) is installed.

## Run it locally

```bash
uv sync --all-extras
VGI_SRC=~/Development/vgi \
HAYBARN_UNITTEST=~/Development/vgi/build/release/test/unittest \
RUNNER_LINKS_EXTENSIONS=1 \
TRANSPORT=stdio \
  ci/run-integration.sh
```

To check the published extension instead, download `haybarn-unittest` for your
platform from a compatible Haybarn release and leave `RUNNER_LINKS_EXTENSIONS=0`:

```bash
gh release download "$HAYBARN_RELEASE" --repo Query-farm-haybarn/haybarn \
  --pattern 'haybarn_unittest-*.zip'
```

The vgi repo's own targets (`make test_subprocess`, `make test_http`, …) and
`scripts/run_all_tests.sh` remain available as described in `CLAUDE.md`.

## Version pins (and their coupling)

The workflow's `VGI_REF` pins the test suite and the native Actions artifact:

| Pin | What | Why |
|-----|------|-----|
| `VGI_REF` | the `Query-farm/vgi` commit supplying the tests and native runner | the downloader requires an artifact built from that exact commit |

The optional standalone mode still installs the currently published community
extension. Use a compatible test suite and worker when checking that release;
an unreleased minor wire revision requires the matching native development
runner. Changing the CI pin does not publish an extension or an SDK package.
