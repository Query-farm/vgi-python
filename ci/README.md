# CI: the vgi integration suite

[`.github/workflows/integration.yml`](../.github/workflows/integration.yml)
runs the canonical [Query-farm/vgi](https://github.com/Query-farm/vgi)
integration sqllogictest suite against this repo's Python example worker on
every push / PR. The same `.test` files run against the Python, Java, and Go
ports, so a green run here is real wire-compatibility evidence — it exercises
the worker through the *published* DuckDB extension, not a mock, and not the
pure-Python in-process `Client` that the conformance tests in `ci.yml` use.

(The separate [`ci.yml`](../.github/workflows/ci.yml) covers lint / mypy / unit
+ conformance tests / docs / LocalStack S3 offload.)

## How it works (no C++ build)

Rather than building the vgi DuckDB extension from source (which needs the
Haybarn vcpkg pipeline), CI drives a **prebuilt** standalone `haybarn-unittest`
(the DuckDB/Haybarn sqllogictest runner, published in Haybarn's releases) and
installs the **signed** vgi extension from the Haybarn community channel:

1. **Install the workers** — `uv sync --all-extras` installs the `vgi-fixtures`
   workspace member, which puts the fixture console scripts
   (`vgi-fixture-worker`, `-versioned-worker`, `-versioned-tables-worker`,
   `-attach-options-worker`, `-bad-protocol-worker`, `-simple-writable-worker`)
   into `.venv/bin`. Every per-catalog worker is the same Python program with a
   different entrypoint; the base `WorkerClass.main()` understands stdin/stdout,
   `--http`, and the launcher's `--unix`, so one install covers every lane.
2. **Checkout the test suite** — `Query-farm/vgi` at a pinned commit; its
   `test/sql/integration/*.test` files are the suite.
3. **Download the runner** — `haybarn_unittest-linux-amd64.zip` from the pinned
   Haybarn release.
4. **Preprocess** — the standalone runner links none of the extensions the tests
   gate on, so [`preprocess-require.awk`](preprocess-require.awk) rewrites each
   `require <ext>` into an explicit signed `INSTALL <ext> FROM {community,core};
   LOAD <ext>;`. `require-env` and everything else pass through.
5. **Run** — [`run-integration.sh`](run-integration.sh) stages the preprocessed
   tree, wires the `VGI_*_WORKER` env vars at the `.venv/bin` scripts for the
   selected transport, `FORCE INSTALL`s the vgi extension (so the run uses what
   users can install today), then runs the suite in a single `haybarn-unittest`
   invocation. Any failed assertion exits non-zero and fails the job.

The env wiring mirrors the vgi repo's Makefile `test_subprocess` / `test_shm` /
`test_launcher` / `test_http` targets — the canonical local way to run this
suite against the Python worker (see the project `CLAUDE.md`).

## Transport lanes

`run-integration.sh` honours `TRANSPORT=stdio|shm|launch|http` (the workflow
runs them as a matrix):

- **`stdio`** — the subprocess transport (the primary lane); the whole suite.
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
- **`launch`** — the AF_UNIX launcher transport; the whole suite, with *every*
  `VGI_*_WORKER` fronted by `launch:` so traffic flows through the C++ launcher
  (`ResolveLauncherSocketPath` → AF_UNIX → `UnixSocketWorker`). Mirrors the vgi
  Makefile's `test_launcher`: the point is that the launcher path produces
  identical query results to the subprocess path, which only a full-suite run
  demonstrates. Setting `VGI_REQUIRE_LAUNCHER_TRANSPORT` additionally un-skips
  the launcher-only tests (`launcher/*`), whose options apply solely to the
  `launch:` dispatch path and which the other lanes skip via `require-env`.
  The versioned / versioned-tables *http* worker vars are left unset (as in
  `test_launcher`), so those tests skip here — stdio covers them. One deliberate
  divergence from `test_launcher`: see `filter_echo_partitioned.test` below.
- **`http`** — the whole suite over the stateless HTTP transport. Staging
  injects `LOAD httpfs` before each worker ATTACH (the prebuilt binary doesn't
  statically link httpfs).

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
  fully covered by stdio.
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
HAYBARN_UNITTEST=/path/to/haybarn-unittest \
TRANSPORT=stdio \
  ci/run-integration.sh
```

Download `haybarn-unittest` for your platform from the pinned Haybarn release:

```bash
gh release download "$HAYBARN_RELEASE" --repo Query-farm-haybarn/haybarn \
  --pattern 'haybarn_unittest-*.zip'
```

For day-to-day local work against a hand-built C++ extension, prefer the vgi
repo's own targets (`make test_subprocess`, `make test_http`, …) /
`scripts/run_all_tests.sh` as described in `CLAUDE.md`; this harness exists for
CI, where building the extension from source is impractical.

## Version pins (and their coupling)

Two pins live in the workflow's `env:` block:

| Pin | What | Why |
|-----|------|-----|
| `VGI_REF` | the `Query-farm/vgi` ref supplying the `.test` files | **for now tracks `main`** (latest at checkout time) so the port is always validated against the newest suite; switch to a commit SHA for reproducible, deliberate bumps |
| `HAYBARN_RELEASE` | the Haybarn release supplying `haybarn-unittest` | must be ABI-compatible with the community vgi extension |

**The coupling to know about:** the vgi extension is pulled live from the
community channel (`FORCE INSTALL vgi FROM community`), which always serves the
*currently published* build — it is not version-pinned here. So CI verifies the
worker against what users can install today. If `VGI_REF` points at a commit
whose tests exercise a protocol feature the published extension doesn't yet ship
(or vice-versa), that test can fail or skip — bump `VGI_REF` deliberately and
re-validate against the current community extension.
