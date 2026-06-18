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
- **`shm`** — `stdio` plus the POSIX shared-memory side channel
  (`VGI_RPC_SHM_SIZE_BYTES`); the whole suite. vgi-rpc-python attaches the
  segment transparently, so the env var alone flips the transport.
- **`launch`** — the AF_UNIX launcher transport; the launcher-only tests
  (`launcher/*`), which the other lanes skip via `require-env`.
- **`http`** — the whole suite over the stateless HTTP transport. Staging
  injects `LOAD httpfs` before each worker ATTACH (the prebuilt binary doesn't
  statically link httpfs).

The `simple_writable/*.test` write-path tests (INSERT/UPDATE/DELETE/RETURNING)
run on the subprocess lanes against `VGI_SIMPLE_WRITABLE_WORKER`
(`vgi-fixture-simple-writable-worker`), in their own `haybarn-unittest`
invocation so their warm pooled connections don't perturb the immediately
following crash-recovery test. They self-skip on the http lane
(skip-on-error `'HTTP'`).

### Excluded tests

Out of scope on **every** lane:

- `writable/` — the opt-in *generic* writable catalog
  (`VGI_WORKER_ENABLE_WRITABLE`), not modelled by a cross-language fixture (the
  vgi Makefile excludes it too). `simple_writable/` is *not* excluded.
- `nested_type_combinations.test` — segfaults the prebuilt standalone runner (a
  property of that C++ build, not the worker, which passes it against a
  locally-built unittest).
- `bool_in_union.test` — a pre-existing, arch-dependent union-bool bug whose
  pinned expected output matches arm64 but not amd64 (CI is amd64).

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

The HTTP-attach / bearer-auth / dynamic-code / schema-reconcile-only tests skip
via their `require-env` gates when we don't set the corresponding worker.

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
