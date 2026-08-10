# Release runbook — describe.json retirement + attach-option `required`

All code is committed on `main` in each repo and green. Nothing is pushed.
This is the ordering and the per-repo trigger; fill in `X.Y.Z` per package.

**Version decision needed first.** Three transports drop `DescribeProvider`
from their public API (breaking): `@query-farm/vgi-rpc`, `vgi-rpc` (Rust),
`farm.query:vgirpc`. Choose major or minor per your compatibility policy — it
cascades into the implementation pins below.

## 1. Transports (must publish first)

| Repo | Trigger | Version lives in |
|---|---|---|
| vgi-rpc-typescript | `gh release create vX.Y.Z` (release: published) | `package.json` (+ `bun install` to sync `bun.lock`) |
| vgi-rpc-rust | tag push — `gh release create vX.Y.Z` | `[workspace.package] version`; `cargo update -w` |
| vgi-rpc-java | `gh release create vX.Y.Z` (release: published) | `build.gradle.kts` |
| vgi-rpc-go | tag push — `gh release create vX.Y.Z` | none (module proxy reads the tag) |

Then wait for index propagation: npm, `https://index.crates.io/3/v/vgi-rpc`,
Maven Central, Go proxy.

## 2. Implementation dependency pins

These currently cannot build against published transports:

- **vgi-typescript** — peer range is `>=0.18.1 <0.19.0`; widen to the new
  transport version. `LandingInfo` resolves only once the transport publishes.
- **vgi-rust** — workspace dep already bumped to `vgi-rpc 0.20.0`; adjust if you
  pick a different number. The `[patch.crates-io]` block stays uncommitted (it
  is documented in `Cargo.toml` for local co-development only).
- **vgi-java** — bump `api("farm.query:vgirpc:X")` and the `VGI_RPC_JAVA_REF`
  pin in `.github/workflows/integration.yml`. (`landing.yml` was deleted with
  the contract it checked.)
- **vgi-go** — self-contained; no pin, but it needs the new vgi-rpc-go tag.

## 3. Implementations

| Repo | Trigger | Version lives in |
|---|---|---|
| vgi-python | `gh release create vX.Y.Z --target main` (PyPI on release) | root `pyproject.toml`; `uv lock` |
| vgi-go | `gh release create vX.Y.Z` (tag → module proxy) | none |
| vgi-rust | `gh release create vX.Y.Z` (tag → crates.io) | `[workspace.package] version` |
| vgi-typescript | **`git tag vX.Y.Z && git push origin vX.Y.Z`** — pushing the bump to main does NOT publish and fails silently | `package.json`; `bun install` |
| vgi-java | `gh release create vX.Y.Z` (Maven Central on release) | `build.gradle.kts` |

## 4. Frontend

- **vgi-web-frontend** — auto-deploys on push to `main`. Push last, so no worker
  is serving a page newer than its vendored bundle.

## Verified before release

- vgi-python 2175 pass / 99 skip; ruff + mypy clean
- vgi-go `go test ./...` clean; vet + gofmt clean
- vgi-rust tests pass; clippy `-D warnings` clean
- vgi-java `:vgi:test` passes
- vgi-rpc-typescript 715 pass / 26 fail — exactly the pre-change baseline
  (confirmed by running the suite at the parent commit in a worktree)
- Landing page verified in a browser against all four language workers:
  Python 221 functions, Go 221, Rust 654, Java 219 — each 2 schemas, 59 tables,
  3 views, with lazy columns loading.

## Not done, tracked

- `vgi-rpc-typescript` has 26 pre-existing test failures (integration/streaming;
  unrelated to this work, present before it).
- `~/vgi-java/CLAUDE.md` still references the landing-conformance workflow in
  its historical notes; the workflow itself is deleted.
