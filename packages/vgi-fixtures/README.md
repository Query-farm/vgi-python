# vgi-fixtures

<p align="center">
  <strong>Example and test workers for <a href="https://pypi.org/project/vgi/">VGI (Vector Gateway Interface)</a>.</strong>
</p>

<p align="center">
  Created by <a href="https://query.farm">Query.Farm</a>
</p>

---

This is a companion distribution for [`vgi`](https://pypi.org/project/vgi/). It ships
the example/test worker modules (`vgi._test_fixtures`) plus the `vgi-fixture-*`
console commands that the VGI test, documentation, and integration suites drive.

These modules are **deliberately excluded from the base `vgi` wheel** so that a
production `pip install vgi` stays lean and free of test-only code and its
dependencies (`numpy`, `sqlglot`). Install this package when you want the example
workers from a traditional, batteries-included setup:

```bash
pip install vgi-fixtures        # vgi + the example/test workers
# equivalently:
pip install 'vgi[fixtures]'
```

Both install the same companion wheel, which overlays `vgi/_test_fixtures/` onto
the installed `vgi` package and registers these commands:

| Command | Module |
|---|---|
| `vgi-fixture-worker` | `vgi._test_fixtures.worker` |
| `vgi-fixture-http` | `vgi._test_fixtures.http_server` |
| `vgi-fixture-versioned-worker` | `vgi._test_fixtures.versioned` |
| `vgi-fixture-versioned-tables-worker` | `vgi._test_fixtures.versioned_tables` |
| `vgi-fixture-attach-options-worker` | `vgi._test_fixtures.attach_options` |
| `vgi-fixture-bad-protocol-worker` | `vgi._test_fixtures.bad_protocol` |
| `vgi-fixture-writable-worker` | `vgi._test_fixtures.writable.worker` |
| `vgi-fixture-simple-writable-worker` | `vgi._test_fixtures.simple_writable` |

> **Not for production use.** These workers exist to demonstrate and exercise VGI
> features. Build your own worker against the public `vgi` API instead — see the
> [`vgi` documentation](https://github.com/Query-farm/vgi-python/tree/main/docs).

## License

Distributed under the Query Farm Source-Available License, Version 1.0. See
[`LICENSE`](https://github.com/Query-farm/vgi-python/blob/main/LICENSE).
