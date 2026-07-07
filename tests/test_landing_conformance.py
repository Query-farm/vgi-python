"""Cross-language landing-page conformance guard (Python side).

Boots the example HTTP worker (``vgi-fixture-http``) and asserts its landing
surface conforms to the shared contract: ``describe.json`` validates against the
pinned JSON Schema and matches the normalized golden, ``GET /`` serves the pinned
``landing.html``, and the lazy column endpoints return valid payloads.

The schema + golden + checker are vendored from ``~/Development/vgi/test/landing``
(the canonical cross-language source). Adding a function/table to the example
worker without regenerating the golden fails this test — the drift guard. Regen by
running ``run_landing_conformance.py --url http://localhost:PORT --golden
fixtures/describe.expected.json --update`` and copying the golden into
``tests/landing/``.
"""

from __future__ import annotations

import importlib.util
import pathlib
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Iterator

import pytest

_LANDING = pathlib.Path(__file__).parent / "landing"


def _load_checker() -> object:
    spec = importlib.util.spec_from_file_location("_landing_checker", _LANDING / "_checker.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _wait_ready(url: str, timeout: float = 45.0) -> None:
    deadline = time.monotonic() + timeout
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:  # noqa: S310 — local test URL
                if resp.status == 200:
                    return
        except (urllib.error.URLError, OSError) as exc:  # not up yet
            last = exc
        time.sleep(0.2)
    raise RuntimeError(f"worker not ready at {url}: {last}")


@pytest.fixture(scope="module")
def worker_url(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    """Boot ``vgi-fixture-http`` on an ephemeral port; yield its base URL."""
    port_file = tmp_path_factory.mktemp("landing") / "port"
    proc = subprocess.Popen(
        [sys.executable, "-m", "vgi._test_fixtures.http_server", "--port", "0", "--port-file", str(port_file)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 45.0
        while time.monotonic() < deadline:
            if port_file.exists() and port_file.read_text().strip():
                break
            if proc.poll() is not None:
                raise RuntimeError(f"fixture http server exited early (code {proc.returncode})")
            time.sleep(0.2)
        else:
            raise RuntimeError("fixture http server did not publish a port")
        port = int(port_file.read_text().strip())
        url = f"http://127.0.0.1:{port}"
        _wait_ready(f"{url}/?format=json")
        yield url
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def test_landing_conformance(worker_url: str) -> None:
    """The example worker's landing surface matches the schema and golden."""
    checker = _load_checker()
    fails = checker.check(  # type: ignore[attr-defined]
        worker_url,
        schema_path=_LANDING / "describe.schema.json",
        golden_path=_LANDING / "describe.expected.json",
    )
    assert not fails, "landing conformance failures:\n" + "\n".join(f"  - {f}" for f in fails)
