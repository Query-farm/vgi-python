# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Exercise the vendored browser client against the Python HTTP server."""

from __future__ import annotations

import json
import shutil
import subprocess
import threading
from pathlib import Path
from wsgiref.simple_server import make_server

import pyarrow as pa
import pytest

from vgi.scalar_function import ScalarFunction
from vgi.serve import create_app
from vgi.worker import Worker


class _Double(ScalarFunction):
    class Meta:
        name = "double"

    def compute(self, x: pa.Int64Array) -> pa.Int64Array:
        return pa.compute.multiply(x, 2)


class _LandingWorker(Worker):
    functions = [_Double]


@pytest.mark.skipif(shutil.which("bun") is None, reason="Bun is required to execute the shipped browser client")
def test_served_browser_client_discovers_catalog(tmp_path: Path) -> None:
    """A successful asset GET is insufficient: the bundle must speak the current RPC protocol."""
    script = tmp_path / "discover.mjs"
    script.write_text("""
const url = process.argv[2];
let rpc;
try {
  const response = await fetch(url + '/vgi-client.js');
  if (!response.ok) throw new Error('bundle HTTP ' + response.status);
  const path = new URL('./client.mjs', import.meta.url);
  await Bun.write(path, await response.arrayBuffer());
  const browser = await import(path.href);
  rpc = browser.httpConnect(url);
  const client = new browser.VgiClient(rpc);
  const infos = await client.catalogsInfo();
  if (!infos.length) throw new Error('No catalogs discovered');
  const attached = await client.catalogAttach(infos[0].name);
  const schemas = await client.schemas(attached.attach_opaque_data);
  const names = [];
  for (const schema of schemas) {
    const functions = await client.schemaContentsFunctions(attached.attach_opaque_data, schema.path, 'SCALAR_FUNCTION');
    names.push(...functions.map(f => f.name));
  }
  console.log(JSON.stringify(names));
} catch (error) {
  console.error(error.stack || String(error));
  process.exitCode = 1;
} finally {
  rpc?.close();
}
""")
    app = create_app(_LandingWorker, signing_key=b"landing-discovery-test-key")
    with make_server("127.0.0.1", 0, app) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            result = subprocess.run(
                ["bun", str(script), f"http://127.0.0.1:{server.server_port}"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            assert result.returncode == 0, result.stderr
            assert "double" in json.loads(result.stdout)
        finally:
            server.shutdown()
            thread.join(timeout=5)
