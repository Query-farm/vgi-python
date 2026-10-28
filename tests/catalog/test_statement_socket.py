# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103
"""Run the bounded statement example in a separate process over real HTTP."""

from __future__ import annotations

import os
import selectors
import subprocess
import sys
from pathlib import Path

from vgi.client import Client

from .statement_reference import CREATE, DROP, INSERT, SELECT, UPDATE
from .test_statement_protocol import attach, begin, execute, finish, parameters, table, version


def test_statement_example_over_socket_http(tmp_path: Path) -> None:
    env = os.environ | {"VGI_STATEMENT_REFERENCE_DIR": str(tmp_path / "provider")}
    with (tmp_path / "provider.log").open("wb") as log:
        server = subprocess.Popen(
            [sys.executable, "-m", "tests.catalog.statement_reference", "--port", "0"],
            cwd=Path(__file__).resolve().parents[2],
            env=env,
            stdout=subprocess.PIPE,
            stderr=log,
        )
        try:
            assert server.stdout is not None
            with selectors.DefaultSelector() as selector:
                selector.register(server.stdout, selectors.EVENT_READ)
                assert selector.select(timeout=30), "statement example did not start"
            announced = server.stdout.readline().decode().strip()
            assert announced.startswith("PORT:"), announced
            url = f"http://127.0.0.1:{int(announced.split(':')[1])}"
            with Client.from_http(url) as client:
                attached = attach(client)
                tx = begin(client, attached)
                assert execute(client, attached, tx, CREATE).to_pydict() == {"affected_rows": [None]}
                assert execute(client, attached, tx, INSERT, parameters(id=10, label="socket ' value")).to_pydict() == {
                    "affected_rows": [1]
                }
                assert table(client, attached, tx) is not None
                assert version(client, attached, tx) == 2
                assert table(client, attached) is None
                finish(client, attached, tx, "commit")
                client.catalog_detach(attach_opaque_data=attached.attach_opaque_data)
            # A new client/attachment observes committed changes and rolls back
            # a later DML+DDL transaction through the same existing RPCs.
            with Client.from_http(url) as client:
                attached = attach(client)
                assert table(client, attached) is not None and version(client, attached) == 2
                tx = begin(client, attached)
                assert execute(client, attached, tx, SELECT).to_pydict()["label"][-1] == "socket ' value"
                execute(client, attached, tx, UPDATE, parameters(id=10, label="rolled back"))
                execute(client, attached, tx, DROP)
                assert table(client, attached, tx) is None
                finish(client, attached, tx)
                tx = begin(client, attached)
                assert execute(client, attached, tx, SELECT).to_pydict()["label"][-1] == "socket ' value"
                assert table(client, attached, tx) is not None and version(client, attached, tx) == 2
                finish(client, attached, tx)
                client.catalog_detach(attach_opaque_data=attached.attach_opaque_data)
        finally:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=10)
            if server.stdout is not None:
                server.stdout.close()
