# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""A scalar's ``compute()`` receives its ATTACH options through an ``Attach()`` parameter."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Annotated, Any

import pyarrow as pa
import pytest
from vgi_rpc.rpc import AuthContext

from vgi import attach_secret_options as aso
from vgi._test_fixtures.writable_tx import TxfixWorker
from vgi.arguments import Arguments, Attach, Param, Returns
from vgi.function_storage import BoundStorage
from vgi.invocation import FunctionType, GlobalInitResponse
from vgi.protocol import BindRequest, CatalogAttachRequest, InitRequest
from vgi.scalar_function import ScalarFunction


@pytest.fixture(autouse=True)
def _private_key_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Point the host-local attach key at a per-test file."""
    monkeypatch.setenv("VGI_ATTACH_SECRET_KEY_FILE", str(tmp_path / "keys" / "attach-secret-options.key"))
    aso._host_local_cache.clear()
    aso.set_attach_secret_options_key(None)
    yield
    aso._host_local_cache.clear()


class _OptionEcho(ScalarFunction):
    """Returns one ATTACH option per row."""

    class Meta:
        """Metadata."""

        name = "option_echo"

    @classmethod
    def compute(
        cls,
        x: Annotated[pa.Int64Array, Param(doc="Any column")],
        options: Annotated[dict[str, Any], Attach()],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Echo the ``token`` option (or 'none') for every row."""
        value = options.get("token")
        return pa.array([str(value) if value is not None else "none"] * len(x), type=pa.string())


def _run(attach_plaintext: bytes | None) -> list[str]:
    bind_call = BindRequest(
        function_name="option_echo",
        arguments=Arguments(),
        function_type=FunctionType.SCALAR,
        input_schema=pa.schema([("x", pa.int64())]),
    )
    init_call = InitRequest(bind_call=bind_call, output_schema=pa.schema([("result", pa.string())]))
    result = _OptionEcho.process(
        batch=pa.record_batch({"x": [1, 2]}),
        init_call=init_call,
        init_response=GlobalInitResponse(),
        storage=BoundStorage(_OptionEcho.storage, b"\x00" * 16, attach_plaintext=attach_plaintext),
        auth_context=AuthContext.anonymous(),
    )
    return [str(v) for v in result.column(0).to_pylist()]


def test_attach_param_detected() -> None:
    """The marker is found and is not a call argument."""
    assert _OptionEcho._attach_param == "options"
    assert "options" not in _OptionEcho._compute_params
    assert "options" not in _OptionEcho._const_params


def test_compute_receives_attach_options_including_secret() -> None:
    """Options of a real attach (a secret one included) reach compute() through the unwrapped attach."""
    worker = TxfixWorker(quiet=True)
    options = pa.RecordBatch.from_pylist([{"token": "tok-attach-echo", "transactional": False}])
    sealed = worker.catalog_attach(
        CatalogAttachRequest(name="txfix", options=options, data_version_spec=None, implementation_version=None)
    ).attach_opaque_data
    assert sealed is not None
    assert b"tok-attach-echo" not in bytes(sealed)

    assert _run(worker._unwrap_attach_full(bytes(sealed))) == ["tok-attach-echo", "tok-attach-echo"]


def test_no_attach_gives_empty_options() -> None:
    """Outside an attached catalog the parameter is ``{}``."""
    assert _run(None) == ["none", "none"]


def test_two_attach_params_rejected() -> None:
    """Declaring the marker twice is an error at class definition."""
    with pytest.raises(TypeError, match="multiple Attach parameters"):

        class _Twice(ScalarFunction):
            class Meta:
                name = "twice"

            @classmethod
            def compute(
                cls,
                x: Annotated[pa.Int64Array, Param(doc="x")],
                a: Annotated[dict[str, Any], Attach()],
                b: Annotated[dict[str, Any], Attach()],
            ) -> Annotated[pa.Int64Array, Returns()]:
                return x
