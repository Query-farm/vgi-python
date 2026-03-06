"""Tests for auth context support in VGI functions."""

from __future__ import annotations

from typing import Annotated, Any
from unittest.mock import patch

import pyarrow as pa
import pytest
from vgi_rpc.rpc import AuthContext, OutputCollector

from vgi.arguments import Auth, Param, Returns
from vgi.function_storage import BoundStorage
from vgi.invocation import FunctionType, GlobalInitResponse
from vgi.scalar_function import ScalarFunction

# ---------------------------------------------------------------------------
# Test functions
# ---------------------------------------------------------------------------


class _AuthEchoFunction(ScalarFunction):
    """Test function that echoes auth principal."""

    class Meta:
        """Test metadata."""

        name = "auth_echo"

    @classmethod
    def compute(
        cls,
        x: Annotated[pa.Int64Array, Param(doc="input")],
        auth: Annotated[AuthContext, Auth()],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Echo the auth principal."""
        name = auth.principal or "anonymous"
        return pa.array([name] * len(x))


class _NoAuthFunction(ScalarFunction):
    """Test function without Auth param."""

    class Meta:
        """Test metadata."""

        name = "no_auth"

    @classmethod
    def compute(
        cls,
        x: Annotated[pa.Int64Array, Param(doc="input")],
    ) -> Annotated[pa.Int64Array, Returns()]:
        """Double the input."""
        return pa.array([v.as_py() * 2 for v in x])


def _make_scalar_test_context(
    func_name: str,
    input_schema: pa.Schema,
    output_schema: pa.Schema,
) -> tuple[Any, Any]:
    """Create BindRequest/InitRequest for testing scalar functions."""
    from vgi.arguments import Arguments
    from vgi.protocol import BindRequest, InitRequest

    bind_call = BindRequest(
        function_name=func_name,
        arguments=Arguments(),
        function_type=FunctionType.SCALAR,
        input_schema=input_schema,
    )
    init_call = InitRequest(
        bind_call=bind_call,
        output_schema=output_schema,
    )
    return bind_call, init_call


# ---------------------------------------------------------------------------
# Auth annotation tests
# ---------------------------------------------------------------------------


class TestAuthAnnotation:
    """Tests for Auth annotation detection and injection."""

    def test_auth_param_detected(self) -> None:
        """Auth param is detected by __init_subclass__."""
        assert _AuthEchoFunction._auth_param == "auth"

    def test_no_auth_param(self) -> None:
        """Functions without Auth have _auth_param = None."""
        assert _NoAuthFunction._auth_param is None

    def test_auth_injected_into_compute(self) -> None:
        """Auth context is injected when processing batches."""
        _, init_call = _make_scalar_test_context(
            "auth_echo",
            pa.schema([("x", pa.int64())]),
            pa.schema([("result", pa.string())]),
        )
        batch = pa.record_batch({"x": [1, 2, 3]})
        auth = AuthContext(principal="alice", authenticated=True, domain="bearer")
        storage = BoundStorage(_AuthEchoFunction.storage, b"\x00" * 16)

        result = _AuthEchoFunction.process(
            batch=batch,
            init_call=init_call,
            init_response=GlobalInitResponse(),
            storage=storage,
            auth_context=auth,
        )
        assert result.column("result").to_pylist() == ["alice", "alice", "alice"]

    def test_auth_defaults_to_anonymous(self) -> None:
        """When anonymous auth is passed, principal is None -> 'anonymous'."""
        _, init_call = _make_scalar_test_context(
            "auth_echo",
            pa.schema([("x", pa.int64())]),
            pa.schema([("result", pa.string())]),
        )
        batch = pa.record_batch({"x": [1]})
        storage = BoundStorage(_AuthEchoFunction.storage, b"\x00" * 16)

        result = _AuthEchoFunction.process(
            batch=batch,
            init_call=init_call,
            init_response=GlobalInitResponse(),
            storage=storage,
            auth_context=AuthContext.anonymous(),
        )
        assert result.column("result").to_pylist() == ["anonymous"]

    def test_no_auth_function_ignores_auth(self) -> None:
        """Functions without Auth param still work with auth_context kwarg."""
        _, init_call = _make_scalar_test_context(
            "no_auth",
            pa.schema([("x", pa.int64())]),
            pa.schema([("result", pa.int64())]),
        )
        batch = pa.record_batch({"x": [5]})
        auth = AuthContext(principal="alice", authenticated=True, domain="bearer")
        storage = BoundStorage(_NoAuthFunction.storage, b"\x00" * 16)

        result = _NoAuthFunction.process(
            batch=batch,
            init_call=init_call,
            init_response=GlobalInitResponse(),
            storage=storage,
            auth_context=auth,
        )
        assert result.column("result").to_pylist() == [10]


# ---------------------------------------------------------------------------
# Table function auth tests
# ---------------------------------------------------------------------------


class TestTableFunctionAuth:
    """Tests for auth in table function ProcessParams."""

    def test_process_params_default_anonymous(self) -> None:
        """ProcessParams defaults to anonymous auth."""
        from vgi.table_function import ProcessParams

        params = ProcessParams(
            args=None,
            init_call=None,  # type: ignore[arg-type]
            init_response=None,  # type: ignore[arg-type]
            output_schema=pa.schema([]),
            settings={},
            secrets={},
            storage=None,  # type: ignore[arg-type]
        )
        assert params.auth_context.authenticated is False
        assert params.auth_context.principal is None

    def test_process_params_with_auth(self) -> None:
        """ProcessParams accepts explicit auth context."""
        from vgi.table_function import ProcessParams

        auth = AuthContext(principal="bob", authenticated=True, domain="jwt")
        params = ProcessParams(
            args=None,
            init_call=None,  # type: ignore[arg-type]
            init_response=None,  # type: ignore[arg-type]
            output_schema=pa.schema([]),
            settings={},
            secrets={},
            storage=None,  # type: ignore[arg-type]
            auth_context=auth,
        )
        assert params.auth_context.principal == "bob"
        assert params.auth_context.domain == "jwt"

    def test_bind_params_default_anonymous(self) -> None:
        """BindParams defaults to anonymous auth."""
        from vgi.table_function import BindParams, SecretsAccessor

        params = BindParams(
            args=None,
            bind_call=None,  # type: ignore[arg-type]
            settings={},
            secrets=SecretsAccessor(None),
        )
        assert params.auth_context.authenticated is False

    def test_bind_params_with_auth(self) -> None:
        """BindParams accepts explicit auth context."""
        from vgi.table_function import BindParams, SecretsAccessor

        auth = AuthContext(principal="carol", authenticated=True, domain="bearer")
        params = BindParams(
            args=None,
            bind_call=None,  # type: ignore[arg-type]
            settings={},
            secrets=SecretsAccessor(None),
            auth_context=auth,
        )
        assert params.auth_context.principal == "carol"

    def test_init_params_default_anonymous(self) -> None:
        """InitParams defaults to anonymous auth."""
        from vgi.table_function import InitParams

        params = InitParams(
            args=None,
            init_call=None,  # type: ignore[arg-type]
            execution_id=b"\x00" * 16,
            output_schema=pa.schema([]),
            settings={},
            secrets={},
            storage=None,  # type: ignore[arg-type]
        )
        assert params.auth_context.authenticated is False


# ---------------------------------------------------------------------------
# Env-var resolver tests
# ---------------------------------------------------------------------------


class TestResolveAuthenticate:
    """Tests for _resolve_authenticate() env var parsing."""

    def test_unset_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """No env vars -> None."""
        monkeypatch.delenv("VGI_BEARER_TOKENS", raising=False)
        monkeypatch.delenv("VGI_JWT_ISSUER", raising=False)
        from vgi.serve import _resolve_authenticate

        assert _resolve_authenticate() is None

    def test_bearer_tokens_single(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Single bearer token parsed correctly."""
        monkeypatch.setenv("VGI_BEARER_TOKENS", "tok123=alice")
        monkeypatch.delenv("VGI_JWT_ISSUER", raising=False)
        from vgi.serve import _resolve_authenticate

        result = _resolve_authenticate()
        assert result is not None
        assert callable(result)

    def test_bearer_tokens_multiple(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Multiple bearer tokens parsed correctly."""
        monkeypatch.setenv("VGI_BEARER_TOKENS", "tok1=alice,tok2=bob")
        monkeypatch.delenv("VGI_JWT_ISSUER", raising=False)
        from vgi.serve import _resolve_authenticate

        result = _resolve_authenticate()
        assert result is not None

    def test_bearer_tokens_malformed_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Malformed token entry (no '=') causes SystemExit."""
        monkeypatch.setenv("VGI_BEARER_TOKENS", "badformat")
        monkeypatch.delenv("VGI_JWT_ISSUER", raising=False)
        from vgi.serve import _resolve_authenticate

        with pytest.raises(SystemExit):
            _resolve_authenticate()

    def test_bearer_tokens_empty_principal(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Token with empty principal is allowed."""
        monkeypatch.setenv("VGI_BEARER_TOKENS", "tok=")
        monkeypatch.delenv("VGI_JWT_ISSUER", raising=False)
        from vgi.serve import _resolve_authenticate

        result = _resolve_authenticate()
        assert result is not None

    def test_jwt_missing_audience_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """JWT issuer without audience causes SystemExit."""
        monkeypatch.setenv("VGI_JWT_ISSUER", "https://issuer.example.com")
        monkeypatch.delenv("VGI_JWT_AUDIENCE", raising=False)
        monkeypatch.delenv("VGI_BEARER_TOKENS", raising=False)
        from vgi.serve import _resolve_authenticate

        with pytest.raises(SystemExit):
            _resolve_authenticate()

    def test_both_bearer_and_jwt(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Both bearer and JWT -> chain_authenticate."""
        monkeypatch.setenv("VGI_BEARER_TOKENS", "tok=alice")
        monkeypatch.setenv("VGI_JWT_ISSUER", "https://issuer.example.com")
        monkeypatch.setenv("VGI_JWT_AUDIENCE", "my-api")
        from vgi.serve import _resolve_authenticate

        # Mock jwt_authenticate since authlib may not be installed
        with patch("vgi.serve._resolve_jwt_authenticate", return_value=lambda req: AuthContext.anonymous()):
            result = _resolve_authenticate()
        assert result is not None


class TestResolveOAuthResourceMetadata:
    """Tests for _resolve_oauth_resource_metadata() env var parsing."""

    def test_unset_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """No env vars -> None."""
        monkeypatch.delenv("VGI_OAUTH_RESOURCE", raising=False)
        from vgi.serve import _resolve_oauth_resource_metadata

        assert _resolve_oauth_resource_metadata() is None

    def test_valid_config(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Valid config produces OAuthResourceMetadata."""
        monkeypatch.setenv("VGI_OAUTH_RESOURCE", "https://api.example.com")
        monkeypatch.setenv("VGI_OAUTH_AUTH_SERVERS", "https://auth.example.com")
        from vgi.serve import _resolve_oauth_resource_metadata

        result = _resolve_oauth_resource_metadata()
        assert result is not None
        assert result.resource == "https://api.example.com"

    def test_missing_auth_servers_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Resource without auth servers causes SystemExit."""
        monkeypatch.setenv("VGI_OAUTH_RESOURCE", "https://api.example.com")
        monkeypatch.delenv("VGI_OAUTH_AUTH_SERVERS", raising=False)
        from vgi.serve import _resolve_oauth_resource_metadata

        with pytest.raises(SystemExit):
            _resolve_oauth_resource_metadata()

    def test_with_optional_fields(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Optional scopes and resource name are passed through."""
        monkeypatch.setenv("VGI_OAUTH_RESOURCE", "https://api.example.com")
        monkeypatch.setenv("VGI_OAUTH_AUTH_SERVERS", "https://auth1.example.com,https://auth2.example.com")
        monkeypatch.setenv("VGI_OAUTH_SCOPES", "read,write")
        monkeypatch.setenv("VGI_OAUTH_RESOURCE_NAME", "My API")
        from vgi.serve import _resolve_oauth_resource_metadata

        result = _resolve_oauth_resource_metadata()
        assert result is not None
        assert len(result.authorization_servers) == 2
        assert result.scopes_supported == ("read", "write")
        assert result.resource_name == "My API"

    def test_client_id_passed_through(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """VGI_OAUTH_CLIENT_ID is forwarded to OAuthResourceMetadata."""
        monkeypatch.setenv("VGI_OAUTH_RESOURCE", "https://api.example.com")
        monkeypatch.setenv("VGI_OAUTH_AUTH_SERVERS", "https://auth.example.com")
        monkeypatch.setenv("VGI_OAUTH_CLIENT_ID", "my-client-id")
        from vgi.serve import _resolve_oauth_resource_metadata

        result = _resolve_oauth_resource_metadata()
        assert result is not None
        assert result.client_id == "my-client-id"

    def test_client_id_absent_is_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Omitting VGI_OAUTH_CLIENT_ID leaves client_id as None."""
        monkeypatch.setenv("VGI_OAUTH_RESOURCE", "https://api.example.com")
        monkeypatch.setenv("VGI_OAUTH_AUTH_SERVERS", "https://auth.example.com")
        monkeypatch.delenv("VGI_OAUTH_CLIENT_ID", raising=False)
        from vgi.serve import _resolve_oauth_resource_metadata

        result = _resolve_oauth_resource_metadata()
        assert result is not None
        assert result.client_id is None

    def test_invalid_client_id_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Invalid client_id causes SystemExit with friendly message."""
        monkeypatch.setenv("VGI_OAUTH_RESOURCE", "https://api.example.com")
        monkeypatch.setenv("VGI_OAUTH_AUTH_SERVERS", "https://auth.example.com")
        monkeypatch.setenv("VGI_OAUTH_CLIENT_ID", 'bad "id')
        from vgi.serve import _resolve_oauth_resource_metadata

        with pytest.raises(SystemExit):
            _resolve_oauth_resource_metadata()

    def test_client_secret_passed_through(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """VGI_OAUTH_CLIENT_SECRET is forwarded to OAuthResourceMetadata."""
        monkeypatch.setenv("VGI_OAUTH_RESOURCE", "https://api.example.com")
        monkeypatch.setenv("VGI_OAUTH_AUTH_SERVERS", "https://auth.example.com")
        monkeypatch.setenv("VGI_OAUTH_CLIENT_SECRET", "my-client-secret")
        monkeypatch.delenv("VGI_OAUTH_CLIENT_ID", raising=False)
        from vgi.serve import _resolve_oauth_resource_metadata

        result = _resolve_oauth_resource_metadata()
        assert result is not None
        assert result.client_secret == "my-client-secret"

    def test_client_secret_absent_is_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Omitting VGI_OAUTH_CLIENT_SECRET leaves client_secret as None."""
        monkeypatch.setenv("VGI_OAUTH_RESOURCE", "https://api.example.com")
        monkeypatch.setenv("VGI_OAUTH_AUTH_SERVERS", "https://auth.example.com")
        monkeypatch.delenv("VGI_OAUTH_CLIENT_SECRET", raising=False)
        from vgi.serve import _resolve_oauth_resource_metadata

        result = _resolve_oauth_resource_metadata()
        assert result is not None
        assert result.client_secret is None

    def test_invalid_client_secret_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Invalid client_secret causes SystemExit."""
        monkeypatch.setenv("VGI_OAUTH_RESOURCE", "https://api.example.com")
        monkeypatch.setenv("VGI_OAUTH_AUTH_SERVERS", "https://auth.example.com")
        monkeypatch.setenv("VGI_OAUTH_CLIENT_SECRET", 'bad "secret')
        from vgi.serve import _resolve_oauth_resource_metadata

        with pytest.raises(SystemExit):
            _resolve_oauth_resource_metadata()

    def test_use_id_token_as_bearer_true(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """VGI_OAUTH_USE_ID_TOKEN=1 sets use_id_token_as_bearer."""
        monkeypatch.setenv("VGI_OAUTH_RESOURCE", "https://api.example.com")
        monkeypatch.setenv("VGI_OAUTH_AUTH_SERVERS", "https://auth.example.com")
        monkeypatch.setenv("VGI_OAUTH_USE_ID_TOKEN", "1")
        from vgi.serve import _resolve_oauth_resource_metadata

        result = _resolve_oauth_resource_metadata()
        assert result is not None
        assert result.use_id_token_as_bearer is True

    def test_use_id_token_as_bearer_false_by_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Omitting VGI_OAUTH_USE_ID_TOKEN leaves use_id_token_as_bearer as False."""
        monkeypatch.setenv("VGI_OAUTH_RESOURCE", "https://api.example.com")
        monkeypatch.setenv("VGI_OAUTH_AUTH_SERVERS", "https://auth.example.com")
        monkeypatch.delenv("VGI_OAUTH_USE_ID_TOKEN", raising=False)
        from vgi.serve import _resolve_oauth_resource_metadata

        result = _resolve_oauth_resource_metadata()
        assert result is not None
        assert result.use_id_token_as_bearer is False


# ---------------------------------------------------------------------------
# Import tests
# ---------------------------------------------------------------------------


class TestAuthImports:
    """Tests for auth-related imports."""

    def test_auth_module_imports(self) -> None:
        """vgi.auth imports core types."""
        from vgi.auth import AuthContext, CallContext

        assert AuthContext is not None
        assert CallContext is not None

    def test_authcontext_in_vgi_init(self) -> None:
        """AuthContext and Auth are accessible from vgi top-level."""
        from vgi import Auth, AuthContext, CallContext

        assert Auth is not None
        assert AuthContext is not None
        assert CallContext is not None

    def test_auth_annotation_in_arguments(self) -> None:
        """Auth annotation is in vgi.arguments."""
        from vgi.arguments import Auth

        assert Auth is not None


# ---------------------------------------------------------------------------
# Duplicate Auth annotation tests
# ---------------------------------------------------------------------------


class TestDuplicateAuth:
    """Tests for duplicate Auth parameter validation."""

    def test_duplicate_auth_raises(self) -> None:
        """Defining two Auth params raises TypeError."""
        with pytest.raises(TypeError, match="multiple Auth parameters"):

            class _BadFunction(ScalarFunction):
                class Meta:
                    name = "bad_dual_auth"

                @classmethod
                def compute(
                    cls,
                    x: Annotated[pa.Int64Array, Param(doc="input")],
                    auth1: Annotated[AuthContext, Auth()],
                    auth2: Annotated[AuthContext, Auth()],
                ) -> Annotated[pa.Int64Array, Returns()]:
                    return x


# ---------------------------------------------------------------------------
# Bearer token format tests
# ---------------------------------------------------------------------------


class TestBearerTokenFormat:
    """Tests for bearer token parsing edge cases."""

    def test_bearer_token_with_equals_in_principal(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Principal containing '=' (e.g. base64) is preserved correctly."""
        monkeypatch.setenv("VGI_BEARER_TOKENS", "mytoken=base64data==")
        monkeypatch.delenv("VGI_JWT_ISSUER", raising=False)
        from vgi.serve import _resolve_bearer_authenticate

        result = _resolve_bearer_authenticate()
        assert result is not None
        # The callback was built — the split(=, 1) preserves base64 principal


# ---------------------------------------------------------------------------
# InitParams auth threading tests
# ---------------------------------------------------------------------------


class TestInitParamsAuth:
    """Tests for auth context threading through global_init()."""

    def test_table_function_global_init_passes_auth(self) -> None:
        """global_init() threads auth from ctx into InitParams."""
        from vgi.auth import CallContext
        from vgi.table_function import TableFunctionGenerator

        captured_auth: list[AuthContext] = []

        class _CaptureAuthFunc(TableFunctionGenerator[None]):
            class Meta:
                name = "capture_auth"

            @classmethod
            def on_init(cls, params: Any) -> GlobalInitResponse:
                captured_auth.append(params.auth_context)
                return GlobalInitResponse()

            @classmethod
            def process(cls, params: Any, state: None, out: OutputCollector) -> None:
                pass

        from vgi.arguments import Arguments
        from vgi.protocol import BindRequest, InitRequest

        bind_call = BindRequest(
            function_name="capture_auth",
            arguments=Arguments(),
            function_type=FunctionType.TABLE,
        )
        init_req = InitRequest(
            bind_call=bind_call,
            output_schema=pa.schema([]),
        )
        auth = AuthContext(principal="test-user", authenticated=True, domain="bearer")
        ctx = CallContext(auth=auth, emit_client_log=lambda *a, **kw: None)

        _CaptureAuthFunc.global_init(init_req, ctx=ctx)

        assert len(captured_auth) == 1
        assert captured_auth[0].principal == "test-user"
        assert captured_auth[0].domain == "bearer"

    def test_table_function_global_init_defaults_anonymous(self) -> None:
        """global_init() without ctx defaults to anonymous auth."""
        from vgi.table_function import TableFunctionGenerator

        captured_auth: list[AuthContext] = []

        class _AnonAuthFunc(TableFunctionGenerator[None]):
            class Meta:
                name = "anon_auth"

            @classmethod
            def on_init(cls, params: Any) -> GlobalInitResponse:
                captured_auth.append(params.auth_context)
                return GlobalInitResponse()

            @classmethod
            def process(cls, params: Any, state: None, out: OutputCollector) -> None:
                pass

        from vgi.arguments import Arguments
        from vgi.protocol import BindRequest, InitRequest

        bind_call = BindRequest(
            function_name="anon_auth",
            arguments=Arguments(),
            function_type=FunctionType.TABLE,
        )
        init_req = InitRequest(
            bind_call=bind_call,
            output_schema=pa.schema([]),
        )

        _AnonAuthFunc.global_init(init_req)

        assert len(captured_auth) == 1
        assert captured_auth[0].authenticated is False
        assert captured_auth[0].principal is None
