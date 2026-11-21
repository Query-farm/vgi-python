# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Fixture workers for Orchard hosted mode (``VGI_ORCHARD_HOSTED=1``).

Kept out of the main fixture worker so the extension's function-count tests
are unaffected. ``tests/test_orchard_hosted.py`` serves these with
``python -m vgi.serve vgi._test_fixtures.orchard_hosted:<Class> --http``.
"""

from __future__ import annotations

import json
from typing import Annotated

import pyarrow as pa
from vgi_rpc.rpc import AuthContext

from vgi.arguments import Auth, Param, Returns
from vgi.auth import TokenIdentity
from vgi.scalar_function import ScalarFunction
from vgi.worker import Worker


class PassClaimsFunction(ScalarFunction):
    """Return the caller's identity and verified Pass claims as JSON.

    This is the fixture function of the SDK-neutral hosted-mode conformance
    harness (the extension repo's ``test/hosted/README.md``), which every
    SDK's hosted fixture worker implements the same way:
    ``main.pass_claims(x BIGINT) -> VARCHAR``, one output row per input row,
    each row the JSON object ``{"principal": <principal>, "domain": <auth
    domain>, "claims": <the verified claim set>}``.

    Claims keep their JSON types: ``email_verified`` is a boolean and ``exp``
    a number, so the harness can tell a real boolean from the string ``"true"``.

    SQL: ``SELECT pass_claims(1)``
    """

    class Meta:
        """Metadata for the pass_claims function."""

        name = "pass_claims"

    @classmethod
    def compute(
        cls,
        x: Annotated[pa.Int64Array, Param(doc="dummy input")],
        auth: Annotated[AuthContext, Auth()],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Return the verified claims plus the principal and auth domain."""
        payload = json.dumps(
            {"principal": auth.principal, "domain": auth.domain, "claims": dict(auth.claims)},
            sort_keys=True,
        )
        return pa.array([payload] * len(x))


class OrchardHostedWorker(Worker):
    """A minimal hosted-mode worker exposing ``pass_claims``."""

    functions = [PassClaimsFunction]


class ResolveTokenWorker(Worker):
    """Overrides ``resolve_token``: hosted mode must refuse to start."""

    functions = [PassClaimsFunction]

    @classmethod
    def resolve_token(cls, token: str) -> TokenIdentity | None:
        """Never consulted: hosted mode refuses this worker."""
        return None


def main() -> None:
    """Run ``OrchardHostedWorker`` through ``Worker.main`` (``--http``/``--tcp``/``--unix``)."""
    OrchardHostedWorker.main()


if __name__ == "__main__":
    main()
