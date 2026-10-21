"""Canonical fingerprint of the nested catalog wire contract for all SDKs."""

import hashlib
from functools import lru_cache

from vgi_rpc.rpc import rpc_methods
from vgi_rpc.rpc._protocol_hash import canonical_json, compute_protocol_hash
from vgi_rpc.rpc._type_tokens import schema_tokens

from vgi.catalog.contents_codec import CATEGORY_TYPES
from vgi.protocol import CatalogContentsResponse, VgiProtocol


@lru_cache(maxsize=1)
def catalog_contract_hash() -> str:
    """Include nested binary schemas, which the RPC method hash alone cannot see."""
    description = {
        "rpc": compute_protocol_hash(VgiProtocol.protocol_name, rpc_methods(VgiProtocol)),
        "response": schema_tokens(CatalogContentsResponse.ARROW_SCHEMA),
        "categories": {kind: schema_tokens(cls.ARROW_SCHEMA) for kind, cls in CATEGORY_TYPES.items()},
    }
    return hashlib.sha256(b"vgi.catalog.contract.v1|" + canonical_json(description)).hexdigest()
