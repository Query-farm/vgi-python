# Client

The Python `Client` spawns or connects to a worker, streams Arrow data to and from its functions,
and surfaces errors as `ClientError`. It is the pure-Python counterpart to the DuckDB extension.

## Discovering hosted protocols

Every worker hosts the vgi-rpc reflection protocol, `vgi_rpc.Reflection.v1`, on every transport.
`Client.list_protocols()` asks it what the worker hosts, in the worker's order: `vgi.v2` first,
then any protocols the worker adds through `Worker.hosted_protocols()`, then the framework's own.
Each entry is a `HostedProtocol` (vgi-rpc's, re-exported from `vgi.client`) with `name`,
`version`, `hash`, `deprecated`, `deprecation_message` and `features`. The hash is a SHA-256 hex digest of the
protocol's canonical description, so two equal hashes mean the same method surface.
`Client.describe_protocol(name)` returns one protocol's methods and schemas as a
`vgi_rpc.ServiceDescription`. Both are thin wrappers over vgi-rpc's `list_protocols(proxy)` /
`describe_protocol(proxy, name)` on the client's primary connection.

```python test="skip"
from vgi.client import Client

with Client("vgi-fixture-worker") as client:
    hosted = {p.name for p in client.list_protocols()}
    if "conformance.Secondary.v1" in hosted:
        print(client.describe_protocol("conformance.Secondary.v1"))
```

A worker that does not host reflection (one built before it existed answers `UNIMPLEMENTED` /
`protocol_not_supported`, which vgi-rpc raises as `ReflectionNotSupportedError`) is not an
error for `list_protocols()`. It returns the single entry
`HostedProtocol("vgi.v2", "", "")`, because `vgi.v2` is the one protocol every worker hosts. The
empty hash marks the entry as inferred rather than reported.

Both calls use the client's primary connection. Do not call them while a stream is open on
the same client.

## API reference

::: vgi.client
