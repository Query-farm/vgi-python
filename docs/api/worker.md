# Worker & Serving

A `Worker` hosts your functions (and optional catalog) in a separate process. It speaks the VGI
protocol over Arrow IPC — either stdin/stdout (subprocess transport) or HTTP. The `vgi-serve`
CLI (`vgi.serve`) is the zero-boilerplate entry point for running one.

```python
from vgi import Worker, ScalarFunction

class MyWorker(Worker):
    functions = [MyScalarFunction()]

# vgi-serve my_module:MyWorker            # stdio
# vgi-serve my_module:MyWorker --http     # HTTP
```

## Hosting additional protocols

A worker can serve other [vgi-rpc](https://vgi-rpc.query.farm) protocols from the same process and
the same listener as the VGI protocol. Override
[`Worker.hosted_protocols`][vgi.worker.Worker.hosted_protocols] to return
`(protocol, implementation)` pairs:

```python
from collections.abc import Sequence
from typing import ClassVar, Protocol

from vgi import Worker


class Audit(Protocol):
    protocol_name: ClassVar[str] = "acme.Audit.v1"

    def record(self, event: str) -> int: ...


class AuditImpl:
    def record(self, event: str) -> int:
        return len(event)


class MyWorker(Worker):
    functions = []

    @classmethod
    def hosted_protocols(cls) -> Sequence[tuple[type, object]]:
        return [(Audit, AuditImpl())]
```

The rules:

- **Every transport hosts the same list.** stdin/stdout, AF_UNIX (or a Windows named pipe), TCP,
  the Iroh raw upstream and HTTP all build their server through one function,
  [`build_rpc_server`][vgi.rpc_server.build_rpc_server].
- **Called once per server.** The hook may read configuration or the environment, but its answer
  is fixed for the life of the process, so reflection output and protocol hashes stay stable.
- **The protocol is the unit of optionality.** There is no way to host part of a protocol. Make
  an optional capability its own protocol, and include it or leave it out.
- **Names are routing keys.** Each protocol needs a distinct `protocol_name`. A repeated name, the
  worker's own `vgi.v2`, or the reserved `vgi_rpc.` prefix stops the worker at startup, and the
  error names `hosted_protocols()`.
- **`vgi.v2` is unaffected.** vgi-rpc routes each request on its `vgi_rpc.protocol` key and never
  falls back to the primary protocol. A client that only speaks `vgi.v2`, such as the DuckDB
  extension, sees exactly the behaviour it saw before.

A `MetaWorker` (`vgi.meta_worker`) hosts the concatenation of its children's lists, in
child order. Two children that list the same protocol name stop it at startup. Over HTTP it
hosts Identity for the one child that overrides `resolve_token` or `mint_grant`; two children
overriding the same hook stop it at startup.

### What each transport hosts

Reflection (`list_protocols`) reports protocols in this order:

| Transport | `vgi.v2` | `hosted_protocols()` | `vgi_rpc.Reflection.v1` | `vgi_rpc.Identity.v1` |
| --- | --- | --- | --- | --- |
| stdin/stdout | yes | yes | yes | no |
| AF_UNIX / named pipe | yes | yes | yes | no |
| TCP | yes | yes | yes | no |
| Iroh raw upstream | yes | yes | yes | no |
| HTTP | yes | yes | unless `--no-describe` | when `resolve_token` or `mint_grant` is overridden |

Identity stays framework-owned. You enable it by overriding
[`resolve_token`][vgi.worker.Worker.resolve_token] or
[`mint_grant`][vgi.worker.Worker.mint_grant], never through `hosted_protocols()`. It is hosted
only on HTTP, which authenticates callers. Its introspection allowlist names principals, and the
other transports have no principals to check.

`vgi.attach_tickets.v1` is framework-owned too. It is hosted on HTTP only, and only when
`VGI_SIGNING_KEY` is configured explicitly and the worker can issue grants (grant keys, or its own
`mint_grant`). See [Attach tickets](../protocol/vgi-attach-tickets.md).

## Worker

::: vgi.worker

## Server construction

::: vgi.rpc_server

## Serving

::: vgi.serve

## Attach tickets

::: vgi.attach_ticket
