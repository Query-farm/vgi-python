---
description: "How to connect the Python VGI client to workers through Tailscale over HTTPS, direct TCP, or userspace SOCKS5h."
---

# Run VGI through a Tailnet

The VGI client uses ordinary HTTPS and TCP endpoints. It does not join a
Tailnet, manage Tailscale credentials, or introduce a `tailscale://` URL
scheme. Run Tailscale on the client host or in a sidecar and pass the worker's
MagicDNS name to the normal client factory.

Connectivity and authentication are separate. A client can reach a worker
through a Tailnet without the worker using Tailnet identity. To authorize from
the caller's verified Tailscale user, node, tags, or application capabilities,
configure a peer-identity provider and an authentication policy on the worker.

## Authenticate an HTTP worker

When Tailscale Serve terminates HTTPS, trust its identity headers only from the
exact local proxy addresses that can reach the application. `require_peer_identity`
requires verified evidence but deliberately does not invent a stable principal
for capability-only or login-scoped Serve requests:

```python test="lint"
from vgi_rpc.http import tailscale_serve_header_provider
from vgi_rpc.rpc import require_peer_identity

from my_worker import MyWorker
from vgi.serve import create_app

tailscale = tailscale_serve_header_provider(
    issuer="tailnet:example",
    trusted_proxy_addresses=("127.0.0.1", "::1"),
)

app = create_app(
    MyWorker,
    peer_identity_providers=(tailscale,),
    peer_authentication_policy=require_peer_identity("tailscale"),
)
```

Serve login names have `subject_stability="login"`; tagged clients may be
capability-only and have no subject at all. Consequently, the built-in
`peer_identity_primary("tailscale")` policy is intentionally unsuitable for
Serve. If a deployment elects to authenticate from a Serve login, it must use
an explicit application policy that accepts that weaker identity and scopes it
to the configured issuer and trusted proxy boundary.

For direct HTTP over a Tailnet, LocalAPI WhoIs can produce a stable user or
node subject and therefore can be the primary authenticator:

```python test="lint"
from vgi_rpc.http import tailscale_localapi_provider
from vgi_rpc.rpc import peer_identity_primary

from my_worker import MyWorker
from vgi.serve import create_app

tailscale = tailscale_localapi_provider(issuer="tailnet:example")
app = create_app(
    MyWorker,
    peer_identity_providers=(tailscale,),
    peer_authentication_policy=peer_identity_primary("tailscale"),
)
```

`create_app()` remains a standard WSGI application, so the same configuration
works behind Waitress, Granian, gunicorn, Envoy, nginx, or a cloud HTTP load
balancer. The provider determines which proxy evidence is trusted; merely
placing the worker behind a proxy never makes forwarded identity trustworthy.

Trusted-proxy validation must see the physical socket peer. Do not install
`ProxyFix`, forwarded-address middleware, or an equivalent rewrite ahead of
VGI's identity middleware: attacker-controlled `Forwarded` or
`X-Forwarded-For` values must never become `REMOTE_ADDR` before the exact proxy
allowlist is checked. Keep the backend unreachable except through those
allowlisted proxies.

## HTTPS through Tailscale Serve

On a host using Tailscale's normal network interface, connect to a worker
published with Tailscale Serve exactly like any other HTTPS worker:

```python test="lint"
from vgi.client import Client

with Client.from_http("https://worker.example-tailnet.ts.net") as client:
    catalogs = client.catalogs()
```

Tailscale terminates HTTPS and adds its verified Serve identity and application
capability headers. The VGI server validates those headers against its trusted
proxy configuration; the client neither constructs nor trusts identity
headers. Do not send `Tailscale-*` headers from application code.

For userspace networking, inject an HTTP client configured for Tailscale's
SOCKS5 server. Install SOCKS support with `pip install 'httpx2[socks]'`:

```python test="lint"
import httpx2

from vgi.client import Client

url = "https://worker.example-tailnet.ts.net"
with httpx2.Client(
    base_url=url,
    proxy="socks5h://127.0.0.1:1055",
    trust_env=False,
    timeout=httpx2.Timeout(60.0, connect=15.0),
) as http_client, Client.from_http(url, httpx_client=http_client) as client:
    catalogs = client.catalogs()
```

Using `socks5h` keeps MagicDNS resolution inside the Tailscale sidecar. A proxy
failure is an error; this configuration does not fall back to a direct route.

## Raw TCP through a Tailnet

Start a high-level Worker with LocalAPI identity using `Worker.serve_tcp()`:

```python test="lint"
from vgi_rpc.http import tailscale_localapi_provider
from vgi_rpc.rpc import peer_identity_primary

from my_worker import MyWorker

tailscale = tailscale_localapi_provider(issuer="tailnet:example")
MyWorker().serve_tcp(
    "0.0.0.0",
    9400,
    threaded=True,
    max_connections=64,
    peer_identity_providers=(tailscale,),
    peer_authentication_policy=peer_identity_primary("tailscale"),
)
```

The connection's verified stable Tailnet subject becomes `CallContext.auth` and
is available to function parameters annotated with `Auth()`. Identity is
snapshotted once for the stateful TCP connection.

When `threaded=True`, all connections share the same Worker instance. Protect
mutable subclass state with appropriate synchronization and never store
per-caller authentication in shared instance attributes. Leave the default
serial mode in place if the implementation is not safe for concurrent calls.

Behind a Tailscale Service configured to emit PROXY protocol v2, require the
preamble and name every immediate proxy address explicitly:

```python test="lint"
MyWorker().serve_tcp(
    "127.0.0.1",
    9400,
    proxy_protocol="required",
    trusted_proxy_addresses=("127.0.0.1", "::1"),
    service_name="svc:vgi-worker",
    peer_identity_providers=(tailscale,),
    peer_authentication_policy=peer_identity_primary("tailscale"),
)
```

Keep that backend unreachable except through the trusted proxy. The asserted
source is accepted only after immediate-peer validation, and `service_name`
selects destination-scoped application capabilities in LocalAPI WhoIs.

Direct TCP works with a MagicDNS name when the client host has Tailscale's
normal network interface:

```python test="lint"
from vgi.client import Client

with Client.from_tcp("worker.example-tailnet.ts.net", 9400) as client:
    catalogs = client.catalogs()
```

For a userspace Tailscale sidecar, use the explicit SOCKS5h option:

```python test="lint"
with Client.from_tcp(
    "worker.example-tailnet.ts.net",
    9400,
    proxy="socks5h://127.0.0.1:1055",
) as client:
    catalogs = client.catalogs()
```

The proxy applies to ordinary function calls, additional worker connections,
and catalog discovery. Target hostnames are passed untouched to SOCKS5h and
are never resolved locally by VGI.

Raw VGI TCP has no application-layer encryption. Tailscale encrypts the
Tailnet path, but the worker port should remain reachable only through the
Tailnet and should not be exposed directly to an untrusted network. Prefer
HTTPS through Tailscale Serve at public or mixed-trust boundaries.
