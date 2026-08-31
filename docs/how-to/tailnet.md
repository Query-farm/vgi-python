---
description: "How to connect the Python VGI client to workers through Tailscale over HTTPS, direct TCP, or userspace SOCKS5h."
---

# Connect through a Tailnet

The VGI client uses ordinary HTTPS and TCP endpoints. It does not join a
Tailnet, manage Tailscale credentials, or introduce a `tailscale://` URL
scheme. Run Tailscale on the client host or in a sidecar and pass the worker's
MagicDNS name to the normal client factory.

## HTTPS through Tailscale Serve

On a host using Tailscale's normal network interface, connect to a worker
published with Tailscale Serve exactly like any other HTTPS worker:

```python
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

```python
import httpx2

from vgi.client import Client

url = "https://worker.example-tailnet.ts.net"
with httpx2.Client(
    base_url=url,
    proxy="socks5h://127.0.0.1:1055",
    trust_env=False,
    timeout=httpx2.Timeout(60.0, connect=15.0),
) as http_client:
    with Client.from_http(url, httpx_client=http_client) as client:
        catalogs = client.catalogs()
```

Using `socks5h` keeps MagicDNS resolution inside the Tailscale sidecar. A proxy
failure is an error; this configuration does not fall back to a direct route.

## Raw TCP through a Tailnet

Direct TCP works with a MagicDNS name when the client host has Tailscale's
normal network interface:

```python
from vgi.client import Client

with Client.from_tcp("worker.example-tailnet.ts.net", 9400) as client:
    catalogs = client.catalogs()
```

For a userspace Tailscale sidecar, use the explicit SOCKS5h option:

```python
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
