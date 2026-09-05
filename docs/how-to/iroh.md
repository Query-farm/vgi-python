# Serve and call a worker over Iroh

VGI supports two authenticated Iroh endpoint forms:

- `iroh://<endpoint-id>` keeps stream state on one connection.
- `httpi://<endpoint-id>[/prefix]` preserves VGI's stateless HTTP exchanges,
  continuation tokens, request/response budgets, authentication headers, and
  externalized batches.

Install the optional native client binding:

```console
pip install 'vgi-python[iroh]'
```

## Start a raw upstream

The language-neutral `vgi-iroh-bridge` owns the public Iroh endpoint. The
Python process binds only loopback and requires the bridge's PROXY-v2 identity
preamble:

```console
vgi-serve my_worker.py \
  --iroh-raw-upstream 127.0.0.1:9400 \
  --iroh-issuer production
```

In another process:

```console
vgi-iroh-bridge \
  --secret-key-file /run/secrets/vgi-iroh-key \
  --raw-upstream tcp://127.0.0.1:9400
```

The bridge prints its EndpointId as the first stdout line. Development may use
`--ephemeral`; a production endpoint must use a persistent secret key.
The raw Iroh listener also enables standard RPC method description so
standalone clients in other VGI languages can bootstrap directly.

## Start an HTTP upstream

HTTP-over-Iroh is the preferred mode for horizontally scaled workers:

```console
vgi-serve my_worker.py \
  --http --host 127.0.0.1 --port 9401 \
  --iroh-issuer production

vgi-iroh-bridge \
  --secret-key-file /run/secrets/vgi-iroh-key \
  --http-upstream http://127.0.0.1:9401
```

`--iroh-issuer` makes the worker trust exactly `127.0.0.1` by default and
promotes the bridge-verified EndpointId to `CallContext.auth`. Use repeated
`--iroh-trusted-proxy <exact-ip>` when the immediate bridge address differs.
Use `--iroh-observe` to expose evidence without authenticating from it.

Never expose the bridge-facing listener as a general public HTTP or TCP
listener. If another proxy sits between the bridge and worker, isolate that
route, clear client-supplied `VGI-Forwarded-Iroh-Endpoint` fields on every other
route, and configure the worker to trust only the intermediary's exact address.

## Connect

The same high-level client accepts both endpoint schemes:

```python test="skip"
from vgi.client import Client

with Client.from_iroh("iroh://<endpoint-id>") as client:
    print(client.catalogs())

with Client.from_iroh("httpi://<endpoint-id>/vgi") as client:
    print(client.catalogs())
```

For a private relay:

```python test="skip"
client = Client.from_iroh(
    "httpi://<endpoint-id>/vgi",
    relay_urls=["https://relay.example.com"],
    remote_relay_url="https://relay.example.com",
)
```

The local relay set and the remote relay hint are distinct. `no_relay=True`
disables relays and requires a working direct path; it never falls back to the
public relay network.
