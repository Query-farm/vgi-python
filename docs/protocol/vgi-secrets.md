# VGI Secrets

**Status:** Normative. It records the contract the DuckDB extension and the SDKs already implement.
**Protocol:** `vgi.v2`. No wire change.
**Reference implementations:** client, `BuildSecretsBatch` and `ExtractVgiSecrets` in the DuckDB
extension; worker, `SecretsAccessor` and `ResolvedSecrets` in vgi-python `vgi/table_function.py`.

The words MUST, MUST NOT and SHOULD are used as in RFC 2119.

## 1. Purpose

A function reads credentials that the user keeps in the client's secret store (in DuckDB,
`CREATE SECRET`). The worker never reads that store. It says which secrets it needs, the client
resolves them, and the resolved values travel to the worker inside the bind request.

Two things are specified here: how a worker asks for a secret, and the layout of the batch that
carries the answers.

## 2. Asking for a secret

A lookup is a triple:

| Field | Meaning |
|---|---|
| `secret_type` | Required. The secret type, such as `s3`. |
| `secret_name` | Optional. The name of one specific secret. |
| `scope` | Optional. A path the secret must apply to, such as `s3://bucket/key`. |

A worker asks in one of two ways.

**Statically.** Function metadata lists lookups in `required_secrets`. The client resolves them
before the first bind, so they are in the first bind request.

**Dynamically.** A bind may need a lookup it can only compute from its arguments, such as the bucket
in a path argument. The worker answers the bind with a *secret-scope request*: a `BindResponse`
whose `lookup_secret_types` list is non-empty. `lookup_scopes` and `lookup_names` are parallel
lists of the same length, with the empty string where a lookup has no scope or no name. The other
fields of that response are ignored.

The client resolves the requested lookups, adds them to the secrets it already resolved, and sends
the bind again with `resolved_secrets_provided = true`. A worker MUST NOT answer that second bind
with another secret-scope request; the client rejects it. A lookup that is still unanswered on the
second bind does not exist.

## 3. How the client resolves a lookup

- **With a name:** the secret of that name, if its type equals `secret_type`. The scope is ignored.
- **Without a name:** the secret of that type whose scope best matches `scope` under the client's
  own rule. DuckDB uses the longest scope that is a prefix of the path. An empty `scope` matches
  only secrets that have no scope.

A lookup that matches nothing is not an error for the client. The secret is left out of the batch
and the worker decides what that means.

A secret type that sets a default scope is not matched by a lookup with an empty scope. DuckDB's
`s3` secrets default to the scope `s3://`, so a worker looking for "the user's S3 secret" asks for
scope `s3://`, not for the type alone.

## 4. The secrets batch

`BindRequest.secrets` is a binary field. It holds an Arrow IPC stream with one record batch, or is
null when no secret was resolved.

The batch has **one row** and **one column per resolved secret**:

- The column is **named by the secret's name**, the name the user gave it or the client's default
  name. It is not named by the secret's type. Several secrets of one type therefore coexist, one
  per bucket for example.
- The column is a **struct**. Its fields are the secret's own key/value pairs, each with the Arrow
  type of its value, plus the four fields below.
- A secret that was resolved by more than one lookup appears once.

| Field | Type | Content |
|---|---|---|
| `type` | utf8 | The secret type. |
| `provider` | utf8 | The provider that created the secret, such as `config` or `credential_chain`. |
| `name` | utf8 | The secret name. Equal to the column name. |
| `scope` | utf8 | The secret's scope prefixes joined by a newline (`\n`). Empty when it has none. |

Example, for two `s3` secrets named `prod` and `dev`:

```text
prod: struct<key_id: utf8, name: utf8, provider: utf8, region: utf8, scope: utf8, secret: utf8, type: utf8>
dev:  struct<key_id: utf8, name: utf8, provider: utf8, region: utf8, scope: utf8, secret: utf8, type: utf8>
```

Rules:

1. A client MUST name each column by the secret name and MUST include `type`, `name` and `scope`.
2. A worker MUST find a secret by column name or by the `type` and `scope` fields. It MUST NOT
   depend on column order, on column position, or on Arrow field metadata.
3. A worker MUST accept struct fields it does not know, and MUST treat a null column as an
   unresolved secret.
4. The batch does not say which lookup produced which secret. A worker matches them itself (§5).
5. The batch contains credentials in plain text. Neither side may log it or copy it into
   `opaque_data`.

The same batch is sent wherever a bind request is sent or embedded, which includes the bind request
carried inside each init request. A function therefore sees the same secrets at execution time as
at bind time.

## 5. How the worker finds its secret

A worker selects from the batch with the rule the client used, so that it picks the secret its own
lookup resolved:

- **By name:** the column of that name, if its `type` field equals the requested type.
- **By scope:** among secrets of the requested type, the one with the longest scope prefix of the
  requested path; failing that, one with an empty scope.
- **By type alone:** a secret of that type with an empty scope; failing that, the first secret of
  that type.

SDKs expose these as selectors on the resolved-secrets object.

| SDK | By type | By scope and type |
|---|---|---|
| Python | `ResolvedSecrets.of_type`, `.lookup` | `.for_scope_of_type`, `.lookup` |
| Rust | `Secrets::of_type` | `Secrets::for_scope_of_type` |
| Go | `Secrets.OfType` | `Secrets.ForScopeOfType` |
| C++ | `Secrets::of_type` | `Secrets::for_scope_of_type` |
| Java | `Secrets.ofType` | `Secrets.forScopeOfType` |
| C# | `SecretArgCodec.AllOfType` | `SecretArgCodec.ForScopeOfType` |
| TypeScript | `secretsOfType` | `secretForScopeOfType` |

## 6. History

vgi-python's bind-time `SecretsAccessor` used to expect dynamically requested secrets in columns
named `secret_0`, `secret_1`, … with the lookup's type, name and scope in Arrow field metadata. No
client sends that layout, so `params.secrets.get(type, name=…)` and `get(type, scope=…)` returned
`None` on the second bind although the secret was in the batch. The accessor now reads the layout
in §4, and `secret_0` is an ordinary secret name.
