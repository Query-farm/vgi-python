# Metadata & Protocol

Functions describe themselves through metadata — stability, examples, parameter info, ordering and
null semantics — which DuckDB reads for introspection and the query optimizer. The protocol and
invocation types model the request/response lifecycle on the wire. See the
[Metadata](../metadata.md) guide for authoring metadata via nested `Meta` classes.

## Metadata

::: vgi.metadata

## Invocation lifecycle

::: vgi.invocation

## Protocol

::: vgi.protocol.CatalogAttachRequest.client_capabilities
    options:
      heading_level: 3

::: vgi.protocol
