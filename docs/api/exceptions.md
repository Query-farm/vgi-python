# Exceptions

VGI raises typed exceptions for binding, catalog, schema, and execution errors. Several
validation errors also live alongside the features they guard (see
[Arguments](arguments.md#vgi.arguments.ArgumentValidationError) and
[Metadata](metadata.md)).

## Error codes

Errors cross the wire with a gRPC-style `error_code` from vgi-rpc's error model
(`vgi_rpc.errors.Code`). An exception class declares its code with an
`error_code` class attribute; one without it is sent as `UNKNOWN`. The SDK codes
its own errors so a client can tell bad input from a worker bug:

| Code | Raised for |
|------|------------|
| `INVALID_ARGUMENT` | `ArgumentValidationError`, `ArgumentTypeError` (failed `type_bound`), `TypeMismatchError` on an input parameter, `InvalidArgumentError` (no matching overload, ambiguous call, wrong call shape), `MissingAttachOptionsError` |
| `NOT_FOUND` | `NotFoundError` (unknown function, catalog, or table) |
| `FAILED_PRECONDITION` | `CatalogReadOnlyError` |
| `UNIMPLEMENTED` | `UnsupportedOperationError` (catalog operations a catalog does not implement) |
| `UNKNOWN` | everything else, including `SchemaValidationError` on a worker's output and `TypeMismatchError` on a return value -- those are worker bugs |

Your own exceptions can declare a code the same way:

```python
from typing import ClassVar

from vgi_rpc.errors import Code


class QuotaExceededError(Exception):
    error_code: ClassVar[Code] = Code.RESOURCE_EXHAUSTED
```

::: vgi.exceptions
