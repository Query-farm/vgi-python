# Transactor

!!! note "Advanced — reference only"
    The transactor is an advanced feature without a dedicated how-to guide yet. This page is the
    API reference; start from the [tutorial](../tutorial/index.md) and
    [function patterns](../how-to/function-patterns.md) if you're new to VGI.

The transactor is a long-lived subprocess that gives worker functions transactional access to a
database, mediated by `TransactorClient` over the `TransactorProtocol`. Requires
`pip install vgi-python[transactor]`.

::: vgi.transactor
