# Transactor

The transactor is a long-lived subprocess that gives worker functions transactional access to a
database, mediated by `TransactorClient` over the `TransactorProtocol`. Requires
`pip install vgi-python[transactor]`.

::: vgi.transactor
