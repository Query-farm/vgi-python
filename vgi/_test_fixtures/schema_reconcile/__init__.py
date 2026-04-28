"""Self-contained schema-reconciliation test fixture.

Exposes one writable virtual table whose declared Arrow schema is
deliberately quirky:

  - ``id`` int64 NOT NULL
  - ``ts`` timestamp[ms, tz=UTC] NOT NULL
  - ``nested`` struct{a int32 NOT NULL, b string, ts2 timestamp[ms, tz=UTC]} NOT NULL
  - ``tags`` list<item: struct<k string NOT NULL, v binary>> NOT NULL
  - ``rowid`` int64 NOT NULL (DuckDB pseudocolumn)

Every facet of this schema (top-level NOT NULL primitives, TZ-aware
millisecond timestamps, NOT NULL leaves inside structs and lists) is
something DuckDB's ``ArrowConverter::ToArrowSchema`` cannot preserve on
the round trip — so the C++ ``ReconcileBatchToSchema`` helper inside the
vgi extension must reshape/cast every batch DuckDB hands the worker.

The INSERT, UPDATE, and DELETE handlers assert on the exact Arrow schema
of each batch they receive. If reconciliation drops a flag or fails to
cast a timestamp, the test fixture raises a loud ``ValueError`` rather
than silently storing wrong data — which is exactly what we want a
regression to look like.

The fixture is dependency-free: no transactor, no DuckDB, no SQLite.
Storage is a process-local dict keyed by rowid. Concurrency is not a
concern (one worker process per test).
"""
