"""Writable test-fixture worker and helpers.

These fixtures depend on ``sqlglot`` (via ``vgi.transactor``) and live behind
the ``vgi[test-fixtures-writable]`` extra. Tests that exercise the write
subsystem (INSERT/UPDATE/DELETE/DDL) import from here.
"""
