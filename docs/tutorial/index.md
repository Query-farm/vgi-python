---
description: "Build your first VGI worker — a scalar then a table function callable from DuckDB — in about 20 minutes."
---

# Tutorial: build your first worker

**What this is:** a two-step path from an empty file to custom functions callable from SQL.
**Who it's for:** anyone who wants to extend DuckDB with Python — no prior VGI knowledge assumed.
Budget about **20 minutes** total.

## Prerequisites

- **Python 3.13+** and **[uv](https://docs.astral.sh/uv/)** installed (`uv --version`).
- A terminal. You do **not** need to install vgi-python or DuckDB first, or create a virtualenv —
  `uv run` handles dependencies from the script itself.

??? info "New to DuckDB extensions or Apache Arrow?"
    A **VGI worker** is just a Python process that DuckDB talks to over [Apache
    Arrow](https://arrow.apache.org/) — a columnar in-memory format. Your functions receive and
    return Arrow `RecordBatch`es (columns of data) rather than row-by-row values, which is what
    makes the transfer fast. You don't need to know Arrow deeply to finish this tutorial; we point
    out the few places it matters.

## What you'll build

A worker exposing a catalog named `calc` with two functions, one per step:

1. **[Your first scalar function](scalar.md)** — `double(value)` maps one row to one row
   (`21` → `42`). *(~10 minutes, gets you a working query.)*
2. **[Add a table function](table.md)** — `series(count)` generates `count` rows from an argument.
   *(~10 minutes.)*

Start with step 1 → **[Your first scalar function](scalar.md)**.
