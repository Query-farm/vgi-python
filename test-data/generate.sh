#!/bin/sh
set -e
duckdb -c "copy (select * as v from range(100000000)) to 'v.parquet';"
