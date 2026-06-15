# Functions

VGI exposes four function patterns. Pick the one that matches how your data flows:

| Pattern | Shape | Base class |
|---|---|---|
| **Scalar** | 1 row in → 1 row out | [`ScalarFunction`](#vgi.scalar_function.ScalarFunction) / `ScalarFunctionGenerator` |
| **Table** | no input → rows out | [`TableFunctionGenerator`](#vgi.table_function.TableFunctionGenerator) |
| **Table-in-out** | rows in → rows out (streaming) | [`TableInOutFunction`](#vgi.table_in_out_function.TableInOutFunction) / `TableInOutGenerator` |
| **Aggregate** | grouped rows → one row per group | [`AggregateFunction`](#vgi.aggregate_function.AggregateFunction) |

All four ultimately derive from the shared [`Function`](#vgi.function.Function) base.

## Scalar functions

::: vgi.scalar_function

## Table functions

::: vgi.table_function

## Table-in-out functions

::: vgi.table_in_out_function

## Aggregate functions

::: vgi.aggregate_function

## Function base

::: vgi.function
