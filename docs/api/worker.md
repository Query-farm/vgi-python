# Worker & Serving

A `Worker` hosts your functions (and optional catalog) in a separate process. It speaks the VGI
protocol over Arrow IPC — either stdin/stdout (subprocess transport) or HTTP. The `vgi-serve`
CLI (`vgi.serve`) is the zero-boilerplate entry point for running one.

```python
from vgi import Worker, ScalarFunction

class MyWorker(Worker):
    functions = [MyScalarFunction()]

# vgi-serve my_module:MyWorker            # stdio
# vgi-serve my_module:MyWorker --http     # HTTP
```

## Worker

::: vgi.worker

## Serving

::: vgi.serve
