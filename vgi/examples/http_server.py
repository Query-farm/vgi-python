"""Run the example worker as an HTTP server.

Usage::

    vgi-example-http
    vgi-example-http --port 9000
    vgi-example-http --host 0.0.0.0 --port 8080 --debug

Requires the ``http`` extra: ``pip install vgi[http]``
"""

from vgi.examples.worker import ExampleWorker


def main() -> None:
    """Run the example worker as an HTTP server."""
    ExampleWorker.main_http()


if __name__ == "__main__":
    main()
