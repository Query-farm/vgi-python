#!/usr/bin/env python3
"""Measure VGI worker cold start time and identify bottlenecks.

This script measures:
1. Total time until worker is ready to receive invocations
2. Import time breakdown using Python's importtime feature
3. Detailed profiling of startup phases

Usage:
    uv run python scripts/measure_startup.py
    uv run python scripts/measure_startup.py --importtime
    uv run python scripts/measure_startup.py --profile
"""

import argparse
import os
import re
import subprocess
import sys
import time


def measure_basic_startup(worker_cmd: list[str], num_runs: int = 5) -> list[float]:
    """Measure time until worker prints 'waiting_for_invocation'.

    Returns list of startup times in seconds.
    """
    times = []
    for i in range(num_runs):
        start = time.perf_counter()
        proc = subprocess.Popen(
            worker_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={**os.environ, "VGI_QUIET": "1"},
        )

        # Read stderr until we see "waiting_for_invocation"
        while True:
            line = proc.stderr.readline()
            if not line:
                break
            line_str = line.decode("utf-8", errors="replace")
            if "waiting_for_invocation" in line_str:
                elapsed = time.perf_counter() - start
                times.append(elapsed)
                break

        # Clean up
        proc.stdin.close()
        proc.terminate()
        try:
            proc.wait(timeout=1)
        except subprocess.TimeoutExpired:
            proc.kill()

        print(f"  Run {i + 1}: {times[-1] * 1000:.1f}ms")

    return times


def measure_importtime(worker_module: str) -> dict[str, float]:
    """Run with -X importtime and parse output.

    Returns dict of module name -> cumulative import time in ms.
    """
    # Run Python with importtime flag, importing just the worker module
    result = subprocess.run(
        [
            sys.executable,
            "-X",
            "importtime",
            "-c",
            f"import {worker_module}",
        ],
        capture_output=True,
        text=True,
        env={**os.environ, "VGI_QUIET": "1"},
    )

    # Parse importtime output from stderr
    # Format: "import time:    self |   cumulative | module"
    import_times: dict[str, float] = {}
    pattern = re.compile(r"import time:\s+(\d+)\s+\|\s+(\d+)\s+\|\s+(.+)")

    for line in result.stderr.split("\n"):
        match = pattern.match(line)
        if match:
            cumulative = int(match.group(2))  # microseconds
            module = match.group(3).strip()
            import_times[module] = cumulative / 1000  # convert to ms

    return import_times


def profile_startup_phases() -> None:
    """Profile startup in more detail using cProfile."""
    import cProfile
    import pstats
    from io import StringIO

    # Profile just the imports
    profiler = cProfile.Profile()
    profiler.enable()

    # Import the worker module
    import vgi._test_fixtures.worker  # noqa: F401

    profiler.disable()

    # Print stats
    stream = StringIO()
    stats = pstats.Stats(profiler, stream=stream)
    stats.sort_stats("cumulative")
    stats.print_stats(30)  # Top 30 functions
    print(stream.getvalue())


def main() -> None:
    """CLI entry point for measuring VGI worker startup time."""
    parser = argparse.ArgumentParser(description="Measure VGI worker startup time")
    parser.add_argument(
        "--importtime",
        action="store_true",
        help="Show import time breakdown",
    )
    parser.add_argument(
        "--profile",
        action="store_true",
        help="Run cProfile on startup",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=5,
        help="Number of runs for timing (default: 5)",
    )
    parser.add_argument(
        "--worker",
        default="vgi-fixture-worker",
        help="Worker command to measure (default: vgi-fixture-worker)",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("VGI Worker Cold Start Measurement")
    print("=" * 60)

    if args.profile:
        print("\n### cProfile of imports ###\n")
        profile_startup_phases()
        return

    if args.importtime:
        print("\n### Import Time Analysis ###\n")
        import_times = measure_importtime("vgi._test_fixtures.worker")

        # Sort by cumulative time (descending)
        sorted_times = sorted(import_times.items(), key=lambda x: -x[1])

        print(f"{'Module':<50} {'Time (ms)':>10}")
        print("-" * 62)

        # Show top 30 slowest imports
        for module, time_ms in sorted_times[:30]:
            print(f"{module:<50} {time_ms:>10.1f}")

        if sorted_times:
            print("-" * 62)
            print(f"{'Total (top-level)':<50} {sorted_times[0][1]:>10.1f}")
        return

    # Basic timing
    print(f"\n### Timing {args.runs} runs of '{args.worker}' ###\n")
    worker_cmd = [args.worker]

    times = measure_basic_startup(worker_cmd, args.runs)

    if times:
        avg_ms = sum(times) / len(times) * 1000
        min_ms = min(times) * 1000
        max_ms = max(times) * 1000

        print()
        print(f"Results ({len(times)} runs):")
        print(f"  Average: {avg_ms:.1f}ms")
        print(f"  Min:     {min_ms:.1f}ms")
        print(f"  Max:     {max_ms:.1f}ms")

    # Also show import time summary
    print("\n### Top 10 Slowest Imports ###\n")
    import_times = measure_importtime("vgi._test_fixtures.worker")
    sorted_times = sorted(import_times.items(), key=lambda x: -x[1])

    print(f"{'Module':<50} {'Time (ms)':>10}")
    print("-" * 62)
    for module, time_ms in sorted_times[:10]:
        print(f"{module:<50} {time_ms:>10.1f}")


if __name__ == "__main__":
    main()
