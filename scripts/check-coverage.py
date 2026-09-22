#!/usr/bin/env python3
"""Report every package in a Go coverage profile and enforce the measured floor."""

from collections import defaultdict
from decimal import Decimal
import os
from pathlib import Path
import sys


def report(profile, threshold):
    packages = defaultdict(lambda: [0, 0])
    lines = profile.read_text().splitlines()
    if not lines or lines[0] not in ("mode: set", "mode: count", "mode: atomic"):
        raise ValueError("invalid coverage profile header")
    for line in lines[1:]:
        location, statements, hits = line.split()
        statements, hits = int(statements), int(hits)
        if statements < 0 or hits < 0:
            raise ValueError("negative coverage count")
        package = location.rsplit("/", 1)[0]
        counts = packages[package]
        counts[0] += statements if hits else 0
        counts[1] += statements
    covered = sum(counts[0] for counts in packages.values())
    total = sum(counts[1] for counts in packages.values())
    if not total:
        raise ValueError("coverage profile contains no statements")
    percent = Decimal(100) * covered / total
    output = [f"Statement coverage: {percent:.2f}% ({covered}/{total}); floor: {threshold}%.",
              "", "| Package | Coverage |", "| --- | ---: |"]
    for package, (hit, count) in sorted(packages.items()):
        output.append(f"| `{package}` | {100 * hit / count:.2f}% |" if count
                      else f"| `{package}` | no statements |")
    return "\n".join(output) + "\n", percent >= threshold


def main():
    threshold = Decimal(Path(".coverage-threshold").read_text().strip())
    if not threshold.is_finite() or not 0 < threshold <= 100:
        raise ValueError("coverage floor must be between 0 and 100")
    output, passed = report(Path(sys.argv[1]), threshold)
    print(output, end="")
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a") as stream:
            stream.write(output)
    if not passed:
        print("::error::Statement coverage regressed below the measured floor.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
