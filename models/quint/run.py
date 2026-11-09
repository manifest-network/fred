#!/usr/bin/env python3
"""Reproduce sampled invariants and expected counterexamples with pinned Quint."""

import argparse
import concurrent.futures
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent
VERSION = "0.33.0"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quint", type=Path, default=ROOT / "node_modules/.bin/quint")
    parser.add_argument("--samples", type=int, default=2000)
    parser.add_argument("--steps", type=int, default=30)
    parser.add_argument("--seed", type=int, default=1517)
    parser.add_argument("--jobs", type=int, default=2)
    parser.add_argument("--model", help="Select cases by model filename stem")
    parser.add_argument("--out", type=Path, default=ROOT / "results")
    args = parser.parse_args()
    if min(args.samples, args.steps, args.jobs) <= 0:
        parser.error("samples, steps, and jobs must be positive")
    quint = str(args.quint.resolve())
    version = subprocess.run([quint, "--version"], capture_output=True, text=True, check=True).stdout.strip()
    if version != VERSION:
        parser.error(f"expected Quint {VERSION}, got {version}")
    cases = json.loads((ROOT / "cases.json").read_text())
    if args.model:
        cases = [case for case in cases if Path(case["model"]).stem == args.model]
    if not cases:
        parser.error("no cases match --model")
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)

    # A parser/type error must never masquerade as an expected counterexample.
    for model in sorted({case["model"] for case in cases}):
        subprocess.run([quint, "typecheck", str(ROOT / model)], check=True)

    def run(item):
        index, case = item
        path = (ROOT / case["model"]).resolve()
        name = f"{index:02d}-{path.stem}-{case['variant']}"
        command = [
            quint, "run", str(path), "--backend=typescript",
            f"--main={case['variant']}", f"--invariant={case['invariant']}",
            f"--step={case['step']}", f"--max-samples={args.samples}",
            f"--max-steps={args.steps}", f"--seed={args.seed}",
            f"--out-itf={out / (name + '.itf.json')}",
        ]
        start = time.monotonic()
        try:
            process = subprocess.run(command, capture_output=True, text=True, timeout=600)
            output = process.stdout + process.stderr
            code = process.returncode
        except subprocess.TimeoutExpired:
            output, code = "Runner timed out after 600 seconds", -1
        (out / (name + ".log")).write_text(output)
        matched = (
            code == 1 and "[violation]" in output and "Invariant violated" in output
            if case["expect_violation"] else code == 0
        )
        result = dict(
            **case, sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            command=command, exit_code=code, matched_expectation=matched,
            seconds=round(time.monotonic() - start, 2),
        )
        print(f"{'PASS' if matched else 'FAIL'} {name}: {case['invariant']}", flush=True)
        return result

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as pool:
        results = list(pool.map(run, enumerate(cases)))
    report = dict(quint=VERSION, backend="typescript", seed=args.seed,
                  samples=args.samples, steps=args.steps, results=results)
    (out / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    return 0 if all(result["matched_expectation"] for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
