#!/usr/bin/env python3
"""Check the cases marked exhaustive in every reachable state, with pinned Quint and TLC."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

from run import ROOT, VERSION

MODULE = re.compile(r"^module (\w+) \{")
DISTINCT = re.compile(r"(\d+) distinct states found")


def single_variant(source, keep, variants):
    """Return source without the modules of the other variants.

    Quint compiles a whole file for the checker, and a file with many
    instances of one module can exceed the compiler's input limit.
    """
    kept, dropping = [], False
    for line in source.splitlines(keepends=True):
        match = MODULE.match(line)
        if match and match.group(1) in variants and match.group(1) != keep:
            dropping = True
        if not dropping:
            kept.append(line)
        elif line.rstrip() == "}":
            dropping = False
    return "".join(kept)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quint", type=Path, default=ROOT / "node_modules/.bin/quint")
    parser.add_argument("--model", help="Select cases by model filename stem")
    parser.add_argument("--timeout", type=int, default=600, help="Seconds allowed per case")
    parser.add_argument("--out", type=Path, default=ROOT / "results" / "exhaustive")
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("timeout must be positive")
    quint = str(args.quint.resolve())
    version = subprocess.run([quint, "--version"], capture_output=True, text=True, check=True).stdout.strip()
    if version != VERSION:
        parser.error(f"expected Quint {VERSION}, got {version}")
    if shutil.which("java") is None:
        parser.error("no java on PATH: the exhaustive check runs TLC, which needs a Java runtime")
    every_case = json.loads((ROOT / "cases.json").read_text())
    cases = [case for case in every_case if case.get("exhaustive")]
    if args.model:
        cases = [case for case in cases if Path(case["model"]).stem == args.model]
    if not cases:
        parser.error("no exhaustive cases match --model")
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)

    # A parser/type error must never masquerade as an expected counterexample.
    for model in sorted({case["model"] for case in cases}):
        subprocess.run([quint, "typecheck", str(ROOT / model)], check=True)

    results = []
    for index, case in enumerate(cases):
        path = (ROOT / case["model"]).resolve()
        name = f"{index:02d}-{path.stem}-{case['variant']}"
        source = path.read_text()
        checked = path
        # A model that imports another file is checked whole, where it is.
        if ' from "' not in source:
            variants = {other["variant"] for other in every_case if other["model"] == case["model"]}
            checked = out / f"{name}.qnt"
            checked.write_text(single_variant(source, case["variant"], variants))
        command = [
            quint, "verify", str(checked), "--backend=tlc",
            f"--main={case['variant']}", f"--invariant={case['invariant']}",
            f"--step={case['step']}",
        ]
        start = time.monotonic()
        try:
            # The checker leaves its working files in the current directory.
            process = subprocess.run(command, capture_output=True, text=True, timeout=args.timeout, cwd=out)
            output = process.stdout + process.stderr
            code = process.returncode
        except subprocess.TimeoutExpired:
            output, code = f"Checker timed out after {args.timeout} seconds", -1
        (out / (name + ".log")).write_text(output)
        counts = DISTINCT.findall(output)
        states = int(counts[-1]) if counts else None
        if case["expect_violation"]:
            matched = code == 1 and "[violation]" in output
            detail = ""
        else:
            # TLC reports this line only after it has visited every reachable state.
            complete = code == 0 and "Model checking completed. No error has been found." in output
            recorded = case.get("distinct_states")
            matched = complete and recorded in (None, states)
            detail = f" ({states} distinct states" + (
                ")" if recorded in (None, states) else f", recorded {recorded})")
        results.append(dict(
            **case, sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            command=command, exit_code=code, observed_distinct_states=states,
            matched_expectation=matched, seconds=round(time.monotonic() - start, 2),
        ))
        print(f"{'PASS' if matched else 'FAIL'} {name}: {case['invariant']}{detail}", flush=True)

    report = dict(quint=VERSION, backend="tlc", results=results)
    (out / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    return 0 if all(result["matched_expectation"] for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
