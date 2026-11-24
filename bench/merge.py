#!/usr/bin/env python3
"""Merges a bench run's results into bench/results.

    python3 bench/merge.py DIR    reads DIR/<name>.csv, rewrites bench/results/<name>.csv

A run's file replaces every row of each system it measured and keeps the other systems' rows,
so a run of some systems leaves the rest of the comparison as it was. Files in DIR that
bench/results does not have are added. Prints each file's systems, one line per file. Standard
library only.
"""

import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench" / "results"
FIELDS = ["system", "workspace", "measure", "rep", "value", "unit", "notes"]


def read(path: Path) -> list[dict[str, str]]:
    """Return the rows of a results file, checking its header."""
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != FIELDS:
            sys.exit(f"{path}: header is {reader.fieldnames}, want {FIELDS}")
        return list(reader)


def merge(old: list[dict[str, str]], new: list[dict[str, str]]) -> list[dict[str, str]]:
    """Return old with each system new has replaced where its rows began, new systems last."""
    by_system: dict[str, list[dict[str, str]]] = {}
    for r in new:
        by_system.setdefault(r["system"], []).append(r)
    rows: list[dict[str, str]] = []
    placed: set[str] = set()
    for r in old:
        system = r["system"]
        if system not in by_system:
            rows.append(r)
        elif system not in placed:
            rows.extend(by_system[system])
            placed.add(system)
    for system, rest in by_system.items():
        if system not in placed:
            rows.extend(rest)
    return rows


def main() -> None:
    """Merge every results file of the run directory given as the only argument."""
    if len(sys.argv) != 2:  # noqa: PLR2004 -- the program name and the directory
        sys.exit(__doc__)
    run = Path(sys.argv[1])
    files = sorted(run.glob("*.csv"))
    if not files:
        sys.exit(f"{run}: no results files")
    for src in files:
        new = read(src)
        if not new:
            continue
        dst = RESULTS / src.name
        rows = merge(read(dst) if dst.exists() else [], new)
        with dst.open("w", newline="") as f:
            w = csv.DictWriter(f, FIELDS, lineterminator="\n")
            w.writeheader()
            w.writerows(rows)
        systems = ", ".join(dict.fromkeys(r["system"] for r in new))
        print(f"{src.name}: {systems}")


if __name__ == "__main__":
    main()
