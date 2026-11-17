"""Bench harness. Runs on a bench host as root:

    python3 bench/harness workspace                    build the workspace image (once per host)
    python3 bench/harness fork [--systems a,b] [--branches N] [--rep R]
    python3 bench/harness cold|warm|replay [--systems a,b] [--reps N]

Results go to /data/results/*.csv (see bench/README.md for the columns).
"""

import argparse

import fork
import start
import workspace


def main():
    p = argparse.ArgumentParser(prog="bench/harness")
    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("workspace", help="build the workspace image")
    f = sub.add_parser("fork", help="the fork measure")
    f.add_argument("--systems", default=",".join(fork.SYSTEMS), help=f"comma-separated, from {', '.join(fork.SYSTEMS)}")
    f.add_argument("--branches", type=int, default=1000, help="branches per system (capped per system)")
    f.add_argument("--rep", type=int, default=1, help="repetition number written to the results")
    start.main_args(sub)
    a = p.parse_args()

    if a.command == "workspace":
        workspace.prepare()
    elif a.command == "fork":
        names = [s for s in a.systems.split(",") if s]
        unknown = [s for s in names if s not in fork.SYSTEMS]
        if unknown:
            p.error(f"unknown systems: {', '.join(unknown)}")
        fork.measure(names, a.branches, a.rep)
    else:
        names = [s for s in a.systems.split(",") if s]
        unknown = [s for s in names if s not in start.SYSTEMS]
        if unknown:
            p.error(f"unknown systems: {', '.join(unknown)}")
        start.measure(a.command, names, a.reps)


main()
