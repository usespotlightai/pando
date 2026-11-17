#!/usr/bin/env python3
"""Draws the benchmark charts and fills them into the README.

    python3 bench/plot.py            reads bench/results/*.csv, writes docs/bench/*.svg, updates README.md

Results are CSV rows `system,workspace,measure,rep,value,unit,notes`. A chart panel takes the
median of the `small` workspace's repetitions per system. Systems with no rows yet are drawn as
"not measured yet", so the charts work as placeholders before the first run.

Each chart is written twice, for GitHub's light and dark themes, and the README shows it with a
<picture> element between `<!-- bench:NAME -->` and `<!-- /bench:NAME -->`, followed by the same
numbers as a table. Standard library only.
"""

import csv
import math
import re
import statistics
import sys
from html import escape
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench" / "results"
OUT = ROOT / "docs" / "bench"
README = ROOT / "README.md"
WORKSPACE = "small"

# GitHub's own neutral grays and blue (Primer), so the charts sit in the README like its native
# elements. Pando is blue, every other system gray. The background is transparent.
THEMES = {
    "light": dict(accent="#0969da", other="#818b98", ink="#1f2328", ink2="#59636e", muted="#59636e", grid="#eaeef2", axis="#d1d9e0"),
    "dark": dict(accent="#4493f8", other="#656c76", ink="#f0f6fc", ink2="#9198a1", muted="#9198a1", grid="#21262d", axis="#3d444d"),
}
FONT = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"
HIGHLIGHT = "Pando"

# Storage stand-ins, named as in bench/README.md.
NATIVE = "native ext4 (NVMe)"
TAR = "tar from S3"
IMAGE = "image download from S3"
OCI = "Docker (overlayfs)"
SOCI = "SOCI lazy pull"
NYDUS = "Nydus lazy pull"
EBS = "EBS snapshot to volume"
JFS = "JuiceFS"
ZFS0 = "ZeroFS (NBD)"
S3F = "Amazon S3 Files"
ARCHIL = "Archil"
REFLINK = "XFS reflink (local)"

REMOTE = [HIGHLIGHT, TAR, IMAGE, OCI, SOCI, NYDUS, EBS, JFS, ZFS0, S3F, ARCHIL]
FORKABLE = [HIGHLIGHT, OCI, EBS, JFS, ZFS0, ARCHIL, REFLINK]
LOCAL_ONLY = {REFLINK: "local only"}


def panel(metric, label, unit, scale="log", better="lower", systems=None, na=None, ref=None):
    return dict(metric=metric, label=label, unit=unit, scale=scale, better=better,
                systems=systems or REMOTE, na=na or {}, ref=ref)


CHARTS = {
    "fork": dict(
        title="Fork: time to a writable branch",
        alt="Bar chart of the time each system takes to make a writable branch of the workspace",
        panels=[
            panel("fork.p50", "p50 over up to 1,000 branches", "s", systems=FORKABLE),
            panel("fork-bytes.per_branch", "storage added per branch", "B", systems=FORKABLE),
            panel("fork-bytes.per_edit", "storage added by a 5 MiB edit to a branch", "B", systems=FORKABLE),
        ]),
    "cold": dict(
        title="Cold start: empty host to first git status",
        alt="Bar chart of the time from an empty host to the first git status finishing, per system",
        panels=[panel("cold.git_status", "fork, mount and git status done", "s", systems=REMOTE + [REFLINK], na=LOCAL_ONLY)]),
    "warm": dict(
        title="Warm start: host that ran a sibling branch",
        alt="Bar chart of the time to the first git status on a host that already ran a branch of the same base",
        panels=[panel("warm.git_status", "fork, mount and git status done", "s", systems=REMOTE + [REFLINK])]),
    "replay": dict(
        title="Agent workload: 14 commands, vs native ext4",
        alt="Bar chart of each system's total time for the agent command replay as a ratio to native ext4",
        panels=[panel("replay.total", "total time as a ratio to native (1.00x)", "x", scale="linear",
                      systems=[NATIVE, HIGHLIGHT, IMAGE, OCI, NYDUS, EBS, JFS, ZFS0, S3F, ARCHIL], ref=1.0)]),
    "save": dict(
        title="Save: last write to readable on another host",
        alt="Bar chart of the time and bytes each system needs to make a branch durable after npm ci",
        panels=[
            panel("save.time", "time to durable after npm ci", "s", systems=[HIGHLIGHT, TAR, OCI, EBS, JFS, ZFS0, ARCHIL]),
            panel("save.uploaded", "uploaded after npm ci", "MiB", systems=[HIGHLIGHT, TAR, OCI, EBS, JFS, ZFS0, ARCHIL]),
        ]),
    "fio": dict(
        title="Raw I/O (fio, direct I/O)",
        alt="Bar charts of random and sequential I/O throughput per system",
        panels=[
            panel(f"fio.{m}", label, u, scale="linear", better="higher",
                  systems=["local NVMe", "Pando (ublk)", "Pando (after handoff)", "EBS gp3", JFS, ZFS0])
            for m, label, u in [
                ("randread_qd1", "4 KiB random read, QD1", "IOPS"),
                ("randread_qd32", "4 KiB random read, 4 x QD32", "IOPS"),
                ("randwrite_qd32", "4 KiB random write, 4 x QD32", "IOPS"),
                ("seqread", "1 MiB sequential read", "MB/s"),
                ("seqwrite", "1 MiB sequential write", "MB/s"),
            ]
        ]),
}

# Values are converted to a base unit per kind before plotting and formatting.
TO_BASE = {"us": ("time", 1e-6), "ms": ("time", 1e-3), "s": ("time", 1.0), "min": ("time", 60.0),
           "B": ("bytes", 1), "KiB": ("bytes", 1 << 10), "MiB": ("bytes", 1 << 20), "GiB": ("bytes", 1 << 30),
           "x": ("ratio", 1.0), "IOPS": ("iops", 1.0), "MB/s": ("mbps", 1.0)}


def fmt(v, kind):
    if kind == "time":
        if v < 1e-3:
            return f"{v * 1e6:.0f} µs"
        if v < 1:
            return f"{v * 1e3:.3g} ms"
        if v < 120:
            return f"{v:.3g} s"
        return f"{v / 60:.3g} min"
    if kind == "bytes":
        for u, d in (("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)):
            if v >= d:
                return f"{v / d:.3g} {u}"
        return f"{v:.0f} B"
    if kind == "ratio":
        return f"{v:.2f}x"
    if kind == "iops":
        return f"{v / 1e3:.3g}k" if v >= 1e3 else f"{v:.0f}"
    return f"{v:,.0f} MB/s"


def load():
    """{metric: {system: (median, min, max)}} in base units, for WORKSPACE."""
    rows = {}
    for f in sorted(RESULTS.glob("*.csv")):
        with f.open(newline="") as h:
            for r in csv.DictReader(h):
                if r["workspace"] != WORKSPACE:
                    continue
                _, mul = TO_BASE[r["unit"]]
                rows.setdefault(r["measure"], {}).setdefault(r["system"], []).append(float(r["value"]) * mul)
    return {m: {s: (statistics.median(v), min(v), max(v)) for s, v in by.items()} for m, by in rows.items()}


def ticks(lo, hi, scale, kind=None):
    if scale == "log" and kind == "bytes":
        # Powers of 32 bytes, so ticks land on whole KiB, MiB and GiB.
        a, b = math.floor(math.log(lo, 32)), math.ceil(math.log(hi, 32))
        return [32.0 ** e for e in range(a, b + 1)]
    if scale == "log":
        a, b = math.floor(math.log10(lo)), math.ceil(math.log10(hi))
        return [10.0 ** e for e in range(a, b + 1)]
    step = 10 ** math.floor(math.log10(hi / 4)) if hi > 0 else 1
    for m in (1, 2, 2.5, 5, 10):
        if hi / (step * m) <= 5:
            step *= m
            break
    return [i * step for i in range(int(hi / step) + 2) if i * step <= hi * 1.0001 + step]


def draw(name, chart, data, theme):
    t = THEMES[theme]
    W, LEFT, RIGHT, ROW, BAR = 760, 212, 84, 26, 14
    x0, x1 = LEFT, W - RIGHT
    out, y = [], 34
    measured_any = False
    out.append(f'<text x="0" y="20" font-size="16" font-weight="600" fill="{t["ink"]}">{escape(chart["title"])}</text>')
    for p in chart["panels"]:
        kind = TO_BASE[p["unit"]][0]
        vals = data.get(p["metric"], {})
        known = {s: vals[s] for s in p["systems"] if s in vals}
        measured_any |= bool(known)
        rows = sorted(known, key=lambda s: known[s][0], reverse=p["better"] == "higher")
        rows += [s for s in p["systems"] if s not in known]
        hint = "lower is better" if p["better"] == "lower" else "higher is better"
        scale_note = ", log scale" if p["scale"] == "log" and known else ""
        y += 18
        out.append(f'<text x="0" y="{y}" font-size="13" fill="{t["ink2"]}">{escape(p["label"])} ({hint}{scale_note})</text>')
        y += 10
        top = y
        if known:
            vmax = max(v[0] for v in known.values())
            if p["ref"]:
                vmax = max(vmax, p["ref"])
            if p["scale"] == "log":
                vmin = min(v[0] for v in known.values() if v[0] > 0)
                tk = ticks(vmin, vmax, "log", kind)
                lo, hi = math.log10(tk[0]), math.log10(tk[-1])
                pos = lambda v: x0 + (math.log10(max(v, tk[0])) - lo) / (hi - lo or 1) * (x1 - x0)
            else:
                tk = ticks(0, vmax, "linear")
                pos = lambda v: x0 + v / (tk[-1] or 1) * (x1 - x0)
        bottom = top + ROW * len(rows)
        if known:
            for v in tk:
                gx = pos(v)
                out.append(f'<line x1="{gx:.1f}" y1="{top}" x2="{gx:.1f}" y2="{bottom}" stroke="{t["grid"]}" stroke-width="1"/>')
                out.append(f'<text x="{gx:.1f}" y="{bottom + 14}" font-size="11" fill="{t["muted"]}" text-anchor="middle">{fmt(v, kind)}</text>')
        out.append(f'<line x1="{x0}" y1="{top}" x2="{x0}" y2="{bottom}" stroke="{t["axis"]}" stroke-width="1"/>')
        for i, s in enumerate(rows):
            cy = top + ROW * i + ROW / 2
            weight = ' font-weight="600"' if s.startswith(HIGHLIGHT) else ""
            out.append(f'<text x="{x0 - 10}" y="{cy + 4:.1f}" font-size="12" fill="{t["ink"]}" text-anchor="end"{weight}>{escape(s)}</text>')
            if s in known:
                v = known[s][0]
                bx = pos(v)
                w = max(bx - x0, 2)
                r = min(4, w)
                fill = t["accent"] if s.startswith(HIGHLIGHT) else t["other"]
                by = cy - BAR / 2
                # Square at the baseline, 4px rounded at the data end.
                out.append(f'<path d="M{x0},{by:.1f} h{w - r:.1f} a{r},{r} 0 0 1 {r},{r} v{BAR - 2 * r} a{r},{r} 0 0 1 -{r},{r} h-{w - r:.1f} z" fill="{fill}">'
                           f'<title>{escape(s)}: {fmt(v, kind)} (range {fmt(known[s][1], kind)} to {fmt(known[s][2], kind)})</title></path>')
                out.append(f'<text x="{x0 + w + 6:.1f}" y="{cy + 4:.1f}" font-size="12" fill="{t["ink2"]}">{fmt(v, kind)}</text>')
            else:
                note = p["na"].get(s, "not measured yet")
                out.append(f'<text x="{x0 + 8}" y="{cy + 4:.1f}" font-size="12" font-style="italic" fill="{t["muted"]}">{escape(note)}</text>')
        if known and p["ref"]:
            rx = pos(p["ref"])
            out.append(f'<line x1="{rx:.1f}" y1="{top - 4}" x2="{rx:.1f}" y2="{bottom}" stroke="{t["ink2"]}" stroke-width="1.5"/>')
        y = bottom + (26 if known else 8)
    if not measured_any:
        out.append(f'<text x="{W}" y="20" font-size="12" fill="{t["muted"]}" text-anchor="end">placeholder: no results yet</text>')
    H = y + 6
    body = "\n".join("  " + line for line in out)
    return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" '
            f'font-family="{FONT}" role="img">\n  <title>{escape(chart["title"])}</title>\n'
            f'  <desc>{escape(chart["alt"])}</desc>\n{body}\n</svg>\n')


def table(chart, data):
    lines = []
    for p in chart["panels"]:
        kind = TO_BASE[p["unit"]][0]
        vals = data.get(p["metric"], {})
        lines += [f"| {p['label']} | median | range |", "|---|---|---|"]
        for s in p["systems"]:
            if s in vals:
                m, lo, hi = vals[s]
                lines.append(f"| {s} | {fmt(m, kind)} | {fmt(lo, kind)} to {fmt(hi, kind)} |")
            else:
                lines.append(f"| {s} | {p['na'].get(s, '—')} | |")
        lines.append("")
    return "\n".join(lines).rstrip()


def block(name, chart, data):
    return (f"<!-- bench:{name} -->\n<picture>\n"
            f'  <source media="(prefers-color-scheme: dark)" srcset="docs/bench/{name}-dark.svg">\n'
            f'  <img alt="{escape(chart["alt"])}" src="docs/bench/{name}.svg">\n</picture>\n\n'
            f"<details>\n<summary>Numbers</summary>\n\n{table(chart, data)}\n\n</details>\n"
            f"<!-- /bench:{name} -->")


def main():
    data = load()
    OUT.mkdir(parents=True, exist_ok=True)
    readme = README.read_text()
    for name, chart in CHARTS.items():
        (OUT / f"{name}.svg").write_text(draw(name, chart, data, "light"))
        (OUT / f"{name}-dark.svg").write_text(draw(name, chart, data, "dark"))
        pat = re.compile(rf"<!-- bench:{name} -->.*?<!-- /bench:{name} -->", re.S)
        if not pat.search(readme):
            print(f"README.md has no bench:{name} markers; skipped", file=sys.stderr)
            continue
        readme = pat.sub(lambda _: block(name, chart, data), readme)
    README.write_text(readme)


if __name__ == "__main__":
    main()
