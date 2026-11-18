"""The `small` workspace: Spotlight at a pinned commit after its setup command, as an ext4 image.

Built once per host into /data/work/workspace and reused by every measure:
  src/        the checkout, after `npm ci` in control-plane/ and web/
  base.img    an ext4 image of src/, made with `mkfs.ext4 -d` (no mount needed)
  meta.json   the commit, the Node version, the size and file count
"""

import json
import os
import shutil
from pathlib import Path
from typing import Any

from common import TOOLS, WORK, fetch, log, run

SPOTLIGHT = "https://github.com/usespotlightai/spotlight.git"
SPOTLIGHT_COMMIT = "eabaef08c9d98f628b5b05c98cd67299566e7b50"
NODE_VERSION = "24.22.3"
NODE_SHA256 = "4f2b175a6d53a5c617a80dba2676238a0688cef28c6da4eba35c7d844e66fcfb"

DIR = WORK / "workspace"
SRC = DIR / "src"
IMAGE = DIR / "base.img"
META = DIR / "meta.json"


def node_bin() -> Path:
    """Return the bin directory of Node.js, installed from nodejs.org and checked against its SHA-256."""
    name = f"node-v{NODE_VERSION}-linux-x64"
    root = TOOLS / name
    if not (root / "bin" / "node").exists():
        tar = fetch(
            f"https://nodejs.org/dist/v{NODE_VERSION}/{name}.tar.xz", TOOLS / f"{name}.tar.xz", NODE_SHA256
        )
        run("tar", "-xJf", tar, "-C", TOOLS)
    return root / "bin"


def prepare() -> dict[str, Any]:
    """Build the workspace image unless this host has it for the pinned commit; return its meta.json."""
    meta = json.loads(META.read_text()) if META.exists() else {}
    if meta.get("commit") == SPOTLIGHT_COMMIT and meta.get("node") == NODE_VERSION and IMAGE.exists():
        log(f"workspace ready: {meta['files']} files, {meta['bytes'] >> 20} MiB")
        return meta
    if DIR.exists():
        shutil.rmtree(DIR)
    DIR.mkdir(parents=True)

    log(f"cloning Spotlight at {SPOTLIGHT_COMMIT[:12]}")
    run("git", "clone", "--quiet", "--no-checkout", SPOTLIGHT, SRC)
    run("git", "-C", SRC, "checkout", "--quiet", "--detach", SPOTLIGHT_COMMIT)

    path = f"{node_bin()}:{os.environ['PATH']}"
    for d in ("control-plane", "web"):
        log(f"npm ci in {d}")
        run("npm", "ci", "--no-audit", "--no-fund", cwd=SRC / d, env_extra={"PATH": path, "CI": "1"})

    files, size = 0, 0
    for root, dirs, names in os.walk(SRC):
        files += len(names) + len(dirs)
        for n in names:
            size += (Path(root) / n).lstat().st_size
    # Room for the file system's own metadata and for the agent workload's writes.
    image_bytes = ((size * 2 + (2 << 30)) >> 30) << 30
    log(f"building the ext4 image: {files} entries, {size >> 20} MiB of data, {image_bytes >> 30} GiB image")
    run("truncate", "-s", image_bytes, IMAGE)
    run("mkfs.ext4", "-q", "-F", "-L", "workspace", "-d", SRC, IMAGE)

    meta = {
        "commit": SPOTLIGHT_COMMIT,
        "node": NODE_VERSION,
        "files": files,
        "bytes": size,
        "image_bytes": image_bytes,
    }
    META.write_text(json.dumps(meta, indent=2))
    return meta
