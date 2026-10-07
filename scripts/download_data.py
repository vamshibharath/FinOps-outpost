"""Download the FOCUS 1.0 sample billing dataset (anonymized, real multi-cloud billing).

Source: FinOps Foundation, https://github.com/FinOps-Open-Cost-and-Usage-Spec/FOCUS-Sample-Data
Providers in the sample: AWS (full month), Microsoft Azure, Oracle OCI, and a small Google Cloud slice.
"""
from __future__ import annotations

import argparse
import hashlib
import sys
import urllib.request
from pathlib import Path

BASE = "https://github.com/FinOps-Open-Cost-and-Usage-Spec/FOCUS-Sample-Data/raw/main/FOCUS-1.0/"
FILES = {
    "small": "focus_sample_10000.csv",
    "full": "focus_sample_100000.csv.gz",
}


def download(size: str, dest_dir: Path) -> Path:
    name = FILES[size]
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / name
    if dest.exists():
        print(f"[download] already present: {dest}")
        return dest
    url = BASE + name
    print(f"[download] {url}")
    with urllib.request.urlopen(url, timeout=120) as resp, open(dest, "wb") as fh:
        fh.write(resp.read())
    sha = hashlib.sha256(dest.read_bytes()).hexdigest()[:12]
    print(f"[download] saved {dest} ({dest.stat().st_size:,} bytes, sha256={sha}...)")
    return dest


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--size", choices=FILES, default="full")
    p.add_argument("--dest", default="data/landing")
    a = p.parse_args()
    try:
        download(a.size, Path(a.dest))
    except Exception as exc:  # network failures should be loud, not silent
        print(f"[download] FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
