#!/usr/bin/env python3
"""Download NYC TLC Yellow Taxi monthly Parquet files into data/raw/.

Only 2016-07 onward is used: from then on trips carry TLC zone IDs (PULocationID / DOLocationID)
instead of lat/lon, which App 3 groups on. Stdlib only; resumable (skips files whose size matches
the server's Content-Length).

    python harness/download_tlc.py                       # 2016-07 .. 2024-12 (~9 GB)
    python harness/download_tlc.py --start 2019-01 --end 2019-03 --jobs 2
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import sys
import urllib.request
from pathlib import Path

BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{ym}.parquet"
ROOT = Path(__file__).resolve().parent.parent


def months(start: str, end: str):
    y, m = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    while (y, m) <= (ey, em):
        yield f"{y:04d}-{m:02d}"
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def remote_size(url: str) -> int | None:
    req = urllib.request.Request(url, method="HEAD")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return int(r.headers["Content-Length"])
    except Exception:
        return None


def fetch(ym: str, dest_dir: Path) -> tuple[str, str]:
    url = BASE.format(ym=ym)
    dest = dest_dir / f"yellow_tripdata_{ym}.parquet"
    size = remote_size(url)
    if size is None:
        return ym, "missing on server (skipped)"
    if dest.exists() and dest.stat().st_size == size:
        return ym, "already present"
    part = dest.with_suffix(".part")
    with urllib.request.urlopen(url, timeout=60) as r, open(part, "wb") as f:
        while chunk := r.read(1 << 20):
            f.write(chunk)
    if part.stat().st_size != size:
        part.unlink(missing_ok=True)
        return ym, f"size mismatch (expected {size})"
    part.rename(dest)
    return ym, f"downloaded {size / 1e6:.0f} MB"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default="2016-07")
    ap.add_argument("--end", default="2024-12")
    ap.add_argument("--out", type=Path, default=ROOT / "data" / "raw")
    ap.add_argument("--jobs", type=int, default=4)
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)

    bad = 0
    with cf.ThreadPoolExecutor(a.jobs) as ex:
        for ym, msg in ex.map(lambda ym: fetch(ym, a.out), months(a.start, a.end)):
            print(f"{ym}: {msg}", flush=True)
            bad += "mismatch" in msg
    total = sum(p.stat().st_size for p in a.out.glob("*.parquet"))
    print(f"\n{len(list(a.out.glob('*.parquet')))} files, {total / 1e9:.2f} GB in {a.out}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
