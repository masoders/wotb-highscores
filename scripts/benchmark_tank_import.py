#!/usr/bin/env python3
import argparse
import asyncio
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tankbot import config, db


def _build_rows(total: int, unique: int) -> list[tuple[str, int, str]]:
    types = ("light", "medium", "heavy", "td")
    unique = max(1, min(unique, total))
    base = [(f"Tank-{i:05d}", (i % 10) + 1, types[i % 4]) for i in range(unique)]
    rows: list[tuple[str, int, str]] = []
    for i in range(total):
        rows.append(base[i % unique])
    return rows


async def _benchmark_legacy(db_path: str, rows: list[tuple[str, int, str]]) -> tuple[int, int, float]:
    config.DB_PATH = db_path
    await db.init_db()
    added = 0
    skipped = 0
    t0 = time.perf_counter()
    for name, tier, ttype in rows:
        try:
            await db.add_tank(name, tier, ttype, "bench", "2026-02-10T00:00:00Z")
            added += 1
        except Exception:
            skipped += 1
    elapsed = time.perf_counter() - t0
    return added, skipped, elapsed


async def _benchmark_bulk(db_path: str, rows: list[tuple[str, int, str]]) -> tuple[int, int, float]:
    config.DB_PATH = db_path
    await db.init_db()
    t0 = time.perf_counter()
    added, skipped = await db.add_tanks_bulk(rows, "bench", "2026-02-10T00:00:00Z")
    elapsed = time.perf_counter() - t0
    return added, skipped, elapsed


async def main():
    parser = argparse.ArgumentParser(
        description="Benchmark tank CSV import strategies (legacy per-row vs bulk)."
    )
    parser.add_argument("--rows", type=int, default=5000, help="Total input rows.")
    parser.add_argument(
        "--unique",
        type=int,
        default=4000,
        help="Unique tank names inside the input rows.",
    )
    args = parser.parse_args()

    rows = _build_rows(args.rows, args.unique)

    with tempfile.TemporaryDirectory(prefix="tankbot-bench-") as tmp:
        tmp_path = Path(tmp)
        legacy_db = str(tmp_path / "legacy.db")
        bulk_db = str(tmp_path / "bulk.db")

        l_added, l_skipped, l_elapsed = await _benchmark_legacy(legacy_db, rows)
        b_added, b_skipped, b_elapsed = await _benchmark_bulk(bulk_db, rows)

    print("Benchmark complete")
    print(f"rows={args.rows} unique={args.unique}")
    print(f"legacy: added={l_added} skipped={l_skipped} elapsed={l_elapsed:.4f}s")
    print(f"bulk:   added={b_added} skipped={b_skipped} elapsed={b_elapsed:.4f}s")
    if b_elapsed > 0:
        print(f"speedup: {l_elapsed / b_elapsed:.2f}x")


if __name__ == "__main__":
    asyncio.run(main())
