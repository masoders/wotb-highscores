import argparse
import asyncio
import sys

from .. import db, logging_setup, tankopedia_static, tankopedia_sync


async def _run(*, force: bool, output_dir: str) -> int:
    await db.init_db()
    sync_result = await tankopedia_sync.sync_now(force=force, actor="cli")
    if sync_result.get("changed"):
        print(
            "updated: "
            f"total={sync_result.get('total_tanks', 0)} "
            f"added={sync_result.get('added_count', 0)} "
            f"removed={sync_result.get('removed_count', 0)} "
            f"updated={sync_result.get('updated_count', 0)}"
        )
    else:
        print("unchanged; skipped")

    static_result = await tankopedia_static.generate_static_site(output_dir=output_dir)
    print(
        "static generated: "
        f"dir={static_result.get('output_dir')} "
        f"tanks={static_result.get('tank_count', 0)}"
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync WG Tankopedia data and generate static browser files.")
    parser.add_argument("--force", action="store_true", help="Sync even if tanks_updated_at is unchanged.")
    parser.add_argument("--output-dir", default="tanks", help="Static output directory (default: tanks).")
    args = parser.parse_args()
    logging_setup.setup_logging()
    try:
        return asyncio.run(_run(force=bool(args.force), output_dir=str(args.output_dir)))
    except Exception as exc:
        print(f"sync_tankopedia failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
