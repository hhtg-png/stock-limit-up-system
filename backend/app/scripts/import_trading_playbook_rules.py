"""Import the transcript-derived trading playbook rule catalog."""
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from app.database import async_session_maker, init_db
from app.services.trading_playbook.rule_catalog import RuleCatalog


async def import_rules(source_root: Path) -> dict[str, int]:
    """Initialize the database and seed the bundled rule catalog."""
    await init_db()
    catalog_path = (
        Path(__file__).resolve().parents[1]
        / "data"
        / "trading_playbook_rules_v2.json"
    )
    async with async_session_maker() as session:
        return await RuleCatalog(catalog_path).seed(session, source_root)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", required=True, type=Path)
    args = parser.parse_args()
    result = asyncio.run(import_rules(args.source_root))
    print(f'sources={result["sources"]} rules={result["rules"]}')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
