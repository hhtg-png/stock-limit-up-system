import hashlib
import json
import re
import tempfile
import unittest
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models import TradingModeRule, TradingRuleSource
from app.services.trading_playbook.rule_catalog import RuleCatalog


CATALOG_PATH = (
    Path(__file__).resolve().parents[1]
    / "app"
    / "data"
    / "trading_playbook_rules_v1.json"
)


class TradingPlaybookRuleCatalogTests(unittest.TestCase):
    def test_loads_complete_version_one_catalog_with_valid_source_refs(self):
        catalog = RuleCatalog(CATALOG_PATH).load()

        self.assertEqual(catalog["catalog_version"], 1)
        self.assertEqual(len(catalog["sources"]), 8)
        self.assertEqual(len(catalog["rules"]), 19)

        source_keys = {source["source_key"] for source in catalog["sources"]}
        mode_keys = [rule["mode_key"] for rule in catalog["rules"]]
        self.assertEqual(len(mode_keys), len(set(mode_keys)))
        for rule in catalog["rules"]:
            self.assertTrue(rule["source_refs"])
            for source_ref in rule["source_refs"]:
                self.assertIn(source_ref["source_key"], source_keys)
                self.assertTrue(source_ref["excerpt"].strip())

    def test_verify_sources_hashes_present_transcript_and_marks_missing_source(self):
        sources = [
            {
                "source_key": "present",
                "source_path": "nested/present.txt",
                "source_title": "Present",
            },
            {
                "source_key": "missing",
                "source_path": "nested/missing.txt",
                "source_title": "Missing",
            },
        ]
        raw_content = "交易规则\n".encode("utf-8")

        with tempfile.TemporaryDirectory() as temporary_directory:
            source_root = Path(temporary_directory)
            transcript_path = source_root / sources[0]["source_path"]
            transcript_path.parent.mkdir(parents=True)
            transcript_path.write_bytes(raw_content)

            verified = RuleCatalog(CATALOG_PATH).verify_sources(source_root, sources)

        self.assertEqual(verified[0]["status"], "ready")
        self.assertEqual(
            verified[0]["content_hash"],
            hashlib.sha256(raw_content).hexdigest(),
        )
        self.assertEqual(len(verified[0]["content_hash"]), 64)
        self.assertEqual(verified[1]["status"], "missing")
        self.assertEqual(verified[1]["content_hash"], "")


class TradingPlaybookRuleCatalogSeedTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine(
            "sqlite+aiosqlite:///:memory:",
            future=True,
            poolclass=StaticPool,
        )
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        self.session_factory = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
        )
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.source_root = Path(self.temporary_directory.name) / "transcripts"
        self.catalog = RuleCatalog(CATALOG_PATH)
        self.catalog_data = self.catalog.load()

    async def asyncTearDown(self):
        self.temporary_directory.cleanup()
        await self.engine.dispose()

    def _write_all_transcripts(self):
        for source in self.catalog_data["sources"]:
            transcript_path = self.source_root / source["source_path"]
            transcript_path.parent.mkdir(parents=True, exist_ok=True)
            transcript_path.write_text(
                f'{source["source_title"]}\n测试转写内容\n',
                encoding="utf-8",
            )

    async def _counts(self):
        async with self.session_factory() as session:
            source_count = await session.scalar(
                select(func.count()).select_from(TradingRuleSource)
            )
            rule_count = await session.scalar(
                select(func.count()).select_from(TradingModeRule)
            )
        return source_count, rule_count

    async def test_seed_creates_expected_rows_is_idempotent_and_enforces_version_bump(self):
        self._write_all_transcripts()

        async with self.session_factory() as session:
            first_result = await self.catalog.seed(session, self.source_root)
        self.assertEqual(first_result, {"sources": 8, "rules": 19})
        self.assertEqual(await self._counts(), (8, 19))

        async with self.session_factory() as session:
            second_result = await self.catalog.seed(session, self.source_root)
        self.assertEqual(second_result, {"sources": 8, "rules": 19})
        self.assertEqual(await self._counts(), (8, 19))

        changed_catalog_data = json.loads(json.dumps(self.catalog_data))
        changed_catalog_data["rules"][0]["name"] = "未经版本升级的变更"
        changed_catalog_path = Path(self.temporary_directory.name) / "changed.json"
        changed_catalog_path.write_text(
            json.dumps(changed_catalog_data, ensure_ascii=False),
            encoding="utf-8",
        )

        async with self.session_factory() as session:
            with self.assertRaisesRegex(ValueError, "version bump"):
                await RuleCatalog(changed_catalog_path).seed(
                    session,
                    self.source_root,
                )
        self.assertEqual(await self._counts(), (8, 19))

    async def test_missing_transcript_lists_relative_path_and_rolls_back_seed(self):
        self._write_all_transcripts()
        missing_relative_path = self.catalog_data["sources"][3]["source_path"]
        (self.source_root / missing_relative_path).unlink()

        async with self.session_factory() as session:
            with self.assertRaisesRegex(
                FileNotFoundError,
                re.escape(missing_relative_path),
            ):
                await self.catalog.seed(session, self.source_root)

        self.assertEqual(await self._counts(), (0, 0))

    async def test_seed_rejects_replacing_mode_key_without_version_bump(self):
        self._write_all_transcripts()
        async with self.session_factory() as session:
            await self.catalog.seed(session, self.source_root)

        changed_catalog_data = json.loads(json.dumps(self.catalog_data))
        changed_catalog_data["rules"][0]["mode_key"] = "renamed_mode"
        changed_catalog_path = Path(self.temporary_directory.name) / "renamed.json"
        changed_catalog_path.write_text(
            json.dumps(changed_catalog_data, ensure_ascii=False),
            encoding="utf-8",
        )

        async with self.session_factory() as session:
            with self.assertRaisesRegex(ValueError, "version bump"):
                await RuleCatalog(changed_catalog_path).seed(
                    session,
                    self.source_root,
                )

        self.assertEqual(await self._counts(), (8, 19))
        async with self.session_factory() as session:
            renamed_rule = await session.scalar(
                select(TradingModeRule).where(
                    TradingModeRule.mode_key == "renamed_mode"
                )
            )
        self.assertIsNone(renamed_rule)


if __name__ == "__main__":
    unittest.main()
