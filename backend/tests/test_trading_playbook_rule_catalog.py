import hashlib
import json
import re
import tempfile
import unittest
from pathlib import Path

from sqlalchemy import event, func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models import TradingModeRule, TradingRuleSource
from app.services.trading_playbook.mode_matcher import ModeMatcher
from app.services.trading_playbook.rule_catalog import RuleCatalog
from app.services.trading_playbook import rule_catalog as rule_catalog_module


CATALOG_PATH = (
    Path(__file__).resolve().parents[1]
    / "app"
    / "data"
    / "trading_playbook_rules_v1.json"
)

EXPECTED_SOURCES = {
    "00-art-1123": (
        "00-zgjys-live/01_zgjys-art-trading-1123.txt",
        "交易的艺术 1123",
    ),
    "00-art-1130": (
        "00-zgjys-live/02_zgjys-art-trading-1130.txt",
        "交易的艺术 1130",
    ),
    "01-specialize": (
        "01-止于心动-专精一艺/01_2025-8-3直播：止于心动，专精一艺.txt",
        "止于心动，专精一艺",
    ),
    "02-window-recognition": (
        "02-window-recognition/01_2026-3-7小灶：窗口+辨识度.txt",
        "窗口与辨识度",
    ),
    "03-loss-qa": (
        "03-loss-qa/01_2026-3-15直播解读：面对亏损该如何正确对待交易？.txt",
        "面对亏损",
    ),
    "04-trading-plan": (
        "04-trading-plan/01_2026-3-22直播：如何制定交易计划表？.txt",
        "交易计划表",
    ),
    "05-new-theme": (
        "05-new-theme/01_2025-7-27直播：新题材爆发怎么做.txt",
        "新题材爆发",
    ),
    "06-short-term-terms": (
        "06-short-term-terms/01_2025-11-16直播：短线交易【名词解释】.txt",
        "短线交易名词解释",
    ),
}
EXPECTED_RULE_KEYS = {
    "mode_key",
    "name",
    "family",
    "style",
    "window",
    "automation_level",
    "priority",
    "role",
    "requirements",
    "entry",
    "invalidation",
    "exit",
    "source_refs",
}
EXPECTED_CATALOG_CANONICAL_SHA256 = (
    "975b3dd811b6e27ec1e576349068356569070c17978ab1c25eb14d3bc7643af1"
)


class TradingPlaybookRuleCatalogTests(unittest.TestCase):
    def test_catalog_matches_exact_version_one_contract(self):
        catalog = RuleCatalog(CATALOG_PATH).load()

        self.assertEqual(set(catalog), {"catalog_version", "sources", "rules"})
        self.assertEqual(catalog["catalog_version"], 1)
        self.assertEqual(len(catalog["sources"]), 8)
        self.assertEqual(len(catalog["rules"]), 19)

        for source in catalog["sources"]:
            self.assertEqual(
                set(source),
                {"source_key", "source_path", "source_title"},
            )
        actual_sources = {
            source["source_key"]: (
                source["source_path"],
                source["source_title"],
            )
            for source in catalog["sources"]
        }
        self.assertEqual(actual_sources, EXPECTED_SOURCES)

        source_keys = set(EXPECTED_SOURCES)
        mode_keys = [rule["mode_key"] for rule in catalog["rules"]]
        self.assertEqual(len(mode_keys), len(set(mode_keys)))
        for rule in catalog["rules"]:
            self.assertEqual(set(rule), EXPECTED_RULE_KEYS)
            for key in (
                "mode_key",
                "name",
                "family",
                "style",
                "window",
                "automation_level",
                "role",
            ):
                self.assertIsInstance(rule[key], str)
                self.assertTrue(rule[key].strip())
            self.assertIs(type(rule["priority"]), int)
            self.assertIsInstance(rule["requirements"], list)
            self.assertTrue(rule["requirements"])
            for requirement in rule["requirements"]:
                self.assertEqual(set(requirement), {"feature", "op", "value"})
                self.assertIsInstance(requirement["feature"], str)
                self.assertTrue(requirement["feature"].strip())
                self.assertIsInstance(requirement["op"], str)
                self.assertTrue(requirement["op"].strip())
                self.assertIn(type(requirement["value"]), {str, int, float, bool})
            for trigger_key in ("entry", "invalidation", "exit"):
                self.assertEqual(set(rule[trigger_key]), {"label"})
                self.assertIsInstance(rule[trigger_key]["label"], str)
                self.assertTrue(rule[trigger_key]["label"].strip())
            self.assertTrue(rule["source_refs"])
            for source_ref in rule["source_refs"]:
                self.assertEqual(set(source_ref), {"source_key", "excerpt"})
                self.assertIsInstance(source_ref["source_key"], str)
                self.assertIsInstance(source_ref["excerpt"], str)
                self.assertIn(source_ref["source_key"], source_keys)
                self.assertTrue(source_ref["excerpt"].strip())

        canonical_payload = json.dumps(
            catalog,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        actual_digest = hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()
        self.assertEqual(
            actual_digest,
            EXPECTED_CATALOG_CANONICAL_SHA256,
            "Catalog content changed. Bump catalog_version and update the exact "
            "catalog expectations intentionally.",
        )

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

    async def test_seed_and_matcher_share_the_exact_canonical_rule_hash(self):
        self._write_all_transcripts()
        helper = getattr(
            rule_catalog_module,
            "canonical_rule_content_hash",
            None,
        )
        self.assertTrue(callable(helper))
        rule = self.catalog_data["rules"][0]

        async with self.session_factory() as session:
            await self.catalog.seed(session, self.source_root)
            stored = await session.scalar(
                select(TradingModeRule).where(
                    TradingModeRule.mode_key == rule["mode_key"]
                )
            )

        expected = helper(rule)
        self.assertEqual(stored.content_hash, expected)
        self.assertEqual(
            ModeMatcher([rule]).rules[0]["content_hash"],
            expected,
        )

    async def test_changed_transcript_appends_source_hash_and_preserves_rules(self):
        self._write_all_transcripts()
        changed_source = self.catalog_data["sources"][0]
        transcript_path = self.source_root / changed_source["source_path"]
        original_content = transcript_path.read_bytes()
        original_hash = hashlib.sha256(original_content).hexdigest()

        async with self.session_factory() as session:
            await self.catalog.seed(session, self.source_root)

        changed_content = original_content + b"changed transcript bytes"
        transcript_path.write_bytes(changed_content)
        changed_hash = hashlib.sha256(changed_content).hexdigest()
        async with self.session_factory() as session:
            await self.catalog.seed(session, self.source_root)

        async with self.session_factory() as session:
            source_rows = (
                await session.scalars(select(TradingRuleSource))
            ).all()
            rule_count = await session.scalar(
                select(func.count()).select_from(TradingModeRule)
            )

        changed_source_rows = [
            row
            for row in source_rows
            if row.source_key == changed_source["source_key"]
        ]
        changed_source_hashes = {
            row.content_hash for row in changed_source_rows
        }
        self.assertEqual(len(source_rows), 9)
        self.assertEqual(len(changed_source_rows), 2)
        self.assertEqual(changed_source_hashes, {original_hash, changed_hash})
        self.assertTrue(all(len(value) == 64 for value in changed_source_hashes))
        self.assertIn(original_hash, changed_source_hashes)
        self.assertEqual(rule_count, 19)

    async def test_rule_insert_failure_rolls_back_source_and_rule_rows(self):
        self._write_all_transcripts()
        failing_mode_key = self.catalog_data["rules"][0]["mode_key"]
        failure_observed = []
        source_counts_at_failure = []

        def fail_selected_rule(_mapper, connection, target):
            if target.mode_key == failing_mode_key:
                failure_observed.append(target.mode_key)
                source_counts_at_failure.append(
                    connection.scalar(
                        select(func.count()).select_from(TradingRuleSource)
                    )
                )
                raise RuntimeError("forced rule insert failure after source DML")

        event.listen(TradingModeRule, "before_insert", fail_selected_rule)
        try:
            async with self.session_factory() as session:
                with self.assertRaisesRegex(
                    RuntimeError,
                    "forced rule insert failure after source DML",
                ):
                    await self.catalog.seed(session, self.source_root)
        finally:
            event.remove(TradingModeRule, "before_insert", fail_selected_rule)

        self.assertEqual(failure_observed, [failing_mode_key])
        self.assertEqual(source_counts_at_failure, [8])
        self.assertEqual(await self._counts(), (0, 0))

    async def test_missing_transcript_lists_relative_path_before_seed_writes(self):
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
