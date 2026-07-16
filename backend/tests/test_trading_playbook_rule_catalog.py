import hashlib
import json
import re
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import event, func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models import TradingModeRule, TradingRuleSource
from app.services.trading_playbook.mode_matcher import ModeMatcher
from app.services.trading_playbook.rule_catalog import RuleCatalog
from app.services.trading_playbook import rule_catalog as rule_catalog_module
from app.services.trading_playbook.rule_catalog import canonical_rule_source_refs


CATALOG_V1_PATH = (
    Path(__file__).resolve().parents[1]
    / "app"
    / "data"
    / "trading_playbook_rules_v1.json"
)
CATALOG_PATH = CATALOG_V1_PATH.with_name("trading_playbook_rules_v2.json")

EXPECTED_SOURCES = {
    "00-art-1123": (
        "00-zgjys-live/01_zgjys-art-trading-1123.txt",
        "交易的艺术 1123",
        "3606c1599ad4aab942fb7c9936e9b178dca154d2eeed049e0982f77469c457db",
    ),
    "00-art-1130": (
        "00-zgjys-live/02_zgjys-art-trading-1130.txt",
        "交易的艺术 1130",
        "c8058034ba23a4ae53564280ca38559a3e8d6e6dc842aec74f3c5a6ecc70c06e",
    ),
    "01-specialize": (
        "01-止于心动-专精一艺/01_2025-8-3直播：止于心动，专精一艺.txt",
        "止于心动，专精一艺",
        "f40100a84e366be52c3f41ad1e79cd316a0059abd9b51856059341ffe7cdacdf",
    ),
    "02-window-recognition": (
        "02-window-recognition/01_2026-3-7小灶：窗口+辨识度.txt",
        "窗口与辨识度",
        "168ca42c4a35fc66a70bf0c0bbf610c16ed6652570fb545209a8dea85cce63e6",
    ),
    "03-loss-qa": (
        "03-loss-qa/01_2026-3-15直播解读：面对亏损该如何正确对待交易？.txt",
        "面对亏损",
        "4d75ee9a8e174fb16e00b129c69c7dc1424f9a037b78c6887ee0ee4b6b4c7fbb",
    ),
    "04-trading-plan": (
        "04-trading-plan/01_2026-3-22直播：如何制定交易计划表？.txt",
        "交易计划表",
        "03827eacc2922c003725862d27fe9619f7739b67e6578a85192a0a48044c1edd",
    ),
    "05-new-theme": (
        "05-new-theme/01_2025-7-27直播：新题材爆发怎么做.txt",
        "新题材爆发",
        "c12c086950b9d47d59ec5c776fd98329fcc6a003f98af246b9f64b072312be68",
    ),
    "06-short-term-terms": (
        "06-short-term-terms/01_2025-11-16直播：短线交易【名词解释】.txt",
        "短线交易名词解释",
        "2a317a65b9cc8483aa6fa376695ef293316dd41c5c157c1187bb983e0ccb099b",
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
    "151ff09ff0821871d6f68f5c22131eb41e65513f37015775a9595d9d83aca496"
)
EXPECTED_V1_CATALOG_CANONICAL_SHA256 = (
    "975b3dd811b6e27ec1e576349068356569070c17978ab1c25eb14d3bc7643af1"
)


def _version_two_catalog_payload():
    payload = json.loads(CATALOG_V1_PATH.read_text(encoding="utf-8"))
    payload["catalog_version"] = 2
    source_hashes = {
        source_key: values[2] for source_key, values in EXPECTED_SOURCES.items()
    }
    for source in payload["sources"]:
        source["content_hash"] = source_hashes[source["source_key"]]
    for rule in payload["rules"]:
        for source_ref in rule["source_refs"]:
            source_ref["source_content_hash"] = source_hashes[
                source_ref["source_key"]
            ]
    return payload


def _historical_v1_rule_content_hash(rule):
    payload = json.loads(json.dumps(rule))
    payload.pop("version", None)
    payload.pop("content_hash", None)
    payload.pop("source_hashes", None)
    for source_ref in payload["source_refs"]:
        source_ref.pop("source_content_hash", None)
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class TradingPlaybookRuleCatalogTests(unittest.TestCase):
    def test_version_one_catalog_remains_the_unchanged_historical_contract(self):
        catalog = json.loads(CATALOG_V1_PATH.read_text(encoding="utf-8"))

        self.assertEqual(catalog["catalog_version"], 1)
        self.assertTrue(
            all(
                set(source) == {"source_key", "source_path", "source_title"}
                for source in catalog["sources"]
            )
        )
        self.assertTrue(
            all(
                set(source_ref) == {"source_key", "excerpt"}
                for rule in catalog["rules"]
                for source_ref in rule["source_refs"]
            )
        )
        canonical_payload = json.dumps(
            catalog,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        self.assertEqual(
            hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest(),
            EXPECTED_V1_CATALOG_CANONICAL_SHA256,
        )

    def test_catalog_matches_exact_version_two_contract(self):
        self.assertTrue(CATALOG_PATH.is_file(), "current v2 catalog is required")
        catalog = RuleCatalog(CATALOG_PATH).load()

        self.assertEqual(set(catalog), {"catalog_version", "sources", "rules"})
        self.assertEqual(catalog["catalog_version"], 2)
        self.assertEqual(len(catalog["sources"]), 8)
        self.assertEqual(len(catalog["rules"]), 19)

        for source in catalog["sources"]:
            self.assertEqual(
                set(source),
                {"source_key", "source_path", "source_title", "content_hash"},
            )
        actual_sources = {
            source["source_key"]: (
                source["source_path"],
                source["source_title"],
                source["content_hash"],
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
                self.assertEqual(
                    set(source_ref),
                    {"source_key", "excerpt", "source_content_hash"},
                )
                self.assertIsInstance(source_ref["source_key"], str)
                self.assertIsInstance(source_ref["excerpt"], str)
                self.assertIn(source_ref["source_key"], source_keys)
                self.assertTrue(source_ref["excerpt"].strip())
                self.assertEqual(
                    source_ref["source_content_hash"],
                    EXPECTED_SOURCES[source_ref["source_key"]][2],
                )

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

    def test_catalog_version_error_uses_the_current_expected_version(self):
        catalog_data = _version_two_catalog_payload()
        catalog_data["catalog_version"] = 1
        with tempfile.TemporaryDirectory() as temporary_directory:
            catalog_path = Path(temporary_directory) / "old.json"
            catalog_path.write_text(
                json.dumps(catalog_data, ensure_ascii=False),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "catalog_version must be 2"):
                RuleCatalog(catalog_path).load()

    def test_verify_sources_hashes_present_transcript_and_marks_missing_source(self):
        raw_content = "交易规则\n".encode("utf-8")
        sources = [
            {
                "source_key": "present",
                "source_path": "nested/present.txt",
                "source_title": "Present",
                "content_hash": hashlib.sha256(raw_content).hexdigest(),
            },
            {
                "source_key": "missing",
                "source_path": "nested/missing.txt",
                "source_title": "Missing",
                "content_hash": "0" * 64,
            },
        ]

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

        with tempfile.TemporaryDirectory() as temporary_directory:
            source_root = Path(temporary_directory)
            transcript_path = source_root / sources[0]["source_path"]
            transcript_path.parent.mkdir(parents=True)
            transcript_path.write_bytes(raw_content + b"changed")
            with self.assertRaisesRegex(ValueError, "version bump"):
                RuleCatalog(CATALOG_PATH).verify_sources(source_root, sources)

    def test_verify_sources_rejects_absolute_and_parent_escape_paths(self):
        raw_content = b"outside transcript"
        content_hash = hashlib.sha256(raw_content).hexdigest()
        with tempfile.TemporaryDirectory() as temporary_directory:
            base = Path(temporary_directory)
            source_root = base / "root"
            source_root.mkdir()
            outside = base / "outside.txt"
            outside.write_bytes(raw_content)

            for source_path in (str(outside), "../outside.txt"):
                with self.subTest(source_path=source_path):
                    with self.assertRaisesRegex(ValueError, "source_path"):
                        RuleCatalog(CATALOG_PATH).verify_sources(
                            source_root,
                            [
                                {
                                    "source_key": "escape",
                                    "source_path": source_path,
                                    "source_title": "Escape",
                                    "content_hash": content_hash,
                                }
                            ],
                        )

    def test_verify_sources_rejects_symlink_that_resolves_outside_root(self):
        raw_content = b"outside transcript"
        content_hash = hashlib.sha256(raw_content).hexdigest()
        with tempfile.TemporaryDirectory() as temporary_directory:
            base = Path(temporary_directory)
            source_root = base / "root"
            source_root.mkdir()
            outside = base / "outside.txt"
            outside.write_bytes(raw_content)
            link = source_root / "linked.txt"
            resolve_context = None
            try:
                link.symlink_to(outside)
            except OSError:
                original_resolve = Path.resolve

                def resolve_as_external_link(path, *args, **kwargs):
                    if path.name == "linked.txt":
                        return outside
                    return original_resolve(path, *args, **kwargs)

                resolve_context = patch.object(
                    Path,
                    "resolve",
                    autospec=True,
                    side_effect=resolve_as_external_link,
                )

            if resolve_context is None:
                with self.assertRaisesRegex(ValueError, "source_path"):
                    RuleCatalog(CATALOG_PATH).verify_sources(
                        source_root,
                        [
                            {
                                "source_key": "linked",
                                "source_path": "linked.txt",
                                "source_title": "Linked",
                                "content_hash": content_hash,
                            }
                        ],
                    )
            else:
                with resolve_context:
                    with self.assertRaisesRegex(ValueError, "source_path"):
                        RuleCatalog(CATALOG_PATH).verify_sources(
                            source_root,
                            [
                                {
                                    "source_key": "linked",
                                    "source_path": "linked.txt",
                                    "source_title": "Linked",
                                    "content_hash": content_hash,
                                }
                            ],
                        )

    def test_source_refs_reject_unknown_fields_in_helper_and_catalog(self):
        catalog_data = _version_two_catalog_payload()
        changed_rule = json.loads(json.dumps(catalog_data["rules"][0]))
        changed_rule["source_refs"][0]["unvalidated_note"] = "must not persist"
        with self.assertRaisesRegex(ValueError, "source_ref.*keys"):
            canonical_rule_source_refs(changed_rule)

        catalog_data["rules"][0] = changed_rule
        with tempfile.TemporaryDirectory() as temporary_directory:
            changed_catalog_path = Path(temporary_directory) / "changed.json"
            changed_catalog_path.write_text(
                json.dumps(catalog_data, ensure_ascii=False),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "source_ref.*keys"):
                RuleCatalog(changed_catalog_path).load()


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
        raw_catalog = _version_two_catalog_payload()
        self.transcript_contents = {
            source["source_key"]: (
                f'{source["source_title"]}\n测试转写内容\n'.encode("utf-8")
            )
            for source in raw_catalog["sources"]
        }
        source_hashes = {
            source_key: hashlib.sha256(content).hexdigest()
            for source_key, content in self.transcript_contents.items()
        }
        for source in raw_catalog["sources"]:
            source["content_hash"] = source_hashes[source["source_key"]]
        for rule in raw_catalog["rules"]:
            for source_ref in rule["source_refs"]:
                source_ref["source_content_hash"] = source_hashes[
                    source_ref["source_key"]
                ]
        test_catalog_path = (
            Path(self.temporary_directory.name) / "test-catalog.json"
        )
        test_catalog_path.write_text(
            json.dumps(raw_catalog, ensure_ascii=False),
            encoding="utf-8",
        )
        self.catalog = RuleCatalog(test_catalog_path)
        self.catalog_data = raw_catalog

    async def asyncTearDown(self):
        self.temporary_directory.cleanup()
        await self.engine.dispose()

    def _write_all_transcripts(self):
        for source in self.catalog_data["sources"]:
            transcript_path = self.source_root / source["source_path"]
            transcript_path.parent.mkdir(parents=True, exist_ok=True)
            transcript_path.write_bytes(
                self.transcript_contents[source["source_key"]]
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

    async def test_seed_v2_preserves_v1_and_is_idempotent_after_migration(self):
        self._write_all_transcripts()
        historical_rules = []
        for rule in self.catalog_data["rules"]:
            historical_refs = [
                {
                    "source_key": source_ref["source_key"],
                    "excerpt": source_ref["excerpt"],
                }
                for source_ref in rule["source_refs"]
            ]
            historical_rules.append(
                TradingModeRule(
                    mode_key=rule["mode_key"],
                    version=1,
                    name=rule["name"],
                    family=rule["family"],
                    style=rule["style"],
                    window=rule["window"],
                    automation_level=rule["automation_level"],
                    description="",
                    prerequisites_json={
                        "requirements": rule["requirements"],
                        "priority": rule["priority"],
                        "role": rule["role"],
                    },
                    candidate_filters_json=[],
                    entry_trigger_json=rule["entry"],
                    invalidation_json=rule["invalidation"],
                    exit_trigger_json=rule["exit"],
                    risk_guidance_json={},
                    source_refs_json=historical_refs,
                    enabled=True,
                    content_hash=_historical_v1_rule_content_hash(rule),
                )
            )

        async with self.session_factory() as session:
            session.add_all(
                [
                    TradingRuleSource(
                        source_key=source["source_key"],
                        source_path=source["source_path"],
                        source_title=source["source_title"],
                        content_hash=source["content_hash"],
                        status="ready",
                    )
                    for source in self.catalog_data["sources"]
                ]
                + historical_rules
            )
            await session.commit()

        async with self.session_factory() as session:
            v1_before = [
                (
                    row.mode_key,
                    row.content_hash,
                    json.loads(json.dumps(row.source_refs_json)),
                )
                for row in (
                    await session.scalars(
                        select(TradingModeRule)
                        .where(TradingModeRule.version == 1)
                        .order_by(TradingModeRule.mode_key)
                    )
                ).all()
            ]
            first_result = await self.catalog.seed(session, self.source_root)

        self.assertEqual(first_result, {"sources": 8, "rules": 19})
        async with self.session_factory() as session:
            all_rules = (
                await session.scalars(
                    select(TradingModeRule).order_by(
                        TradingModeRule.version,
                        TradingModeRule.mode_key,
                    )
                )
            ).all()
            source_count = await session.scalar(
                select(func.count()).select_from(TradingRuleSource)
            )
        self.assertEqual(source_count, 8)
        self.assertEqual(len(all_rules), 38)
        self.assertEqual(
            [sum(row.version == version for row in all_rules) for version in (1, 2)],
            [19, 19],
        )
        self.assertEqual(
            [
                (
                    row.mode_key,
                    row.content_hash,
                    row.source_refs_json,
                )
                for row in all_rules
                if row.version == 1
            ],
            v1_before,
        )
        self.assertTrue(
            all(
                row.source_refs_json
                == canonical_rule_source_refs(
                    next(
                        rule
                        for rule in self.catalog_data["rules"]
                        if rule["mode_key"] == row.mode_key
                    )
                )
                for row in all_rules
                if row.version == 2
            )
        )

        async with self.session_factory() as session:
            second_result = await self.catalog.seed(session, self.source_root)
        self.assertEqual(second_result, {"sources": 8, "rules": 19})
        self.assertEqual(await self._counts(), (8, 38))

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
            stored.source_refs_json,
            canonical_rule_source_refs(rule),
        )
        self.assertEqual(
            ModeMatcher(
                [rule],
                catalog_version=self.catalog_data["catalog_version"],
            ).rules[0]["content_hash"],
            expected,
        )
        self.assertEqual(
            ModeMatcher(
                [rule],
                catalog_version=self.catalog_data["catalog_version"],
            ).rule_snapshot()[0]["source_hashes"],
            [
                {
                    "source_key": source_ref["source_key"],
                    "content_hash": source_ref["source_content_hash"],
                }
                for source_ref in sorted(
                    rule["source_refs"],
                    key=lambda item: item["source_key"],
                )
            ],
        )

    async def test_changed_transcript_is_rejected_without_version_bump_atomically(self):
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
            with self.assertRaisesRegex(ValueError, "version bump"):
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
        self.assertEqual(len(source_rows), 8)
        self.assertEqual(len(changed_source_rows), 1)
        self.assertEqual(changed_source_hashes, {original_hash})
        self.assertTrue(all(len(value) == 64 for value in changed_source_hashes))
        self.assertIn(original_hash, changed_source_hashes)
        self.assertNotIn(changed_hash, changed_source_hashes)
        self.assertEqual(rule_count, 19)

    async def test_load_with_source_root_validates_declared_transcript_hashes(self):
        self._write_all_transcripts()

        loaded = self.catalog.load(self.source_root)

        self.assertTrue(all(row["status"] == "ready" for row in loaded["sources"]))

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
