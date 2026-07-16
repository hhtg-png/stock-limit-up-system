# Trading Playbook Alerts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a transcript-derived daily trading playbook that generates versioned plans at 14:40, 15:30, 08:50, and 09:26, requires manual confirmation, sends isolated in-app reminders, and reviews execution after close.

**Architecture:** Add a focused `trading_playbook` backend package containing source ingestion, normalized market snapshots, deterministic feature extraction, a manifest-driven mode matcher, immutable plan generation, alert monitoring, and review services. Persist rules, plans, candidates, alerts, settings, and reviews in new SQLAlchemy tables; expose them through a dedicated FastAPI router and a standalone Vue page/store that does not reuse the global alert store.

**Tech Stack:** Python 3.10+, FastAPI, SQLAlchemy async, SQLite, APScheduler, unittest, Vue 3, TypeScript, Pinia, Element Plus, Node test runner.

---

## File map

### Backend files to create

- `backend/app/models/trading_playbook.py` — persistent rule sources, rules, plan versions, candidates, alert events, reviews, and settings.
- `backend/app/schemas/trading_playbook.py` — API request/response schemas and enums.
- `backend/app/services/trading_playbook/__init__.py` — package exports.
- `backend/app/services/trading_playbook/domain.py` — normalized snapshots and evaluation dataclasses.
- `backend/app/services/trading_playbook/rule_catalog.py` — load, validate, hash, and seed the curated rule catalog.
- `backend/app/services/trading_playbook/market_data.py` — full-market/candidate quote snapshots, speed calculation, auction fields, and quality metadata.
- `backend/app/services/trading_playbook/market_state.py` — market style, window, theme ranking, and recognition ranking.
- `backend/app/services/trading_playbook/mode_features.py` — derive all mode prerequisites from current and historical snapshots.
- `backend/app/services/trading_playbook/mode_matcher.py` — deterministic manifest condition evaluation for all 19 modes.
- `backend/app/services/trading_playbook/plan_service.py` — immutable plan versions, three-candidate limit, manual revisions, and confirmation.
- `backend/app/services/trading_playbook/orchestrator.py` — build one stage end-to-end from source dates, market facts, rules, mode evaluations, and persistence.
- `backend/app/services/trading_playbook/channels.py` — notification interface and in-app implementation.
- `backend/app/services/trading_playbook/alert_service.py` — active-plan monitoring, deduplication, persistence, and broadcast.
- `backend/app/services/trading_playbook/review_service.py` — 15:10 review and 15:30 reconciliation.
- `backend/app/api/v1/trading_playbook.py` — dedicated REST API.
- `backend/app/scripts/import_trading_playbook_rules.py` — explicit transcript-source verification and rule seeding command.
- `backend/app/scripts/replay_trading_playbook.py` — point-in-time historical replay command.
- `backend/app/data/trading_playbook_rules_v1.json` — curated catalog containing all 19 transcript-derived modes and source references.
- `backend/tests/test_trading_playbook_models.py`
- `backend/tests/test_trading_playbook_rule_catalog.py`
- `backend/tests/test_trading_playbook_market_data.py`
- `backend/tests/test_trading_playbook_market_state.py`
- `backend/tests/test_trading_playbook_mode_matcher.py`
- `backend/tests/test_trading_playbook_plan_service.py`
- `backend/tests/test_trading_playbook_orchestrator.py`
- `backend/tests/test_trading_playbook_api.py`
- `backend/tests/test_trading_playbook_scheduler.py`
- `backend/tests/test_trading_playbook_alerts.py`
- `backend/tests/test_trading_playbook_review.py`
- `backend/tests/test_trading_playbook_replay.py`

### Backend files to modify

- `backend/app/models/__init__.py` — register new models before `Base.metadata.create_all()`.
- `backend/app/api/v1/__init__.py` — mount `/trading-playbook` router.
- `backend/app/config.py` — feature flag, transcript root, polling interval, and default risk settings.
- `backend/app/data_collectors/tencent_api.py` — support Beijing exchange symbols and safe quote chunking.
- `backend/app/data_collectors/scheduler.py` — five trading-day jobs plus the active-plan monitor.
- `backend/app/core/websocket_manager.py` — subscribe and broadcast `trading_plan_alert` without using existing alert semantics.
- `backend/app/main.py` — no new lifecycle service; scheduler remains the single owner of background execution.

### Frontend files to create

- `frontend/src/types/trading-playbook.ts` — plan, rule, candidate, alert, review, and settings types.
- `frontend/src/api/trading-playbook.ts` — REST client.
- `frontend/src/stores/trading-playbook.ts` — isolated plan and reminder state.
- `frontend/src/views/TradingPlaybook.vue` — standalone plan UI.
- `frontend/tests/tradingPlaybookApi.test.mjs`
- `frontend/tests/tradingPlaybookStore.test.mjs`
- `frontend/tests/tradingPlaybookUi.test.mjs`
- `frontend/tests/tradingPlaybookRoutes.test.mjs`

### Frontend files to modify

- `frontend/src/router/index.ts` — register `/trading-playbook`.
- `frontend/src/App.vue` — add desktop and mobile navigation entries only.
- `frontend/src/composables/useWebSocket.ts` — route `trading_plan_alert` into the isolated store.

## Task 1: Add persistent trading playbook models

**Files:**
- Create: `backend/app/models/trading_playbook.py`
- Modify: `backend/app/models/__init__.py`
- Test: `backend/tests/test_trading_playbook_models.py`

- [ ] **Step 1: Write the failing model metadata test**

```python
import unittest

from app.database import Base
import app.models  # noqa: F401


class TradingPlaybookModelTests(unittest.TestCase):
    def test_all_trading_playbook_tables_are_registered(self):
        expected = {
            "trading_rule_sources",
            "trading_mode_rules",
            "trading_plan_versions",
            "trading_plan_candidates",
            "trading_alert_events",
            "trading_execution_reviews",
            "trading_playbook_settings",
        }
        self.assertTrue(expected.issubset(set(Base.metadata.tables)))

    def test_plan_version_uses_source_target_and_parent_columns(self):
        table = Base.metadata.tables["trading_plan_versions"]
        self.assertIn("source_trade_date", table.c)
        self.assertIn("target_trade_date", table.c)
        self.assertIn("parent_plan_version_id", table.c)

    def test_candidate_has_action_trade_date(self):
        table = Base.metadata.tables["trading_plan_candidates"]
        self.assertIn("action_trade_date", table.c)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the model test and verify it fails**

Working directory: `backend`

Run: `python -m unittest tests.test_trading_playbook_models -v`

Expected: FAIL because `trading_plan_versions` and the other new tables are absent.

- [ ] **Step 3: Create the model module and register it**

Create `backend/app/models/trading_playbook.py` with these exact table names, unique constraints, and JSON defaults:

```python
from datetime import datetime

from sqlalchemy import Boolean, Column, Date, DateTime, Float, ForeignKey, Integer, JSON, String, Text, UniqueConstraint

from app.database import Base


class TradingRuleSource(Base):
    __tablename__ = "trading_rule_sources"
    __table_args__ = (UniqueConstraint("source_key", "content_hash", name="uq_trading_rule_source_hash"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_key = Column(String(80), nullable=False)
    source_path = Column(String(500), nullable=False)
    source_title = Column(String(255), nullable=False)
    content_hash = Column(String(64), nullable=False)
    transcript_generated_at = Column(DateTime)
    ingested_at = Column(DateTime, default=datetime.now, nullable=False)
    status = Column(String(20), default="ready", nullable=False)


class TradingModeRule(Base):
    __tablename__ = "trading_mode_rules"
    __table_args__ = (UniqueConstraint("mode_key", "version", name="uq_trading_mode_rule_version"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    mode_key = Column(String(80), nullable=False, index=True)
    version = Column(Integer, nullable=False)
    name = Column(String(120), nullable=False)
    family = Column(String(40), nullable=False)
    style = Column(String(40), nullable=False)
    window = Column(String(80), nullable=False)
    automation_level = Column(String(20), nullable=False)
    description = Column(Text, default="", nullable=False)
    prerequisites_json = Column(JSON, default=dict, nullable=False)
    candidate_filters_json = Column(JSON, default=list, nullable=False)
    entry_trigger_json = Column(JSON, default=dict, nullable=False)
    invalidation_json = Column(JSON, default=dict, nullable=False)
    exit_trigger_json = Column(JSON, default=dict, nullable=False)
    risk_guidance_json = Column(JSON, default=dict, nullable=False)
    source_refs_json = Column(JSON, default=list, nullable=False)
    enabled = Column(Boolean, default=True, nullable=False)
    content_hash = Column(String(64), nullable=False)
    created_at = Column(DateTime, default=datetime.now, nullable=False)


class TradingPlanVersion(Base):
    __tablename__ = "trading_plan_versions"
    __table_args__ = (UniqueConstraint("target_trade_date", "stage", "version_no", name="uq_trading_plan_stage_version"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_trade_date = Column(Date, nullable=False, index=True)
    target_trade_date = Column(Date, nullable=False, index=True)
    stage = Column(String(20), nullable=False)
    version_no = Column(Integer, nullable=False)
    parent_plan_version_id = Column(Integer, ForeignKey("trading_plan_versions.id"))
    status = Column(String(20), default="draft", nullable=False)
    market_state_json = Column(JSON, default=dict, nullable=False)
    theme_ranking_json = Column(JSON, default=list, nullable=False)
    mode_radar_json = Column(JSON, default=list, nullable=False)
    rule_snapshot_json = Column(JSON, default=list, nullable=False)
    risk_settings_json = Column(JSON, default=dict, nullable=False)
    data_quality_json = Column(JSON, default=dict, nullable=False)
    change_summary_json = Column(JSON, default=dict, nullable=False)
    input_hash = Column(String(64), nullable=False)
    generated_at = Column(DateTime, default=datetime.now, nullable=False)
    confirmed_at = Column(DateTime)
    confirmed_by = Column(String(80))


class TradingPlanCandidate(Base):
    __tablename__ = "trading_plan_candidates"
    __table_args__ = (UniqueConstraint("plan_version_id", "stock_code", "primary_mode_key", name="uq_trading_plan_candidate"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    plan_version_id = Column(Integer, ForeignKey("trading_plan_versions.id"), nullable=False, index=True)
    stock_code = Column(String(10), nullable=False, index=True)
    stock_name = Column(String(50), nullable=False)
    action_trade_date = Column(Date, nullable=False, index=True)
    theme_name = Column(String(120), default="", nullable=False)
    primary_mode_key = Column(String(80), nullable=False)
    supporting_mode_keys_json = Column(JSON, default=list, nullable=False)
    role = Column(String(60), nullable=False)
    rank = Column(Integer, nullable=False)
    recognition_json = Column(JSON, default=dict, nullable=False)
    entry_trigger_json = Column(JSON, default=dict, nullable=False)
    invalidation_json = Column(JSON, default=dict, nullable=False)
    exit_trigger_json = Column(JSON, default=dict, nullable=False)
    risk_level = Column(String(20), nullable=False)
    position_reference = Column(Float, default=0, nullable=False)
    evidence_json = Column(JSON, default=list, nullable=False)
    manual_overrides_json = Column(JSON, default=dict, nullable=False)
    status = Column(String(20), default="waiting", nullable=False)


class TradingAlertEvent(Base):
    __tablename__ = "trading_alert_events"
    __table_args__ = (UniqueConstraint("dedup_key", name="uq_trading_alert_dedup"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    plan_version_id = Column(Integer, ForeignKey("trading_plan_versions.id"), nullable=False, index=True)
    candidate_id = Column(Integer, ForeignKey("trading_plan_candidates.id"), index=True)
    event_type = Column(String(40), nullable=False)
    severity = Column(String(20), nullable=False)
    dedup_key = Column(String(255), nullable=False)
    triggered_at = Column(DateTime, default=datetime.now, nullable=False)
    market_snapshot_json = Column(JSON, default=dict, nullable=False)
    message = Column(Text, nullable=False)
    channel_status_json = Column(JSON, default=dict, nullable=False)
    acknowledged_at = Column(DateTime)


class TradingExecutionReview(Base):
    __tablename__ = "trading_execution_reviews"
    __table_args__ = (UniqueConstraint("trade_date", "plan_version_id", name="uq_trading_execution_review"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(Date, nullable=False, index=True)
    plan_version_id = Column(Integer, ForeignKey("trading_plan_versions.id"), nullable=False)
    signal_review_json = Column(JSON, default=dict, nullable=False)
    manual_execution_json = Column(JSON, default=dict, nullable=False)
    plan_compliance_json = Column(JSON, default=dict, nullable=False)
    outcome_snapshot_json = Column(JSON, default=dict, nullable=False)
    data_quality_json = Column(JSON, default=dict, nullable=False)
    generated_at = Column(DateTime, default=datetime.now, nullable=False)
    finalized_at = Column(DateTime)


class TradingPlaybookSettings(Base):
    __tablename__ = "trading_playbook_settings"

    id = Column(Integer, primary_key=True, default=1)
    enabled = Column(Boolean, default=True, nullable=False)
    trial_position_pct = Column(Float, default=10, nullable=False)
    confirmed_position_pct = Column(Float, default=30, nullable=False)
    hard_stop_pct = Column(Float, default=5, nullable=False)
    max_action_candidates = Column(Integer, default=3, nullable=False)
    in_app_enabled = Column(Boolean, default=True, nullable=False)
    wechat_enabled = Column(Boolean, default=False, nullable=False)
    channel_config_json = Column(JSON, default=dict, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)
```

Add all seven classes to the imports and `__all__` list in `backend/app/models/__init__.py`.

- [ ] **Step 4: Run the model test and verify it passes**

Working directory: `backend`

Run: `python -m unittest tests.test_trading_playbook_models -v`

Expected: 3 tests PASS.

- [ ] **Step 5: Commit the model layer**

```bash
git add backend/app/models/trading_playbook.py backend/app/models/__init__.py backend/tests/test_trading_playbook_models.py
git commit -m "feat: add trading playbook models"
```

## Task 2: Add the complete transcript-derived rule catalog and importer

**Files:**
- Create: `backend/app/data/trading_playbook_rules_v1.json`
- Create: `backend/app/services/trading_playbook/__init__.py`
- Create: `backend/app/services/trading_playbook/rule_catalog.py`
- Create: `backend/app/scripts/import_trading_playbook_rules.py`
- Modify: `backend/app/config.py`
- Test: `backend/tests/test_trading_playbook_rule_catalog.py`

- [ ] **Step 1: Write failing catalog coverage and source verification tests**

```python
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.services.trading_playbook.rule_catalog import RuleCatalog


class TradingPlaybookRuleCatalogTests(unittest.IsolatedAsyncioTestCase):
    def test_catalog_contains_all_nineteen_modes(self):
        path = Path("app/data/trading_playbook_rules_v1.json")
        rules = json.loads(path.read_text(encoding="utf-8"))["rules"]
        self.assertEqual(len(rules), 19)
        self.assertEqual(len({rule["mode_key"] for rule in rules}), 19)
        self.assertTrue(all(rule["source_refs"] for rule in rules))

    def test_verify_sources_hashes_every_present_transcript(self):
        with TemporaryDirectory() as root:
            source = Path(root) / "lesson.txt"
            source.write_text("窗口、方向、辨识度", encoding="utf-8")
            catalog = RuleCatalog(catalog_path=Path("app/data/trading_playbook_rules_v1.json"))
            result = catalog.verify_sources(Path(root), [{"source_path": "lesson.txt", "source_title": "课程"}])
            self.assertEqual(result[0]["status"], "ready")
            self.assertEqual(len(result[0]["content_hash"]), 64)

    async def test_seed_rejects_content_change_without_version_bump(self):
        payload = json.loads(Path("app/data/trading_playbook_rules_v1.json").read_text(encoding="utf-8"))
        with TemporaryDirectory() as root:
            root_path = Path(root)
            catalog_path = root_path / "catalog.json"
            source_root = root_path / "transcripts"
            for source in payload["sources"]:
                path = source_root / source["source_path"]
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(source["source_title"], encoding="utf-8")
            catalog_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            engine = create_async_engine("sqlite+aiosqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
            Session = async_sessionmaker(engine, expire_on_commit=False)
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            async with Session() as db:
                await RuleCatalog(catalog_path).seed(db, source_root)
            payload["rules"][0]["name"] = "被错误覆盖的名称"
            catalog_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            async with Session() as db:
                with self.assertRaisesRegex(ValueError, "version bump"):
                    await RuleCatalog(catalog_path).seed(db, source_root)
            await engine.dispose()


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the catalog tests and verify they fail**

Working directory: `backend`

Run: `python -m unittest tests.test_trading_playbook_rule_catalog -v`

Expected: FAIL because the catalog and `RuleCatalog` do not exist.

- [ ] **Step 3: Create the catalog, validator, and import command**

Create the JSON document with `catalog_version: 1`, the eight source paths from the approved design, and these 19 rule objects:

```json
{
  "catalog_version": 1,
  "sources": [
    {"source_key":"00-art-1123","source_path": "00-zgjys-live/01_zgjys-art-trading-1123.txt", "source_title": "交易的艺术 1123"},
    {"source_key":"00-art-1130","source_path": "00-zgjys-live/02_zgjys-art-trading-1130.txt", "source_title": "交易的艺术 1130"},
    {"source_key":"01-specialize","source_path": "01-止于心动-专精一艺/01_2025-8-3直播：止于心动，专精一艺.txt", "source_title": "止于心动，专精一艺"},
    {"source_key":"02-window-recognition","source_path": "02-window-recognition/01_2026-3-7小灶：窗口+辨识度.txt", "source_title": "窗口与辨识度"},
    {"source_key":"03-loss-qa","source_path": "03-loss-qa/01_2026-3-15直播解读：面对亏损该如何正确对待交易？.txt", "source_title": "面对亏损"},
    {"source_key":"04-trading-plan","source_path": "04-trading-plan/01_2026-3-22直播：如何制定交易计划表？.txt", "source_title": "交易计划表"},
    {"source_key":"05-new-theme","source_path": "05-new-theme/01_2025-7-27直播：新题材爆发怎么做.txt", "source_title": "新题材爆发"},
    {"source_key":"06-short-term-terms","source_path": "06-short-term-terms/01_2025-11-16直播：短线交易【名词解释】.txt", "source_title": "短线交易名词解释"}
  ],
  "rules": [
    {"mode_key":"new_theme_high_volatility","name":"新题材高波动套利","family":"outbreak","style":"dual_active","window":"outbreak","automation_level":"assisted","priority":100,"role":"high_volatility","requirements":[{"feature":"market.window","op":"eq","value":"outbreak"},{"feature":"candidate.high_volatility","op":"eq","value":true},{"feature":"candidate.theme_rank","op":"lte","value":2}],"entry":{"label":"爆发确认后高波动先手"},"invalidation":{"label":"题材扩散失败或高波动掉队"},"exit":{"label":"加速兑现或次日不及预期"},"source_refs":[{"source_key":"05-new-theme","excerpt":"爆发当日高波动优先于高身位，优先于同身位换手"},{"source_key":"01-specialize","excerpt":"重大题材起爆当日先选择高波动套利"}]},
    {"mode_key":"new_theme_high_position","name":"新题材高身位套利","family":"outbreak","style":"board_flow","window":"outbreak","automation_level":"assisted","priority":95,"role":"high_position","requirements":[{"feature":"market.window","op":"eq","value":"outbreak"},{"feature":"candidate.high_position","op":"eq","value":true}],"entry":{"label":"新题材确认后的高身位套利"},"invalidation":{"label":"放量后不能承接"},"exit":{"label":"放量兑现"},"source_refs":[{"source_key":"05-new-theme","excerpt":"高波动优先于高身位，高身位优先于同身位换手"}]},
    {"mode_key":"new_theme_same_level_turnover","name":"新题材同身位换手","family":"outbreak","style":"board_flow","window":"outbreak","automation_level":"assisted","priority":85,"role":"same_level_turnover","requirements":[{"feature":"market.window","op":"eq","value":"outbreak"},{"feature":"candidate.same_level_turnover","op":"eq","value":true}],"entry":{"label":"同身位换手胜出"},"invalidation":{"label":"同身位淘汰"},"exit":{"label":"次日强度下降"},"source_refs":[{"source_key":"05-new-theme","excerpt":"同身位换手不是不好，而是选择更难"}]},
    {"mode_key":"big_middle_army_transition","name":"大中军过渡套利","family":"outbreak","style":"trend_main_wave","window":"outbreak,first_divergence","automation_level":"assisted","priority":90,"role":"middle_army","requirements":[{"feature":"candidate.middle_army","op":"eq","value":true},{"feature":"candidate.theme_rank","op":"lte","value":2}],"entry":{"label":"高波动兑现后的容量承接"},"invalidation":{"label":"中军不能代表板块或板块扩散结束"},"exit":{"label":"大中军兑现或全面分歧"},"source_refs":[{"source_key":"05-new-theme","excerpt":"第一波高波动做完，再做一次大中军套利"},{"source_key":"01-specialize","excerpt":"首次分歧可以选择流动性好的中军"}]},
    {"mode_key":"first_mover_leader","name":"分歧一致先于龙","family":"leader","style":"board_flow","window":"outbreak,first_divergence","automation_level":"assisted","priority":96,"role":"first_mover","requirements":[{"feature":"candidate.started_before_theme","op":"eq","value":true},{"feature":"candidate.recognition_rank","op":"lte","value":3}],"entry":{"label":"个股分歧转一致并带动方向"},"invalidation":{"label":"失去先于性或号召力"},"exit":{"label":"核心地位被取代"},"source_refs":[{"source_key":"02-window-recognition","excerpt":"通过窗口寻找方向和核心辨识度"},{"source_key":"06-short-term-terms","excerpt":"先于板块启动并完成个股分歧转一致"}]},
    {"mode_key":"unique_survivor_trial","name":"唯一活口试错","family":"leader","style":"dual_active","window":"divergence_exhaustion","automation_level":"assisted","priority":98,"role":"survivor","requirements":[{"feature":"candidate.unique_survivor","op":"eq","value":true},{"feature":"market.window","op":"eq","value":"divergence_exhaustion"}],"entry":{"label":"分歧衰竭保留先手"},"invalidation":{"label":"次日补跌或趋势破坏"},"exit":{"label":"活口未转强"},"source_refs":[{"source_key":"05-new-theme","excerpt":"全面分歧后等待杀出唯一活口"},{"source_key":"06-short-term-terms","excerpt":"都死了而它还在，这种叫活口"}]},
    {"mode_key":"leader_turn_two","name":"龙头一转二","family":"leader","style":"board_flow","window":"divergence_to_consensus","automation_level":"automatic","priority":100,"role":"leader","requirements":[{"feature":"candidate.turn_confirmed","op":"eq","value":true},{"feature":"candidate.recognition_rank","op":"eq","value":1}],"entry":{"label":"活口分歧转一致确认"},"invalidation":{"label":"转强失败"},"exit":{"label":"次日不能强更强"},"source_refs":[{"source_key":"00-art-1123","excerpt":"启动之后超预期，再打开第二浪"},{"source_key":"06-short-term-terms","excerpt":"转点确认之后才是推仓位的关键窗口"}]},
    {"mode_key":"leader_stronger_confirmation","name":"龙头强更强确认","family":"leader","style":"board_flow","window":"stronger_confirmation","automation_level":"automatic","priority":100,"role":"confirmed_leader","requirements":[{"feature":"candidate.stronger_confirmed","op":"eq","value":true}],"entry":{"label":"转二后的继续确认"},"invalidation":{"label":"强更强失败或被卡位"},"exit":{"label":"加速转分歧"},"source_refs":[{"source_key":"06-short-term-terms","excerpt":"转二之后隔日强更强，才能确认龙头"}]},
    {"mode_key":"leader_acceleration_to_divergence","name":"龙头加速转分歧","family":"leader","style":"board_flow","window":"second_divergence","automation_level":"assisted","priority":88,"role":"leader_divergence","requirements":[{"feature":"candidate.confirmed_leader","op":"eq","value":true},{"feature":"candidate.acceleration_to_divergence","op":"eq","value":true}],"entry":{"label":"确认龙头的计划内分歧承接"},"invalidation":{"label":"板块负反馈扩散"},"exit":{"label":"分歧无法修复"},"source_refs":[{"source_key":"06-short-term-terms","excerpt":"龙头三个买点包括加速转分歧"}]},
    {"mode_key":"stage_three_high_low_switch","name":"三阶段高低切","family":"rotation","style":"board_flow","window":"stage_three","automation_level":"assisted","priority":86,"role":"high_low_switch","requirements":[{"feature":"market.window","op":"eq","value":"stage_three"},{"feature":"candidate.low_position_new_start","op":"eq","value":true}],"entry":{"label":"龙头分歧窗口的低位首板"},"invalidation":{"label":"未能挡刀或卡位"},"exit":{"label":"低位竞争失败"},"source_refs":[{"source_key":"01-specialize","excerpt":"一致再一致时同时考虑高低切"},{"source_key":"06-short-term-terms","excerpt":"关键转点低位启动形成高低切"}]},
    {"mode_key":"stage_transition_supplement","name":"转点补涨","family":"rotation","style":"board_flow","window":"first_divergence,stage_three","automation_level":"assisted","priority":78,"role":"supplement","requirements":[{"feature":"candidate.supplement","op":"eq","value":true}],"entry":{"label":"关键转点的题材内补涨"},"invalidation":{"label":"启动时点不在关键窗口"},"exit":{"label":"补涨使命完成"},"source_refs":[{"source_key":"06-short-term-terms","excerpt":"转点补涨卡在关键窗口"}]},
    {"mode_key":"leader_first_bearish_rebound","name":"龙头首阴或双头预期","family":"leader","style":"board_flow","window":"stage_three,decline","automation_level":"manual_only","priority":65,"role":"leader_rebound","requirements":[{"feature":"candidate.confirmed_leader","op":"eq","value":true},{"feature":"candidate.first_bearish","op":"eq","value":true}],"entry":{"label":"只对已确认龙头人工判断"},"invalidation":{"label":"普通中位股不适用"},"exit":{"label":"反抽兑现"},"source_refs":[{"source_key":"06-short-term-terms","excerpt":"龙头首阴低吸只适用于已确认龙头"}]},
    {"mode_key":"trend_core_pullback","name":"趋势核心回调","family":"trend","style":"trend_main_wave","window":"first_divergence","automation_level":"assisted","priority":92,"role":"trend_core","requirements":[{"feature":"candidate.trend_established","op":"eq","value":true},{"feature":"candidate.resilience_rank","op":"lte","value":3},{"feature":"candidate.pullback","op":"eq","value":true}],"entry":{"label":"上升趋势中的抗跌核心回调"},"invalidation":{"label":"趋势角度或板块联动破坏"},"exit":{"label":"反弹弱或核心被替代"},"source_refs":[{"source_key":"00-art-1130","excerpt":"向上主升方向在分歧衰竭买核心辨识度"},{"source_key":"02-window-recognition","excerpt":"在正确窗口寻找相对最强的辨识度"}]},
    {"mode_key":"trend_consolidation_rebreak","name":"趋势横盘再突破","family":"trend","style":"trend_main_wave","window":"divergence_to_consensus","automation_level":"automatic","priority":96,"role":"trend_rebreak","requirements":[{"feature":"candidate.consolidation_rebreak","op":"eq","value":true},{"feature":"candidate.linkage_confirmed","op":"eq","value":true}],"entry":{"label":"横盘后新高并获得联动验证"},"invalidation":{"label":"孤证突破或跌回平台"},"exit":{"label":"联动减弱"},"source_refs":[{"source_key":"00-art-1123","excerpt":"一波拉升横盘三到五天后的再突破"},{"source_key":"00-art-1130","excerpt":"个股突破需要板块联动交叉验证"}]},
    {"mode_key":"trend_turn_two","name":"趋势一转二","family":"trend","style":"trend_main_wave","window":"divergence_to_consensus","automation_level":"automatic","priority":99,"role":"trend_turn_two","requirements":[{"feature":"candidate.trend_turn_two","op":"eq","value":true},{"feature":"candidate.middle_army_linkage","op":"eq","value":true}],"entry":{"label":"一浪横盘后的二浪确认"},"invalidation":{"label":"中军和板块不能共振"},"exit":{"label":"二浪趋势破坏"},"source_refs":[{"source_key":"00-art-1123","excerpt":"趋势主升浪转二，横盘后再破新高"},{"source_key":"00-art-1130","excerpt":"二浪必须由中军和板块共振确认"}]},
    {"mode_key":"resilient_core_exhaustion","name":"连续分歧后的抗跌核心","family":"trend","style":"dual_active","window":"divergence_exhaustion","automation_level":"assisted","priority":90,"role":"resilient_core","requirements":[{"feature":"candidate.divergence_days","op":"gte","value":3},{"feature":"candidate.resilience_rank","op":"eq","value":1}],"entry":{"label":"弱中选强后的衰竭承接"},"invalidation":{"label":"分时最弱或领跌"},"exit":{"label":"抗跌特征消失"},"source_refs":[{"source_key":"04-trading-plan","excerpt":"低吸是弱中选强，不是买分时最弱"}]},
    {"mode_key":"alive_theme_snake_arbitrage","name":"板块未死的蛇形套利","family":"trend","style":"dual_active","window":"divergence_exhaustion","automation_level":"manual_only","priority":70,"role":"snake_arbitrage","requirements":[{"feature":"candidate.theme_alive","op":"eq","value":true},{"feature":"candidate.snake_setup","op":"eq","value":true}],"entry":{"label":"补涨仍在时的左侧套利"},"invalidation":{"label":"板块已无补涨和容量"},"exit":{"label":"套利反弹完成"},"source_refs":[{"source_key":"04-trading-plan","excerpt":"板块没走完，行为本质是套利"}]},
    {"mode_key":"dead_pile_right_confirmation","name":"死人堆反转的右侧确认","family":"trend","style":"dual_active","window":"divergence_to_consensus","automation_level":"manual_only","priority":74,"role":"right_confirmation","requirements":[{"feature":"candidate.theme_dead","op":"eq","value":true},{"feature":"candidate.right_reversal","op":"eq","value":true}],"entry":{"label":"死人堆龙头只做右侧确认"},"invalidation":{"label":"提前左侧低吸"},"exit":{"label":"右侧突破失败"},"source_refs":[{"source_key":"04-trading-plan","excerpt":"死人堆爬出来的只能右侧确认"}]},
    {"mode_key":"external_high_low_switch","name":"题材外高低切","family":"rotation","style":"dual_active","window":"first_divergence,stage_three","automation_level":"manual_only","priority":68,"role":"external_switch","requirements":[{"feature":"candidate.external_switch","op":"eq","value":true}],"entry":{"label":"旧主线分歧时的新方向夺权"},"invalidation":{"label":"新方向没有扩散"},"exit":{"label":"夺权失败"},"source_refs":[{"source_key":"01-specialize","excerpt":"高位分歧时题材外高低切夺权"},{"source_key":"06-short-term-terms","excerpt":"题材外新方向必须形成扩散和卡位"}]}
  ]
}
```

Implement `RuleCatalog` with the following behavior:

```python
import hashlib
import json
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.models.trading_playbook import TradingModeRule, TradingRuleSource


class RuleCatalog:
    def __init__(self, catalog_path: Path):
        self.catalog_path = catalog_path

    def load(self) -> dict:
        payload = json.loads(self.catalog_path.read_text(encoding="utf-8"))
        if payload.get("catalog_version") != 1 or len(payload.get("rules", [])) != 19:
            raise ValueError("invalid trading playbook catalog")
        if len({rule["mode_key"] for rule in payload["rules"]}) != 19:
            raise ValueError("duplicate trading playbook mode_key")
        source_keys = {source["source_key"] for source in payload.get("sources", [])}
        for rule in payload["rules"]:
            refs = rule.get("source_refs", [])
            if not refs or any(ref.get("source_key") not in source_keys or not ref.get("excerpt", "").strip() for ref in refs):
                raise ValueError(f"invalid source reference: {rule['mode_key']}")
        return payload

    def verify_sources(self, source_root: Path, sources: list[dict]) -> list[dict]:
        rows = []
        for source in sources:
            path = source_root / source["source_path"]
            if not path.is_file():
                rows.append({**source, "content_hash": "", "status": "missing"})
                continue
            content = path.read_bytes()
            rows.append({**source, "content_hash": hashlib.sha256(content).hexdigest(), "status": "ready"})
        return rows

    async def seed(self, db, source_root: Path) -> dict:
        payload = self.load()
        sources = self.verify_sources(source_root, payload["sources"])
        missing = [row["source_path"] for row in sources if row["status"] != "ready"]
        if missing:
            raise FileNotFoundError("missing transcripts: " + ", ".join(missing))
        for row in sources:
            stmt = sqlite_insert(TradingRuleSource).values(**row)
            stmt = stmt.on_conflict_do_nothing(index_elements=["source_key", "content_hash"])
            await db.execute(stmt)
        version = int(payload["catalog_version"])
        for rule in payload["rules"]:
            canonical = json.dumps(rule, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            content_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
            existing = await db.scalar(select(TradingModeRule).where(
                TradingModeRule.mode_key == rule["mode_key"],
                TradingModeRule.version == version,
            ))
            if existing is not None and existing.content_hash != content_hash:
                raise ValueError(f"immutable rule changed without catalog version bump: {rule['mode_key']}")
            if existing is not None:
                continue
            db.add(TradingModeRule(
                mode_key=rule["mode_key"], version=version, name=rule["name"], family=rule["family"],
                style=rule["style"], window=rule["window"], automation_level=rule["automation_level"],
                description=rule.get("description", ""), prerequisites_json={"requirements": rule["requirements"], "priority": rule["priority"], "role": rule["role"]},
                entry_trigger_json=rule["entry"], invalidation_json=rule["invalidation"], exit_trigger_json=rule["exit"],
                risk_guidance_json={}, source_refs_json=rule["source_refs"], enabled=True, content_hash=content_hash,
            ))
        await db.commit()
        return {"sources": len(sources), "rules": len(payload["rules"])}
```

Rule rows are immutable: changing any normalized rule requires incrementing `catalog_version`; seeding the same version with different content fails before commit. Source rows remain append-only by `(source_key, content_hash)`.

Because the transcripts contain ASR errors and mixed traditional/simplified characters, `source_refs[].excerpt` is a normalized evidence synopsis, not a claimed verbatim quotation. Traceability comes from the stable `source_key`, exact relative source path, and stored file SHA-256; the UI labels the field “依据摘要”.

Add these settings:

```python
TRADING_PLAYBOOK_ENABLED: bool = True
TRADING_PLAYBOOK_TRANSCRIPT_ROOT: Optional[str] = None
TRADING_PLAYBOOK_MONITOR_INTERVAL_SECONDS: int = 3
TRADING_PLAYBOOK_TRIAL_POSITION_PCT: float = 10.0
TRADING_PLAYBOOK_CONFIRMED_POSITION_PCT: float = 30.0
TRADING_PLAYBOOK_HARD_STOP_PCT: float = 5.0
TRADING_PLAYBOOK_MAX_ACTION_CANDIDATES: int = 3
```

The import script must use this complete entry point:

```python
import argparse
import asyncio
from pathlib import Path

from app.database import async_session_maker, init_db
from app.services.trading_playbook.rule_catalog import RuleCatalog


async def run(source_root: Path) -> None:
    await init_db()
    catalog = RuleCatalog(Path(__file__).resolve().parents[1] / "data" / "trading_playbook_rules_v1.json")
    async with async_session_maker() as db:
        result = await catalog.seed(db, source_root)
    print(f"sources={result['sources']} rules={result['rules']}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", required=True, type=Path)
    args = parser.parse_args()
    asyncio.run(run(args.source_root))


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the catalog tests and import dry verification**

Working directory: `backend`

Run: `python -m unittest tests.test_trading_playbook_rule_catalog -v`

Expected: 3 tests PASS.

Run:

```powershell
python -m app.scripts.import_trading_playbook_rules --source-root 'C:\Users\Administrator\Documents\Codex\2026-07-07\ysheba257-lgtm-xiaoe-scraper-https-github\xiaoe-scraper\videos'
```

Expected: output reports `sources=8 rules=19`.

- [ ] **Step 5: Commit the catalog and importer**

```bash
git add backend/app/data/trading_playbook_rules_v1.json backend/app/services/trading_playbook backend/app/scripts/import_trading_playbook_rules.py backend/app/config.py backend/tests/test_trading_playbook_rule_catalog.py
git commit -m "feat: add transcript trading rule catalog"
```

## Task 3: Define normalized domain snapshots and safe quote collection

**Files:**
- Create: `backend/app/services/trading_playbook/domain.py`
- Create: `backend/app/services/trading_playbook/market_data.py`
- Modify: `backend/app/data_collectors/tencent_api.py`
- Test: `backend/tests/test_trading_playbook_market_data.py`

- [ ] **Step 1: Write failing quote and quality tests**

```python
import unittest
from datetime import date, datetime
from unittest.mock import AsyncMock

from app.data_collectors.tencent_api import TencentStockAPI
from app.services.trading_playbook.market_data import TradingPlaybookMarketDataProvider


class TradingPlaybookMarketDataTests(unittest.IsolatedAsyncioTestCase):
    def test_beijing_codes_use_bj_prefix(self):
        api = TencentStockAPI()
        self.assertEqual(api._format_code("920001"), "bj920001")
        self.assertEqual(api._format_code("430001"), "bj430001")
        self.assertEqual(api._format_code("830001"), "bj830001")

    async def test_snapshot_calculates_speed_from_previous_price(self):
        quote_api = AsyncMock()
        quote_api.get_quotes_batch.return_value = {
            "000001": {"code": "000001", "name": "样本", "price": 10.2, "pre_close": 10.0, "amount": 2000, "datetime": "20260710144000"}
        }
        provider = TradingPlaybookMarketDataProvider(quote_api=quote_api, batch_size=60, max_concurrency=1)
        provider._previous_prices = {"000001": 10.0}
        snapshot = await provider.quote_snapshot(["000001"], date(2026, 7, 10), datetime(2026, 7, 10, 14, 40))
        self.assertEqual(snapshot.quotes["000001"].speed_pct, 2.0)
        self.assertEqual(snapshot.quality.status, "ready")

    async def test_kline_features_detect_new_high_and_consolidation(self):
        async def load_kline(stock_code, market, period, limit, **kwargs):
            closes = [10.0, 12.0, 11.8, 11.9, 12.0, 12.5]
            return [{"close": value, "high": value, "low": value - 0.2, "amount": 1000} for value in closes]
        provider = TradingPlaybookMarketDataProvider(quote_api=AsyncMock(), kline_loader=load_kline)
        features = await provider.kline_features("000001", "SZ", "样本")
        self.assertTrue(features["n_day_high"])
        self.assertEqual(features["consolidation_days"], 4)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the market data tests and verify they fail**

Working directory: `backend`

Run: `python -m unittest tests.test_trading_playbook_market_data -v`

Expected: FAIL because Beijing formatting and the provider are absent.

- [ ] **Step 3: Implement domain dataclasses and the provider**

Define these immutable dataclasses in `domain.py`:

```python
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class DataQuality:
    status: str
    as_of: datetime
    source: str
    stale: bool = False
    warnings: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class QuotePoint:
    stock_code: str
    stock_name: str
    price: float
    pre_close: float
    open_price: float
    change_pct: float
    speed_pct: float
    amount: float
    turnover_rate: float
    bid1_price: float
    bid1_volume: float
    limit_up: float
    captured_at: datetime


@dataclass(frozen=True)
class QuoteSnapshot:
    trade_date: date
    quotes: Dict[str, QuotePoint]
    quality: DataQuality


@dataclass
class CandidateSnapshot:
    stock_code: str
    stock_name: str
    theme_name: str
    features: Dict[str, Any]
    evidence: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class MarketSnapshot:
    source_trade_date: date
    target_trade_date: date
    stage: str
    as_of: datetime
    market_features: Dict[str, Any]
    candidates: List[CandidateSnapshot]
    theme_rankings: List[Dict[str, Any]]
    quality: DataQuality


@dataclass(frozen=True)
class ModeEvaluation:
    mode_key: str
    stock_code: str
    status: str
    score: float
    role: str
    risk_level: str
    entry_trigger: Dict[str, Any]
    invalidation: Dict[str, Any]
    exit_trigger: Dict[str, Any]
    evidence: List[Dict[str, Any]]
    rule_version: int = 1
    rule_hash: str = ""
    action_scope: str = "target"
```

Update `_format_code` so codes starting with `4`, `8`, or `92` use `bj`; split `get_quotes_batch` into chunks of at most 80 symbols and merge the results.

Implement `TradingPlaybookMarketDataProvider.quote_snapshot` with bounded concurrency, previous-price speed calculation, missing-code warnings, and `DataQuality(status="degraded")` when fewer than 90% of requested quotes arrive.

The provider constructor accepts `kline_loader`; the production default is the existing `app.api.v1.market._fetch_kline_from_em` callable injected by the orchestration factory, so K-line HTTP parsing is not duplicated. Implement:

```python
async def kline_features(self, stock_code, market, stock_name):
    points = await self.kline_loader(stock_code, market, "day", 60, stock_name=stock_name)
    closes = [float(point["close"]) for point in points if point.get("close")]
    if len(closes) < 6:
        return {"n_day_high": False, "consolidation_days": 0, "trend_established": False, "kline_quality": "missing"}
    prior_high = max(closes[:-1])
    recent = closes[-5:-1]
    band = (max(recent) - min(recent)) / max(min(recent), 0.01)
    return {
        "n_day_high": closes[-1] > prior_high,
        "consolidation_days": 4 if band <= 0.08 else 0,
        "trend_established": closes[-1] > sum(closes[-6:-1]) / 5,
        "kline_quality": "ready",
    }
```

Implement `build_market_snapshot` with this bounded universe algorithm:

1. Query all non-ST `Stock.stock_code` values.
2. Fetch quotes in chunks and calculate full-market change/speed ranks.
3. Union the top 200 change/speed codes, current realtime limit-up pool, previous ten trading days of `MarketReviewStockDaily`, and current plan candidates.
4. Fetch 60-day K-lines only for that union and derive N-day-high/consolidation features.
5. At `auction`, require quote timestamps between 09:15 and 09:26 and derive `auction_change_pct`, `auction_amount`, `bid1_volume`, and same-theme auction rank.
6. Mark only the affected features missing when K-line or auction data fails; never manufacture zero-valued evidence.

- [ ] **Step 4: Run the market data tests and existing realtime tests**

Working directory: `backend`

Run:

```bash
python -m unittest tests.test_trading_playbook_market_data tests.test_realtime_limit_up_service tests.test_websocket_manager -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit normalized market data**

```bash
git add backend/app/services/trading_playbook/domain.py backend/app/services/trading_playbook/market_data.py backend/app/data_collectors/tencent_api.py backend/tests/test_trading_playbook_market_data.py
git commit -m "feat: add trading playbook market snapshots"
```

## Task 4: Classify market style, windows, themes, and recognition

**Files:**
- Create: `backend/app/services/trading_playbook/market_state.py`
- Test: `backend/tests/test_trading_playbook_market_state.py`

- [ ] **Step 1: Write failing classifier and ranking tests**

```python
import unittest

from app.services.trading_playbook.market_state import MarketStateClassifier, RecognitionRanker, ThemeRanker


class TradingPlaybookMarketStateTests(unittest.TestCase):
    def test_board_flow_and_outbreak_require_expansion(self):
        result = MarketStateClassifier().classify({
            "limit_up_count": 82,
            "limit_up_count_prev": 42,
            "max_board_height": 6,
            "seal_rate": 79,
            "limit_down_count": 2,
            "trend_new_high_count": 8,
            "trend_new_high_count_prev": 7,
        })
        self.assertEqual(result["style"], "board_flow")
        self.assertEqual(result["window"], "outbreak")

    def test_theme_ranker_prefers_expanding_theme(self):
        rows = ThemeRanker().rank([
            {"theme_name": "甲", "limit_up_count": 6, "new_high_count": 4, "sealed_count": 5, "broken_count": 1, "middle_army_strength": 8},
            {"theme_name": "乙", "limit_up_count": 3, "new_high_count": 1, "sealed_count": 2, "broken_count": 2, "middle_army_strength": 2},
        ])
        self.assertEqual(rows[0]["theme_name"], "甲")
        self.assertEqual(rows[0]["rank"], 1)

    def test_recognition_is_relative_and_ranked(self):
        rows = RecognitionRanker().rank([
            {"stock_code": "000001", "first_limit_seconds": 34260, "board_height": 4, "seal_strength": 9, "resilience": 8, "influence": 7},
            {"stock_code": "000002", "first_limit_seconds": 36000, "board_height": 2, "seal_strength": 4, "resilience": 3, "influence": 2},
        ])
        self.assertEqual(rows[0]["stock_code"], "000001")
        self.assertEqual(rows[0]["recognition_rank"], 1)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the tests and verify they fail**

Working directory: `backend`

Run: `python -m unittest tests.test_trading_playbook_market_state -v`

Expected: FAIL because the classifiers do not exist.

- [ ] **Step 3: Implement deterministic scoring**

Implement `MarketStateClassifier.classify` with these ordered rules:

```python
if limit_down_count >= 10 or seal_rate < 50:
    style = "chaos_retreat"
elif max_board_height >= 4 and limit_up_count >= 50 and limit_up_growth > trend_growth:
    style = "board_flow"
elif trend_new_high_count >= 20 and trend_growth >= limit_up_growth:
    style = "trend_main_wave"
else:
    style = "dual_active"

if negative_feedback:
    window = "decline"
elif limit_up_growth >= 0.35 and seal_rate >= 65:
    window = "outbreak"
elif divergence_days >= 3 and sell_pressure_falling:
    window = "divergence_exhaustion"
elif prior_window in {"first_divergence", "divergence_exhaustion"} and breadth_recovered:
    window = "divergence_to_consensus"
elif prior_window == "divergence_to_consensus" and breadth_recovered:
    window = "stronger_confirmation"
elif prior_window == "stronger_confirmation" and sell_pressure_rising:
    window = "second_divergence"
else:
    window = "first_divergence"
```

Implement `ThemeRanker` score as:

```python
score = limit_up_count * 5 + new_high_count * 3 + sealed_count * 2 - broken_count * 3 + middle_army_strength
```

Implement `RecognitionRanker` by ranking each of fastest, highest, hardest, resilient, and influential, then sorting by the sum of inverse ranks. Return raw evidence and `recognition_rank`.

- [ ] **Step 4: Run the classifier tests**

Working directory: `backend`

Run: `python -m unittest tests.test_trading_playbook_market_state -v`

Expected: 3 tests PASS.

- [ ] **Step 5: Commit market state classification**

```bash
git add backend/app/services/trading_playbook/market_state.py backend/tests/test_trading_playbook_market_state.py
git commit -m "feat: classify trading playbook market state"
```

## Task 5: Derive mode features and match all 19 rules

**Files:**
- Create: `backend/app/services/trading_playbook/mode_features.py`
- Create: `backend/app/services/trading_playbook/mode_matcher.py`
- Test: `backend/tests/test_trading_playbook_mode_matcher.py`

- [ ] **Step 1: Write failing matcher tests for each mode family**

```python
import unittest

from app.services.trading_playbook.domain import CandidateSnapshot
from app.services.trading_playbook.mode_matcher import ModeMatcher


RULES = [
    {"mode_key": "new_theme_high_volatility", "automation_level": "assisted", "priority": 100, "role": "high_volatility", "requirements": [
        {"feature": "market.window", "op": "eq", "value": "outbreak"},
        {"feature": "candidate.high_volatility", "op": "eq", "value": True},
    ], "entry": {"label": "进入"}, "invalidation": {"label": "失效"}, "exit": {"label": "退出"}},
    {"mode_key": "leader_turn_two", "automation_level": "automatic", "priority": 100, "role": "leader", "requirements": [
        {"feature": "candidate.turn_confirmed", "op": "eq", "value": True},
    ], "entry": {"label": "进入"}, "invalidation": {"label": "失效"}, "exit": {"label": "退出"}},
    {"mode_key": "trend_turn_two", "automation_level": "automatic", "priority": 99, "role": "trend_turn_two", "requirements": [
        {"feature": "candidate.trend_turn_two", "op": "eq", "value": True},
    ], "entry": {"label": "进入"}, "invalidation": {"label": "失效"}, "exit": {"label": "退出"}},
    {"mode_key": "dead_pile_right_confirmation", "automation_level": "manual_only", "priority": 74, "role": "right_confirmation", "requirements": [
        {"feature": "candidate.theme_dead", "op": "eq", "value": True},
    ], "entry": {"label": "进入"}, "invalidation": {"label": "失效"}, "exit": {"label": "退出"}},
]


class TradingPlaybookModeMatcherTests(unittest.TestCase):
    def test_matches_outbreak_leader_and_trend_modes(self):
        candidate = CandidateSnapshot("000001", "样本", "机器人", {
            "high_volatility": True,
            "turn_confirmed": True,
            "trend_turn_two": True,
            "theme_dead": False,
        })
        rows = ModeMatcher(RULES).evaluate({"window": "outbreak"}, candidate)
        self.assertEqual({row.mode_key for row in rows if row.status == "matched"}, {
            "new_theme_high_volatility", "leader_turn_two", "trend_turn_two"
        })

    def test_manual_only_mode_never_becomes_actionable(self):
        candidate = CandidateSnapshot("000002", "反转", "旧题材", {"theme_dead": True})
        row = next(item for item in ModeMatcher(RULES).evaluate({"window": "decline"}, candidate) if item.mode_key == "dead_pile_right_confirmation")
        self.assertEqual(row.status, "manual_review")
        self.assertEqual(row.risk_level, "watch")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the matcher tests and verify they fail**

Working directory: `backend`

Run: `python -m unittest tests.test_trading_playbook_mode_matcher -v`

Expected: FAIL because `ModeMatcher` does not exist.

- [ ] **Step 3: Implement feature extraction and the condition evaluator**

`ModeFeatureBuilder.build` must produce these candidate feature keys from review rows, quote/K-line facts, theme ranks, recognition ranks, and prior plan state:

```python
FEATURE_KEYS = {
    "high_volatility", "high_position", "same_level_turnover", "middle_army",
    "started_before_theme", "unique_survivor", "turn_confirmed", "stronger_confirmed",
    "confirmed_leader", "acceleration_to_divergence", "low_position_new_start", "supplement",
    "first_bearish", "trend_established", "pullback", "consolidation_rebreak",
    "linkage_confirmed", "trend_turn_two", "middle_army_linkage", "divergence_days",
    "resilience_rank", "theme_alive", "theme_dead", "snake_setup", "right_reversal",
    "external_switch", "theme_rank", "recognition_rank", "tail_action_eligible",
    "reference_price", "planned_pullback_price", "planned_breakout_price",
    "exit_change_pct_floor"
}
```

Use explicit definitions:

- `high_volatility`: code starts with `300`, `301`, `688`, `8`, `4`, or `92` and the candidate is in the top 20 speed/change ranks.
- `high_position`: board height is the theme maximum and at least 2.
- `middle_army`: theme amount rank is 1 and tradable market value is at least the theme median.
- `unique_survivor`: recognition rank is 1 and all other former high-position candidates are broken, opened, or trend-broken.
- `turn_confirmed`: prior state was survivor and current state is resealed, reversal limit-up, or right-side breakout with influence.
- `stronger_confirmed`: prior state was turn-confirmed and current open/price strength is positive without a new recognition leader.
- `trend_turn_two`: established trend, 3–10 session consolidation, new-high breakout, and linkage confirmed.
- `theme_alive`: the theme still has at least one sealed supplement or middle-army positive trend.
- `theme_dead`: no sealed/new-high peer and theme breadth is negative for at least two sessions.
- `tail_action_eligible`: stage is `preclose`, quotes are fresh, the entry condition is already satisfied, the invalidation condition is not satisfied, and the candidate is not `manual_only`; this is the only way an action can target `source_trade_date`.
- `reference_price`: latest point-in-time quote; `planned_pullback_price`: nearest validated support or 5-day low; `planned_breakout_price`: prior N-day high plus one market tick; `exit_change_pct_floor`: mode-specific exit floor, defaulting to -5 only when the rule has no stricter value.

Implement `ModeMatcher` operators `eq`, `in`, `lte`, `gte`, and comma-separated rule windows. Missing features produce `waiting`, failed features produce `not_matched`, satisfied `manual_only` rules produce `manual_review`, and satisfied other rules produce `matched`. Map risk levels to `trial` for assisted and `confirmed` for automatic.

Normalize every rule in `ModeMatcher.__init__`: set `version` from the catalog version, compute a deterministic SHA-256 `content_hash` when absent, and retain the complete normalized rule list. Populate `ModeEvaluation.rule_version` and `rule_hash`, set `action_scope="tail"` only when `tail_action_eligible` is true, and expose `rule_snapshot()` as a sorted list of `{mode_key, version, content_hash}` for all 19 rules.

Materialize live conditions instead of persisting labels alone:

```python
def _entry_trigger(self, rule, candidate):
    role = rule["role"]
    label = rule["entry"]["label"]
    reference_price = candidate.features["reference_price"]
    if role in {"survivor", "trend_core", "resilient_core", "snake_arbitrage"}:
        return {"label": label, "price_lte": candidate.features["planned_pullback_price"], "reference_price": reference_price}
    if role in {"leader", "confirmed_leader", "first_mover", "high_position", "same_level_turnover"}:
        return {"label": label, "sealed": True, "reference_price": reference_price}
    return {"label": label, "price_gte": candidate.features["planned_breakout_price"], "reference_price": reference_price}

def _invalidation(self, rule, candidate):
    return {"label": rule["invalidation"]["label"]}

def _exit_trigger(self, rule, candidate):
    return {
        "label": rule["exit"]["label"],
        "change_pct_lte": candidate.features.get("exit_change_pct_floor", -5.0),
    }
```

`ModeFeatureBuilder` always computes `planned_pullback_price`, `planned_breakout_price`, and `hard_stop_price` from the current/reference price and configured 5% hard stop. `ModeEvaluation` stores these materialized dictionaries so `PlanAlertMonitor` can evaluate them directly.

- [ ] **Step 4: Run matcher and catalog coverage tests**

Working directory: `backend`

Run:

```bash
python -m unittest tests.test_trading_playbook_mode_matcher tests.test_trading_playbook_rule_catalog -v
```

Expected: all tests PASS and all 19 catalog rules can be loaded by `ModeMatcher`.

- [ ] **Step 5: Commit the complete mode engine**

```bash
git add backend/app/services/trading_playbook/mode_features.py backend/app/services/trading_playbook/mode_matcher.py backend/tests/test_trading_playbook_mode_matcher.py
git commit -m "feat: match transcript trading modes"
```

## Task 6: Generate immutable three-candidate plan versions

**Files:**
- Create: `backend/app/services/trading_playbook/plan_service.py`
- Test: `backend/tests/test_trading_playbook_plan_service.py`

- [ ] **Step 1: Write failing plan version tests**

```python
import asyncio
import unittest
from datetime import date, datetime

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models.trading_playbook import TradingPlanCandidate, TradingPlanVersion
from app.services.trading_playbook.domain import DataQuality, MarketSnapshot, ModeEvaluation
from app.services.trading_playbook.plan_service import TradingPlanService


class TradingPlaybookPlanServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
        self.Session = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def test_generate_limits_action_candidates_to_three_and_preserves_radar(self):
        snapshot = MarketSnapshot(
            date(2026, 7, 10), date(2026, 7, 13), "preclose", datetime(2026, 7, 10, 14, 40),
            {"style": "board_flow", "window": "outbreak"}, [], [],
            DataQuality("ready", datetime(2026, 7, 10, 14, 40), "test"),
        )
        evaluations = [
            ModeEvaluation("leader_turn_two", f"00000{i}", "matched", 100 - i, "leader", "confirmed", {"label":"进"}, {"label":"退"}, {"label":"出"}, [])
            for i in range(1, 6)
        ]
        async with self.Session() as db:
            plan = await TradingPlanService().generate(
                db,
                snapshot,
                evaluations,
                stock_names={f"00000{i}": f"股票{i}" for i in range(1, 6)},
                rule_snapshot=[{"mode_key":"leader_turn_two", "version":1, "content_hash":"rule-a"}],
            )
            self.assertEqual(len(plan["candidates"]), 3)
            self.assertEqual(len(plan["mode_radar"]), 5)
            self.assertEqual(plan["rule_snapshot"][0]["content_hash"], "rule-a")
            self.assertEqual(plan["risk_settings"]["max_candidates"], 3)

    async def test_manual_revision_creates_child_version(self):
        async with self.Session() as db:
            parent = TradingPlanVersion(source_trade_date=date(2026,7,10), target_trade_date=date(2026,7,13), stage="after_close", version_no=1, status="draft", input_hash="a")
            db.add(parent)
            await db.commit()
            await db.refresh(parent)
            child = await TradingPlanService().revise(db, parent.id, {"change_note": "人工调整"})
            self.assertEqual(child.parent_plan_version_id, parent.id)
            self.assertNotEqual(child.id, parent.id)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the plan tests and verify they fail**

Working directory: `backend`

Run: `python -m unittest tests.test_trading_playbook_plan_service -v`

Expected: FAIL because `TradingPlanService` does not exist.

- [ ] **Step 3: Implement version generation, revision, and confirmation**

Implement:

```python
class TradingPlanService:
    async def generate(self, db, snapshot, evaluations, stock_names, rule_snapshot=None):
        radar = [self._serialize_evaluation(row) for row in evaluations]
        eligible = [row for row in evaluations if row.status == "matched"]
        eligible.sort(key=lambda row: (-row.score, row.stock_code, row.mode_key))
        settings = await self._get_or_create_settings(db)
        risk_settings = {
            "trial": settings.trial_position_pct,
            "confirmed": settings.confirmed_position_pct,
            "hard_stop": settings.hard_stop_pct,
            "max_candidates": min(settings.max_action_candidates, 3),
            "source_refs": [
                {"source_key": "03-loss-qa", "excerpt": "候选不超过三只，开仓和退出条件必须预先写清，并执行刚性止损"},
                {"source_key": "04-trading-plan", "excerpt": "交易前形成书面计划，盘后区分信号、执行与结果"},
            ],
        }
        selected = [row for row in eligible if not (snapshot.quality.status == "degraded" and row.risk_level == "confirmed")][:risk_settings["max_candidates"]]
        if rule_snapshot is None:
            rule_snapshot = sorted(
                {row.mode_key: {"mode_key": row.mode_key, "version": row.rule_version, "content_hash": row.rule_hash} for row in evaluations}.values(),
                key=lambda item: item["mode_key"],
            )
        version_no = await self._next_version_no(db, snapshot.target_trade_date, snapshot.stage)
        input_hash = self._input_hash(snapshot, radar, rule_snapshot, risk_settings)
        existing = await self._find_same_input(db, snapshot.target_trade_date, snapshot.stage, input_hash)
        if existing is not None:
            return await self.serialize(db, existing)
        plan = TradingPlanVersion(
            source_trade_date=snapshot.source_trade_date,
            target_trade_date=snapshot.target_trade_date,
            stage=snapshot.stage,
            version_no=version_no,
            status="draft",
            market_state_json=snapshot.market_features,
            theme_ranking_json=snapshot.theme_rankings,
            mode_radar_json=radar,
            rule_snapshot_json=rule_snapshot,
            risk_settings_json=risk_settings,
            data_quality_json=self._quality_dict(snapshot.quality),
            change_summary_json=await self._change_summary(db, snapshot.target_trade_date, radar),
            input_hash=input_hash,
        )
        db.add(plan)
        await db.flush()
        for rank, row in enumerate(selected, start=1):
            db.add(self._candidate_from_evaluation(plan.id, snapshot, row, stock_names, rank, risk_settings))
        await db.commit()
        await db.refresh(plan)
        return await self.serialize(db, plan)

    async def revise(self, db, plan_id, changes):
        parent = await db.get(TradingPlanVersion, plan_id)
        if parent is None:
            raise ValueError("plan not found")
        child = self._clone_plan(parent)
        child.parent_plan_version_id = parent.id
        child.version_no = await self._next_version_no(db, parent.target_trade_date, parent.stage)
        child.status = "draft"
        child.change_summary_json = {"manual": True, **changes}
        child.confirmed_at = None
        child.confirmed_by = None
        db.add(child)
        await db.flush()
        await self._clone_candidates(db, parent.id, child.id, changes)
        await db.commit()
        await db.refresh(child)
        return child

    async def confirm(self, db, plan_id, confirmed_by):
        plan = await db.get(TradingPlanVersion, plan_id)
        if plan is None or plan.status not in {"draft", "confirmed"}:
            raise ValueError("plan cannot be confirmed")
        await self._supersede_active_plans(db, plan.target_trade_date, plan.id)
        plan.status = "active"
        plan.confirmed_at = datetime.now()
        plan.confirmed_by = confirmed_by
        await db.commit()
        await db.refresh(plan)
        return plan
```

`_get_or_create_settings` reads singleton `TradingPlaybookSettings(id=1)` and creates it from `settings.TRADING_PLAYBOOK_*` defaults when absent. `_input_hash` includes the normalized snapshot, radar, complete rule snapshot, and risk settings, so a rule or risk change creates a new immutable version.

`_candidate_from_evaluation` must use `source_trade_date` only when `snapshot.stage == "preclose"` and `row.action_scope == "tail"`; use `target_trade_date` otherwise. It copies the entry trigger, takes its `reference_price`, and materializes `invalidation_json.price_lte = round(reference_price * (1 - risk_settings["hard_stop"] / 100), 2)` while retaining the rule's semantic invalidation label. It maps risk level `trial`/`confirmed` to the matching position percentage in `risk_settings`. Explicit manual revision data may change `action_trade_date`. Degraded snapshots may create radar entries but cannot create `confirmed` risk candidates.

- [ ] **Step 4: Run the plan service tests**

Working directory: `backend`

Run: `python -m unittest tests.test_trading_playbook_plan_service -v`

Expected: 2 tests PASS.

- [ ] **Step 5: Commit immutable plan generation**

```bash
git add backend/app/services/trading_playbook/plan_service.py backend/tests/test_trading_playbook_plan_service.py
git commit -m "feat: generate versioned trading plans"
```

## Task 7: Orchestrate one complete plan stage

**Files:**
- Create: `backend/app/services/trading_playbook/orchestrator.py`
- Test: `backend/tests/test_trading_playbook_orchestrator.py`

- [ ] **Step 1: Write the failing orchestration test**

```python
import unittest
from datetime import date, datetime
from unittest.mock import AsyncMock, MagicMock

from app.services.trading_playbook.orchestrator import TradingPlaybookOrchestrator


class TradingPlaybookOrchestratorTests(unittest.IsolatedAsyncioTestCase):
    async def test_preclose_targets_next_trade_day_and_runs_all_layers(self):
        market_data = AsyncMock()
        market_data.build_market_snapshot.return_value = MagicMock(
            source_trade_date=date(2026, 7, 10),
            target_trade_date=date(2026, 7, 13),
            stage="preclose",
            market_features={},
            candidates=[],
        )
        classifier = MagicMock()
        classifier.classify.return_value = {"style": "board_flow", "window": "outbreak"}
        feature_builder = MagicMock()
        matcher = MagicMock()
        matcher.evaluate.return_value = []
        matcher.rule_snapshot.return_value = [{"mode_key":"leader_turn_two", "version":1, "content_hash":"rule-a"}]
        plan_service = AsyncMock()
        plan_service.generate.return_value = {"stage": "preclose", "candidates": []}
        orchestrator = TradingPlaybookOrchestrator(
            market_data=market_data,
            classifier=classifier,
            feature_builder=feature_builder,
            matcher=matcher,
            plan_service=plan_service,
            next_trade_date=lambda value: date(2026, 7, 13),
        )

        result = await orchestrator.build_stage(AsyncMock(), date(2026, 7, 10), "preclose", datetime(2026, 7, 10, 14, 40))

        self.assertEqual(result["stage"], "preclose")
        market_data.build_market_snapshot.assert_awaited_once()
        classifier.classify.assert_called_once()
        plan_service.generate.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the orchestration test and verify it fails**

Working directory: `backend`

Run: `python -m unittest tests.test_trading_playbook_orchestrator -v`

Expected: FAIL because `TradingPlaybookOrchestrator` does not exist.

- [ ] **Step 3: Implement the stage pipeline**

```python
class TradingPlaybookOrchestrator:
    VALID_STAGES = {"preclose", "after_close", "overnight", "auction"}

    def __init__(self, market_data, classifier, feature_builder, matcher, plan_service, next_trade_date):
        self.market_data = market_data
        self.classifier = classifier
        self.feature_builder = feature_builder
        self.matcher = matcher
        self.plan_service = plan_service
        self.next_trade_date = next_trade_date

    async def build_stage(self, db, source_trade_date, stage, as_of, degraded=False):
        if stage not in self.VALID_STAGES:
            raise ValueError(f"unsupported stage: {stage}")
        target_trade_date = self.next_trade_date(source_trade_date) if stage in {"preclose", "after_close"} else source_trade_date
        snapshot = await self.market_data.build_market_snapshot(
            db=db,
            source_trade_date=source_trade_date,
            target_trade_date=target_trade_date,
            stage=stage,
            as_of=as_of,
            force_degraded=degraded,
        )
        market_state = self.classifier.classify(snapshot.market_features)
        snapshot.market_features.update(market_state)
        evaluations = []
        stock_names = {}
        for candidate in snapshot.candidates:
            candidate.features.update(self.feature_builder.build(snapshot, candidate))
            stock_names[candidate.stock_code] = candidate.stock_name
            evaluations.extend(self.matcher.evaluate(snapshot.market_features, candidate))
        return await self.plan_service.generate(
            db,
            snapshot,
            evaluations,
            stock_names,
            rule_snapshot=self.matcher.rule_snapshot(),
        )
```

For `overnight` and `auction`, the API/scheduler passes the target trading date as `source_trade_date`; `build_market_snapshot` obtains the previous trading day internally for historical context. The orchestrator never sends notifications; only confirmation and `PlanAlertMonitor` can do that.

Add a single composition root so API and scheduler do not construct different pipelines:

```python
def build_default_orchestrator(*, tencent_api, next_trade_date):
    catalog = RuleCatalog(Path(__file__).parents[2] / "data" / "trading_playbook_rules_v1.json").load()
    market_data = TradingPlaybookMarketDataProvider(
        quote_client=tencent_api,
        kline_loader=_fetch_kline_from_em,
    )
    return TradingPlaybookOrchestrator(
        market_data=market_data,
        classifier=MarketStateClassifier(),
        feature_builder=ModeFeatureBuilder(),
        matcher=ModeMatcher(catalog["rules"], catalog_version=catalog["catalog_version"]),
        plan_service=TradingPlanService(),
        next_trade_date=next_trade_date,
    )
```

Both callers inject one resolver: query `_get_cn_trading_dates(value + timedelta(days=1), value + timedelta(days=15))`, return the first date, and raise `TradingCalendarLookupError` when empty. This keeps the calendar outcome identical for manual API generation and scheduled generation.

- [ ] **Step 4: Run the orchestration and plan tests**

Working directory: `backend`

Run:

```bash
python -m unittest tests.test_trading_playbook_orchestrator tests.test_trading_playbook_plan_service -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit orchestration**

```bash
git add backend/app/services/trading_playbook/orchestrator.py backend/tests/test_trading_playbook_orchestrator.py
git commit -m "feat: orchestrate trading plan stages"
```

## Task 8: Add schemas, REST API, and settings endpoints

**Files:**
- Create: `backend/app/schemas/trading_playbook.py`
- Create: `backend/app/api/v1/trading_playbook.py`
- Modify: `backend/app/api/v1/__init__.py`
- Test: `backend/tests/test_trading_playbook_api.py`

- [ ] **Step 1: Write failing API tests**

```python
import asyncio
import unittest
from datetime import date

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.api.v1.trading_playbook import router
from app.database import Base, get_db
from app.models.trading_playbook import TradingPlanVersion, TradingPlaybookSettings


class TradingPlaybookApiTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
        self.Session = async_sessionmaker(self.engine, expire_on_commit=False)
        asyncio.run(self._seed())
        app = FastAPI()
        app.include_router(router, prefix="/trading-playbook")
        async def override_db():
            async with self.Session() as db:
                yield db
        app.dependency_overrides[get_db] = override_db
        self.client = TestClient(app)

    async def _seed(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        async with self.Session() as db:
            db.add(TradingPlanVersion(source_trade_date=date(2026,7,10), target_trade_date=date(2026,7,13), stage="after_close", version_no=1, status="draft", input_hash="seed"))
            await db.commit()

    def tearDown(self):
        self.client.close()
        asyncio.run(self.engine.dispose())

    def test_list_plans_and_confirm(self):
        payload = self.client.get("/trading-playbook/plans", params={"trade_date":"2026-07-13"}).json()
        self.assertEqual(len(payload["items"]), 1)
        plan_id = payload["items"][0]["id"]
        confirmed = self.client.post(f"/trading-playbook/plans/{plan_id}/confirm", json={"confirmed_by":"local-user"}).json()
        self.assertEqual(confirmed["status"], "active")

    def test_wechat_cannot_be_enabled_in_first_release(self):
        response = self.client.put("/trading-playbook/settings", json={"wechat_enabled": True})
        self.assertEqual(response.status_code, 422)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the API tests and verify they fail**

Working directory: `backend`

Run: `python -m unittest tests.test_trading_playbook_api -v`

Expected: FAIL because the router and schemas do not exist.

- [ ] **Step 3: Implement the router and strict schemas**

Define literal enums for stages, statuses, event types, and risk levels. Define request schemas:

```python
from datetime import date
from typing import Literal

from pydantic import BaseModel, Field

PlanStage = Literal["preclose", "after_close", "overnight", "auction"]
PlanStatus = Literal["draft", "confirmed", "active", "superseded", "expired"]
RiskLevel = Literal["watch", "trial", "confirmed"]


class PlanGenerateRequest(BaseModel):
    source_trade_date: date
    stage: PlanStage


class PlanConfirmRequest(BaseModel):
    confirmed_by: str = Field(min_length=1, max_length=80)


class PlanRevisionRequest(BaseModel):
    change_note: str = Field(min_length=1, max_length=500)
    candidate_overrides: list[dict] = Field(default_factory=list)


class ManualExecutionUpdate(BaseModel):
    executions: dict[str, dict] = Field(default_factory=dict)


class TradingPlaybookSettingsUpdate(BaseModel):
    trial_position_pct: float | None = Field(default=None, ge=0, le=100)
    confirmed_position_pct: float | None = Field(default=None, ge=0, le=100)
    hard_stop_pct: float | None = Field(default=None, gt=0, le=20)
    max_action_candidates: int | None = Field(default=None, ge=1, le=3)
    in_app_enabled: bool | None = None
    wechat_enabled: Literal[False] | None = None
```

Implement these exact router operations and service calls:

| Method | Path | Exact behavior |
| --- | --- | --- |
| GET | `/rules` | Select enabled `TradingModeRule` rows ordered by family, priority from `prerequisites_json`, and mode key; return serialized source references. |
| GET | `/plans` | Select `TradingPlanVersion.target_trade_date == trade_date`, order by `generated_at DESC`, and serialize candidates for every version. |
| GET | `/plans/{plan_id}` | Call `TradingPlanService.serialize`; return 404 when absent. |
| POST | `/plans/generate` | Call `TradingPlaybookOrchestrator.build_stage(db, source_trade_date, stage, now_cn())`; unchanged input returns the existing version. |
| PUT | `/plans/{plan_id}` | Call `TradingPlanService.revise`; never mutate the parent; return 409 for invalid state. |
| POST | `/plans/{plan_id}/confirm` | Call `TradingPlanService.confirm`; return 409 for invalid state. |
| POST | `/plans/{plan_id}/cancel` | Set only a draft or active plan to `expired`; return 409 for other states. |
| GET | `/alerts` | Select `TradingAlertEvent`, filtering `acknowledged_at IS NULL` when `unread_only=true`. |
| POST | `/alerts/{alert_id}/ack` | Set `acknowledged_at=now_cn()`; return 404 when absent. |
| PUT | `/reviews/{trade_date}` | Call `TradingPlaybookReviewService.update_manual_execution`. |
| GET | `/settings` | Return row id 1, creating it with configured defaults when absent. |
| PUT | `/settings` | Update row id 1 after schema validation; `wechat_enabled=true` is rejected by the schema. |

Translate missing records to HTTP 404 and invalid plan transitions to HTTP 409. Mount the router in `backend/app/api/v1/__init__.py` with prefix `/trading-playbook`.

- [ ] **Step 4: Run API and existing intelligence tests**

Working directory: `backend`

Run:

```bash
python -m unittest tests.test_trading_playbook_api tests.test_intelligence_api -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit the API**

```bash
git add backend/app/schemas/trading_playbook.py backend/app/api/v1/trading_playbook.py backend/app/api/v1/__init__.py backend/tests/test_trading_playbook_api.py
git commit -m "feat: expose trading playbook API"
```

## Task 9: Register five timed stages, the active monitor, and the data-ready barrier

**Files:**
- Modify: `backend/app/data_collectors/scheduler.py`
- Modify: `backend/app/main.py`
- Test: `backend/tests/test_trading_playbook_scheduler.py`
- Test: `backend/tests/test_main_lifespan.py`

- [ ] **Step 1: Write failing scheduler registration and barrier tests**

```python
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from app.data_collectors.scheduler import DataScheduler


class TradingPlaybookSchedulerTests(unittest.IsolatedAsyncioTestCase):
    def test_start_registers_all_playbook_jobs(self):
        scheduler = DataScheduler()
        scheduler.scheduler = MagicMock()
        with patch("app.data_collectors.scheduler.settings.TRADING_PLAYBOOK_ENABLED", True):
            scheduler.start()
        ids = {call.kwargs["id"] for call in scheduler.scheduler.add_job.call_args_list}
        self.assertTrue({
            "trading_playbook_preclose", "trading_playbook_review", "trading_playbook_after_close",
            "trading_playbook_overnight", "trading_playbook_auction", "trading_playbook_monitor"
        }.issubset(ids))

    async def test_after_close_waits_for_data_ready(self):
        scheduler = DataScheduler()
        scheduler._wait_for_trading_playbook_data = AsyncMock(return_value=False)
        scheduler._build_trading_playbook_plan = AsyncMock()
        scheduler._finalize_trading_playbook_review = AsyncMock()
        await scheduler._build_trading_playbook_after_close()
        scheduler._build_trading_playbook_plan.assert_awaited_once_with("after_close", degraded=True)
        scheduler._finalize_trading_playbook_review.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the scheduler tests and verify they fail**

Working directory: `backend`

Run: `python -m unittest tests.test_trading_playbook_scheduler -v`

Expected: FAIL because the jobs and methods are absent.

- [ ] **Step 3: Register exact Shanghai-time jobs**

Add these registrations when `TRADING_PLAYBOOK_ENABLED` is true:

```python
self.scheduler.add_job(self._build_trading_playbook_preclose, CronTrigger(hour=14, minute=40, timezone=CN_TZ), id="trading_playbook_preclose", max_instances=1)
self.scheduler.add_job(self._review_trading_playbook, CronTrigger(hour=15, minute=10, timezone=CN_TZ), id="trading_playbook_review", max_instances=1)
self.scheduler.add_job(self._build_trading_playbook_after_close, CronTrigger(hour=15, minute=30, timezone=CN_TZ), id="trading_playbook_after_close", max_instances=1)
self.scheduler.add_job(self._build_trading_playbook_overnight, CronTrigger(hour=8, minute=50, timezone=CN_TZ), id="trading_playbook_overnight", max_instances=1)
self.scheduler.add_job(self._build_trading_playbook_auction, CronTrigger(hour=9, minute=26, timezone=CN_TZ), id="trading_playbook_auction", max_instances=1)
self.scheduler.add_job(self._monitor_trading_playbook, IntervalTrigger(seconds=settings.TRADING_PLAYBOOK_MONITOR_INTERVAL_SECONDS), id="trading_playbook_monitor", max_instances=1)
```

Every method first checks `_get_cn_trading_dates`. `_wait_for_trading_playbook_data` polls for a `MarketReviewDailyMetric.updated_at` newer than 15:00 for at most 180 seconds using non-blocking waits no longer than 10 seconds. A timeout generates a degraded version; a later successful input creates a new immutable version.

At application startup, build exactly one production `TradingPlaybookOrchestrator`, pass that same instance to the scheduler, and register it with `trading_playbook_runtime.install_orchestrator`. Reset the registry during shutdown. Do not construct an API-only pipeline. Add a `backend/app/main.py` integration test against the real mounted app (no dependency override) proving `/api/v1/trading-playbook/plans/generate` resolves the registered instance; the unregistered case must remain a controlled HTTP 503.

`_build_trading_playbook_plan` calls `TradingPlaybookOrchestrator.build_stage`; when `send_notifications=True`, it then calls `TradingPlaybookAlertService.notify_plan_ready`, which emits isolated `plan_ready` and `confirmation_required` events without creating an action reminder. `_review_trading_playbook` calls `TradingPlaybookReviewService.build(db, today, finalized=False)` at 15:10. After `_build_trading_playbook_after_close` generates the 15:30 plan, it calls `_finalize_trading_playbook_review`, which invokes `TradingPlaybookReviewService.build(db, today, finalized=True)` so final facts reconcile into the same review row without overwriting manual execution.

Add startup catch-up with these exact branches, all guarded by the China trading calendar:

```python
if time(8, 50) <= now.time() < time(9, 26) and not await self._playbook_stage_exists(today, "overnight"):
    await self._build_trading_playbook_plan("overnight", send_notifications=False)
elif time(9, 26) <= now.time() <= time(15, 0) and not await self._playbook_stage_exists(today, "auction"):
    await self._build_trading_playbook_plan("auction", degraded=True, send_notifications=False)

if time(14, 40) <= now.time() < time(15, 0) and not await self._playbook_stage_exists(next_trade_date, "preclose"):
    await self._build_trading_playbook_plan("preclose", send_notifications=False)

if now.time() >= time(15, 10) and not await self._playbook_review_exists(today):
    await self._review_trading_playbook()
if now.time() >= time(15, 30) and not await self._playbook_stage_exists(next_trade_date, "after_close"):
    await self._build_trading_playbook_after_close(send_notifications=False)
```

Compute `next_trade_date` from `_get_cn_trading_dates(today + timedelta(days=1), today + timedelta(days=15))[0]`; raise `TradingCalendarLookupError` when the range is empty. Historical backfills always set `send_notifications=False`.

- [ ] **Step 4: Run scheduler and lifespan tests**

Working directory: `backend`

Run:

```bash
python -m unittest tests.test_trading_playbook_scheduler tests.test_market_review_scheduler tests.test_main_lifespan -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit scheduler integration**

```bash
git add backend/app/data_collectors/scheduler.py backend/tests/test_trading_playbook_scheduler.py
git commit -m "feat: schedule daily trading playbooks"
```

## Task 10: Add isolated in-app channel, monitoring, and WebSocket delivery

**Files:**
- Create: `backend/app/services/trading_playbook/channels.py`
- Create: `backend/app/services/trading_playbook/alert_service.py`
- Modify: `backend/app/core/websocket_manager.py`
- Test: `backend/tests/test_trading_playbook_alerts.py`
- Test: `backend/tests/test_websocket_manager.py`

- [ ] **Step 1: Write failing confirmation, deduplication, and broadcast tests**

```python
import unittest
from datetime import date
from unittest.mock import AsyncMock

from app.core.websocket_manager import ConnectionManager
from app.services.trading_playbook.alert_service import TradingPlaybookAlertService


class TradingPlaybookAlertTests(unittest.IsolatedAsyncioTestCase):
    async def test_unconfirmed_plan_only_emits_watch(self):
        service = TradingPlaybookAlertService(channel=AsyncMock())
        events = await service.evaluate_candidate(
            plan_status="draft",
            candidate={"id": 1, "entry_trigger_json": {"price_gte": 10}, "invalidation_json": {}},
            quote={"price": 10.5},
        )
        self.assertEqual([event["event_type"] for event in events], ["watch"])

    async def test_active_plan_emits_entry_trigger_once(self):
        service = TradingPlaybookAlertService(channel=AsyncMock())
        first = await service.evaluate_candidate("active", {"id": 2, "entry_trigger_json": {"price_gte": 10}, "invalidation_json": {}}, {"price": 10.5})
        second = await service.evaluate_candidate("active", {"id": 2, "entry_trigger_json": {"price_gte": 10}, "invalidation_json": {}}, {"price": 10.6})
        self.assertEqual(first[0]["event_type"], "entry_triggered")
        self.assertEqual(second, [])

    async def test_websocket_manager_subscribes_to_isolated_type(self):
        manager = ConnectionManager()
        await manager.connect(AsyncMock(), "client")
        self.assertIn("trading_plan_alert", manager.message_types["client"])


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run alert tests and verify they fail**

Working directory: `backend`

Run: `python -m unittest tests.test_trading_playbook_alerts -v`

Expected: FAIL because the channel, service, and message subscription are absent.

- [ ] **Step 3: Implement the notification interface and monitor**

Create:

```python
from abc import ABC, abstractmethod

from app.core.websocket_manager import manager


class NotificationChannel(ABC):
    @abstractmethod
    async def send(self, event: dict) -> dict:
        raise NotImplementedError

    @abstractmethod
    async def healthcheck(self) -> dict:
        raise NotImplementedError


class InAppTradingPlanChannel(NotificationChannel):
    async def send(self, event: dict) -> dict:
        await manager.broadcast(event, "trading_plan_alert", event.get("stock_code"))
        return {"channel": "in_app", "status": "sent"}

    async def healthcheck(self) -> dict:
        return {"channel": "in_app", "status": "ready", "connections": manager.connection_count}
```

Implement trigger evaluation with exact precedence:

```python
async def evaluate_candidate(self, plan_status, candidate, quote):
    if plan_status != "active":
        return self._dedupe_memory(candidate["id"], "watch", [{"event_type": "watch", "severity": "info"}])
    if self._condition_matches(candidate.get("invalidation_json", {}), quote):
        return self._dedupe_memory(candidate["id"], "invalidated", [{"event_type": "invalidated", "severity": "warning"}])
    if self._condition_matches(candidate.get("exit_trigger_json", {}), quote):
        return self._dedupe_memory(candidate["id"], "exit_triggered", [{"event_type": "exit_triggered", "severity": "warning"}])
    if self._condition_matches(candidate.get("entry_trigger_json", {}), quote):
        return self._dedupe_memory(candidate["id"], "entry_triggered", [{"event_type": "entry_triggered", "severity": "action"}])
    return []
```

`_condition_matches` supports `price_gte`, `price_lte`, `change_pct_gte`, `change_pct_lte`, `sealed`, and `open_count_gte`; it treats `label` and `reference_price` as metadata and ignores them during comparison. `monitor(db, now)` loads active plans with candidates whose `action_trade_date` is today, fetches quotes only for those candidates, evaluates conditions, inserts `TradingAlertEvent` before delivery, catches unique-key conflicts as deduplication, calls the channel, and persists delivery status. Draft plans may only create `watch`, `plan_ready`, or `confirmation_required` events.

Implement `notify_plan_ready(db, plan, send=True)` with database dedup keys `{plan.id}:plan_ready` and `{plan.id}:confirmation_required`. It persists both informational events and sends them through `InAppTradingPlanChannel` only when singleton settings have `in_app_enabled=True`; it never emits `entry_triggered`, `exit_triggered`, or `invalidated`. The active monitor uses the same settings check. `wechat_enabled` remains rejected by the API and no WeChat channel class is created in this release.

Add `trading_plan_alert` to default WebSocket subscriptions and add `broadcast_trading_plan_alert` as a thin wrapper around `broadcast`.

- [ ] **Step 4: Run alert and existing WebSocket tests**

Working directory: `backend`

Run:

```bash
python -m unittest tests.test_trading_playbook_alerts tests.test_websocket_manager -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit isolated reminders**

```bash
git add backend/app/services/trading_playbook/channels.py backend/app/services/trading_playbook/alert_service.py backend/app/core/websocket_manager.py backend/tests/test_trading_playbook_alerts.py backend/tests/test_websocket_manager.py
git commit -m "feat: add isolated trading plan alerts"
```

## Task 11: Add execution review and 15:30 reconciliation

**Files:**
- Create: `backend/app/services/trading_playbook/review_service.py`
- Modify: `backend/app/main.py`
- Test: `backend/tests/test_trading_playbook_review.py`
- Test: `backend/tests/test_main_lifespan.py`

- [ ] **Step 1: Write failing review classification tests**

```python
import unittest

from app.services.trading_playbook.review_service import TradingPlaybookReviewService


class TradingPlaybookReviewTests(unittest.TestCase):
    def test_review_separates_not_triggered_invalidated_and_not_executed(self):
        result = TradingPlaybookReviewService().summarize(
            candidates=[
                {"id": 1, "stock_code": "000001", "status": "waiting"},
                {"id": 2, "stock_code": "000002", "status": "invalidated"},
                {"id": 3, "stock_code": "000003", "status": "triggered"},
            ],
            events=[{"candidate_id": 3, "event_type": "entry_triggered"}],
            manual_execution={"3": {"executed": False}},
        )
        self.assertEqual(result["not_triggered"], ["000001"])
        self.assertEqual(result["invalidated"], ["000002"])
        self.assertEqual(result["triggered_not_executed"], ["000003"])

    def test_missing_execution_never_infers_account_profit(self):
        result = TradingPlaybookReviewService().summarize([], [], {})
        self.assertNotIn("account_profit", result)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run review tests and verify they fail**

Working directory: `backend`

Run: `python -m unittest tests.test_trading_playbook_review -v`

Expected: FAIL because `TradingPlaybookReviewService` does not exist.

- [ ] **Step 3: Implement preliminary review and final reconciliation**

Implement `summarize`, async `build(db, trade_date, finalized=False)`, and `update_manual_execution`. The summary keys must be:

```python
{
    "not_triggered": [],
    "invalidated": [],
    "triggered_executed": [],
    "triggered_not_executed": [],
    "plan_compliance": {"planned": 0, "executed": 0, "unplanned": 0},
    "signal_outcomes": [],
}
```

At 15:10, store closing snapshot values with `finalized_at=None`. At 15:30, update the same `(trade_date, plan_version_id)` review with final market-review facts and set `finalized_at`; do not overwrite `manual_execution_json`.

Create one shared `TradingPlaybookReviewService` during application startup, pass it to the scheduler, and register that same instance with `trading_playbook_runtime.install_review_service`; reset the registry during shutdown. Add a real `backend/app/main.py` integration test with no dependency override proving the mounted review endpoint resolves the registered service. Before registration it must return HTTP 503.

- [ ] **Step 4: Run review tests**

Working directory: `backend`

Run: `python -m unittest tests.test_trading_playbook_review -v`

Expected: 2 tests PASS.

- [ ] **Step 5: Commit execution review**

```bash
git add backend/app/services/trading_playbook/review_service.py backend/tests/test_trading_playbook_review.py
git commit -m "feat: review trading plan execution"
```

## Task 12: Add frontend types, API client, and isolated store

**Files:**
- Create: `frontend/src/types/trading-playbook.ts`
- Create: `frontend/src/api/trading-playbook.ts`
- Create: `frontend/src/stores/trading-playbook.ts`
- Modify: `frontend/src/composables/useWebSocket.ts`
- Test: `frontend/tests/tradingPlaybookApi.test.mjs`
- Test: `frontend/tests/tradingPlaybookStore.test.mjs`

- [ ] **Step 1: Write failing source-contract tests**

```javascript
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import test from 'node:test'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')
const read = path => readFileSync(resolve(root, path), 'utf8')

test('trading playbook API exposes plans confirmation alerts reviews and settings', () => {
  const api = read('src/api/trading-playbook.ts')
  for (const name of ['getTradingPlans', 'confirmTradingPlan', 'getTradingAlerts', 'ackTradingAlert', 'getTradingPlaybookSettings']) {
    assert.match(api, new RegExp(`function\\s+${name}|const\\s+${name}`))
  }
})

test('trading playbook store is isolated from the global alert store', () => {
  const store = read('src/stores/trading-playbook.ts')
  assert.match(store, /defineStore\(['"]trading-playbook['"]/)
  assert.doesNotMatch(store, /useAlertStore/)
  const websocket = read('src/composables/useWebSocket.ts')
  assert.match(websocket, /case ['"]trading_plan_alert['"]/)
  assert.match(websocket, /tradingPlaybookStore\.receiveAlert/)
})
```

- [ ] **Step 2: Run frontend contract tests and verify they fail**

Working directory: `frontend`

Run:

```bash
node --test tests/tradingPlaybookApi.test.mjs tests/tradingPlaybookStore.test.mjs
```

Expected: FAIL because the files do not exist.

- [ ] **Step 3: Implement types, API calls, store, and WebSocket routing**

Define these exact interfaces without renaming snake_case keys:

```typescript
export interface TradingPlanCandidate {
  id: number
  stock_code: string
  stock_name: string
  action_trade_date: string
  theme_name: string
  primary_mode_key: string
  supporting_mode_keys_json: string[]
  role: string
  rank: number
  recognition_json: Record<string, unknown>
  entry_trigger_json: Record<string, unknown>
  invalidation_json: Record<string, unknown>
  exit_trigger_json: Record<string, unknown>
  risk_level: 'avoid' | 'watch' | 'trial' | 'confirmed'
  position_reference: number
  evidence_json: Array<Record<string, unknown>>
  manual_overrides_json: Record<string, unknown>
  status: string
}

export interface TradingPlanVersion {
  id: number
  source_trade_date: string
  target_trade_date: string
  stage: 'preclose' | 'after_close' | 'overnight' | 'auction'
  version_no: number
  parent_plan_version_id?: number | null
  status: 'draft' | 'confirmed' | 'active' | 'superseded' | 'expired'
  market_state_json: Record<string, unknown>
  theme_ranking_json: Array<Record<string, unknown>>
  mode_radar_json: Array<Record<string, unknown>>
  rule_snapshot_json: string[]
  risk_settings_json: Record<string, unknown>
  data_quality_json: { status: string; stale?: boolean; warnings?: string[] }
  change_summary_json: Record<string, unknown>
  input_hash: string
  generated_at: string
  confirmed_at?: string | null
  confirmed_by?: string | null
  candidates: TradingPlanCandidate[]
}

export interface TradingAlertEvent {
  id: number
  plan_version_id: number
  candidate_id?: number | null
  event_type: string
  severity: string
  dedup_key: string
  triggered_at: string
  market_snapshot_json: Record<string, unknown>
  message: string
  channel_status_json: Record<string, unknown>
  acknowledged_at?: string | null
}

export interface TradingModeRule {
  id: number
  mode_key: string
  version: number
  name: string
  family: string
  style: string
  window: string
  automation_level: 'automatic' | 'assisted' | 'manual_only'
  description: string
  source_refs_json: Array<{ source_key: string; excerpt: string }>
}

export interface TradingExecutionReview {
  id: number
  trade_date: string
  plan_version_id: number
  signal_review_json: Record<string, unknown>
  manual_execution_json: Record<string, unknown>
  plan_compliance_json: Record<string, unknown>
  outcome_snapshot_json: Record<string, unknown>
  generated_at: string
  finalized_at?: string | null
}

export interface TradingPlaybookSettings {
  enabled: boolean
  trial_position_pct: number
  confirmed_position_pct: number
  hard_stop_pct: number
  max_action_candidates: number
  in_app_enabled: boolean
  wechat_enabled: false
}
```

Implement API functions against `/trading-playbook`:

```typescript
export async function getTradingPlans(tradeDate: string) {
  const { data } = await api.get('/trading-playbook/plans', { params: { trade_date: tradeDate } })
  return data as { items: TradingPlanVersion[] }
}

export async function confirmTradingPlan(planId: number, confirmedBy: string) {
  const { data } = await api.post(`/trading-playbook/plans/${planId}/confirm`, { confirmed_by: confirmedBy })
  return data as TradingPlanVersion
}

export async function getTradingAlerts(unreadOnly = true) {
  const { data } = await api.get('/trading-playbook/alerts', { params: { unread_only: unreadOnly } })
  return data as { items: TradingAlertEvent[] }
}

export async function ackTradingAlert(alertId: number) {
  const { data } = await api.post(`/trading-playbook/alerts/${alertId}/ack`)
  return data as TradingAlertEvent
}

export async function getTradingPlaybookSettings() {
  const { data } = await api.get('/trading-playbook/settings')
  return data as TradingPlaybookSettings
}
```

Add these functions with the exact HTTP calls shown: `getTradingRules` → `GET /trading-playbook/rules`; `getTradingPlan` → `GET /trading-playbook/plans/{id}`; `generateTradingPlan` → `POST /trading-playbook/plans/generate` with `stage` and `source_trade_date` params; `reviseTradingPlan` → `PUT /trading-playbook/plans/{id}`; `cancelTradingPlan` → `POST /trading-playbook/plans/{id}/cancel`; `updateTradingExecutionReview` → `PUT /trading-playbook/reviews/{tradeDate}`; `updateTradingPlaybookSettings` → `PUT /trading-playbook/settings`. Each function returns the corresponding interface without changing snake_case keys. Use this store interface:

```typescript
export const useTradingPlaybookStore = defineStore('trading-playbook', () => {
  const plans = ref<TradingPlanVersion[]>([])
  const activePlan = ref<TradingPlanVersion | null>(null)
  const alerts = ref<TradingAlertEvent[]>([])
  const unreadCount = computed(() => alerts.value.filter(item => !item.acknowledged_at).length)

  function receiveAlert(alert: TradingAlertEvent) {
    if (alerts.value.some(item => item.id === alert.id || item.dedup_key === alert.dedup_key)) return
    alerts.value.unshift(alert)
    alerts.value = alerts.value.slice(0, 200)
  }

  async function loadPlans(tradeDate: string) {
    const response = await getTradingPlans(tradeDate)
    plans.value = response.items
    activePlan.value = response.items.find(item => item.status === 'active') || response.items[0] || null
  }

  return { plans, activePlan, alerts, unreadCount, receiveAlert, loadPlans }
})
```

In `useWebSocket.ts`, create the trading store next to existing stores and add only:

```typescript
case 'trading_plan_alert':
  tradingPlaybookStore.receiveAlert(message.data as TradingAlertEvent)
  break
```

Do not call `alertStore.addMessage`, `useSpeech`, or the global desktop notification path for this message type.

- [ ] **Step 4: Run frontend contract tests and type build**

Working directory: `frontend`

Run:

```bash
node --test tests/tradingPlaybookApi.test.mjs tests/tradingPlaybookStore.test.mjs
npm run build
```

Expected: tests PASS and Vite build succeeds.

- [ ] **Step 5: Commit frontend data flow**

```bash
git add frontend/src/types/trading-playbook.ts frontend/src/api/trading-playbook.ts frontend/src/stores/trading-playbook.ts frontend/src/composables/useWebSocket.ts frontend/tests/tradingPlaybookApi.test.mjs frontend/tests/tradingPlaybookStore.test.mjs
git commit -m "feat: add trading playbook frontend state"
```

## Task 13: Build the standalone Trading Playbook page and navigation

**Files:**
- Create: `frontend/src/views/TradingPlaybook.vue`
- Modify: `frontend/src/router/index.ts`
- Modify: `frontend/src/App.vue`
- Test: `frontend/tests/tradingPlaybookUi.test.mjs`
- Test: `frontend/tests/tradingPlaybookRoutes.test.mjs`

- [ ] **Step 1: Write failing UI and route tests**

```javascript
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import test from 'node:test'
import assert from 'node:assert/strict'

const root = resolve(import.meta.dirname, '..')
const read = path => readFileSync(resolve(root, path), 'utf8')

test('standalone playbook page exposes required sections and confirmation', () => {
  const view = read('src/views/TradingPlaybook.vue')
  for (const text of ['交易预案', '市场状态', '版本时间轴', '正式行动计划', '全模式雷达', '独立提醒', '执行复盘', '规则来源']) {
    assert.match(view, new RegExp(text))
  }
  assert.match(view, /confirmTradingPlan/)
  assert.match(view, /candidates\.slice\(0,\s*3\)/)
})

test('router and navigation expose an independent trading playbook entry', () => {
  const router = read('src/router/index.ts')
  const app = read('src/App.vue')
  assert.match(router, /path:\s*['"]\/trading-playbook['"]/)
  assert.match(router, /TradingPlaybook\.vue/)
  assert.match(app, /index=['"]\/trading-playbook['"]/)
  assert.match(app, /交易预案/)
})
```

- [ ] **Step 2: Run UI tests and verify they fail**

Working directory: `frontend`

Run:

```bash
node --test tests/tradingPlaybookUi.test.mjs tests/tradingPlaybookRoutes.test.mjs
```

Expected: FAIL because the page and route do not exist.

- [ ] **Step 3: Implement the page and navigation**

Build `TradingPlaybook.vue` around this exact section structure; bind each table to the corresponding API/store collection and use Element Plus empty states when arrays are empty:

```vue
<template>
  <div class="trading-playbook">
    <div class="toolbar">
      <h3>交易预案</h3>
      <el-date-picker v-model="selectedDate" type="date" value-format="YYYY-MM-DD" :clearable="false" />
      <el-button @click="loadAll">刷新</el-button>
    </div>

    <el-alert v-if="isDegraded" type="warning" title="数据不完整，当前版本仅供观察" :closable="false" show-icon />

    <section class="panel">
      <h4>市场状态</h4>
      <el-descriptions :column="3" border>
        <el-descriptions-item label="风格">{{ selectedPlan?.market_state_json.style || '-' }}</el-descriptions-item>
        <el-descriptions-item label="窗口">{{ selectedPlan?.market_state_json.window || '-' }}</el-descriptions-item>
        <el-descriptions-item label="数据时间">{{ selectedPlan?.generated_at || '-' }}</el-descriptions-item>
      </el-descriptions>
    </section>

    <section class="panel">
      <h4>版本时间轴</h4>
      <el-timeline>
        <el-timeline-item v-for="plan in plans" :key="plan.id" :timestamp="plan.generated_at" @click="selectedPlanId = plan.id">
          {{ plan.stage }} v{{ plan.version_no }} · {{ plan.status }}
        </el-timeline-item>
      </el-timeline>
    </section>

    <section class="panel">
      <div class="section-header">
        <h4>正式行动计划</h4>
        <el-button v-if="canConfirm" type="primary" @click="confirmSelectedPlan">确认预案</el-button>
      </div>
      <div class="candidate-grid">
        <el-card v-for="item in candidates" :key="item.id">
          <h5>{{ item.stock_name }}（{{ item.stock_code }}）</h5>
          <p>{{ item.primary_mode_key }} · {{ item.role }} · {{ item.risk_level }}</p>
          <p>触发：{{ readable(item.entry_trigger_json) }}</p>
          <p>失效：{{ readable(item.invalidation_json) }}</p>
          <p>退出：{{ readable(item.exit_trigger_json) }}</p>
        </el-card>
      </div>
    </section>

    <section class="panel"><h4>全模式雷达</h4><el-table :data="selectedPlan?.mode_radar_json || []" /></section>
    <section class="panel"><h4>独立提醒</h4><el-table :data="alerts" /></section>
    <section class="panel"><h4>执行复盘</h4><el-table :data="reviewRows" /></section>
    <section class="panel"><h4>规则来源</h4><el-table :data="rules" /></section>
  </div>
</template>
```

Use these computed properties and confirmation guard:

```typescript
const selectedPlan = computed(() => plans.value.find(item => item.id === selectedPlanId.value) || activePlan.value)
const candidates = computed(() => (selectedPlan.value?.candidates || []).slice(0, 3))
const canConfirm = computed(() => selectedPlan.value?.status === 'draft' && selectedPlan.value.data_quality_json.status !== 'missing')

async function confirmSelectedPlan() {
  if (!selectedPlan.value || !canConfirm.value) return
  await confirmTradingPlan(selectedPlan.value.id, 'local-user')
  await store.loadPlans(selectedDate.value)
  ElMessage.success('预案已确认，行动级提醒已启用')
}
```

Display degraded quality with an Element Plus warning and label action cards as observation-only. Register the route with title `交易预案`, add desktop navigation near the existing trading mode entry, and add the mobile navigation item without changing existing order tests except for the explicit new entry.

- [ ] **Step 4: Run UI tests and full frontend validation**

Working directory: `frontend`

Run:

```bash
node --test tests/tradingPlaybookUi.test.mjs tests/tradingPlaybookRoutes.test.mjs tests/pageDescriptions.test.mjs tests/mobileLayout.test.mjs
npm run build
```

Expected: all tests PASS and build succeeds.

- [ ] **Step 5: Commit the page**

```bash
git add frontend/src/views/TradingPlaybook.vue frontend/src/router/index.ts frontend/src/App.vue frontend/tests/tradingPlaybookUi.test.mjs frontend/tests/tradingPlaybookRoutes.test.mjs
git commit -m "feat: add standalone trading playbook page"
```

## Task 14: Add point-in-time replay and source-backed golden scenarios

**Files:**
- Create: `backend/app/scripts/replay_trading_playbook.py`
- Create: `backend/tests/fixtures/trading_playbook_scenarios.json`
- Create: `backend/tests/test_trading_playbook_replay.py`

- [ ] **Step 1: Write failing replay tests**

```python
import json
import unittest
from pathlib import Path

from app.scripts.replay_trading_playbook import replay_scenario


class TradingPlaybookReplayTests(unittest.TestCase):
    def test_all_nineteen_modes_have_positive_or_manual_scenario(self):
        payload = json.loads(Path("tests/fixtures/trading_playbook_scenarios.json").read_text(encoding="utf-8"))
        self.assertEqual(len({row["mode_key"] for row in payload}), 19)

    def test_replay_rejects_future_facts(self):
        scenario = {
            "as_of": "2026-07-10T14:40:00",
            "facts": [{"captured_at": "2026-07-10T15:00:00", "feature": "candidate.turn_confirmed", "value": True}],
            "mode_key": "leader_turn_two",
        }
        with self.assertRaisesRegex(ValueError, "future fact"):
            replay_scenario(scenario)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run replay tests and verify they fail**

Working directory: `backend`

Run: `python -m unittest tests.test_trading_playbook_replay -v`

Expected: FAIL because replay script and fixtures are absent.

- [ ] **Step 3: Create 19 explicit scenarios and replay validation**

The fixture must contain exactly one scenario per catalog mode with:

```json
{
  "mode_key": "leader_turn_two",
  "as_of": "2026-07-10T09:26:00",
  "market_features": {"window": "divergence_to_consensus", "style": "board_flow"},
  "candidate_features": {"turn_confirmed": true, "recognition_rank": 1},
  "expected_status": "matched",
  "facts": [
    {"captured_at": "2026-07-10T09:25:00", "feature": "candidate.turn_confirmed", "value": true}
  ]
}
```

Create all 19 scenarios with these exact minimum feature sets:

| `mode_key` | `market.window` | Candidate features that are `true` or numeric | Expected |
| --- | --- | --- | --- |
| `new_theme_high_volatility` | `outbreak` | `high_volatility=true`, `theme_rank=1` | `matched` |
| `new_theme_high_position` | `outbreak` | `high_position=true` | `matched` |
| `new_theme_same_level_turnover` | `outbreak` | `same_level_turnover=true` | `matched` |
| `big_middle_army_transition` | `first_divergence` | `middle_army=true`, `theme_rank=1` | `matched` |
| `first_mover_leader` | `first_divergence` | `started_before_theme=true`, `recognition_rank=1` | `matched` |
| `unique_survivor_trial` | `divergence_exhaustion` | `unique_survivor=true` | `matched` |
| `leader_turn_two` | `divergence_to_consensus` | `turn_confirmed=true`, `recognition_rank=1` | `matched` |
| `leader_stronger_confirmation` | `stronger_confirmation` | `stronger_confirmed=true` | `matched` |
| `leader_acceleration_to_divergence` | `second_divergence` | `confirmed_leader=true`, `acceleration_to_divergence=true` | `matched` |
| `stage_three_high_low_switch` | `stage_three` | `low_position_new_start=true` | `matched` |
| `stage_transition_supplement` | `stage_three` | `supplement=true` | `matched` |
| `leader_first_bearish_rebound` | `stage_three` | `confirmed_leader=true`, `first_bearish=true` | `manual_review` |
| `trend_core_pullback` | `first_divergence` | `trend_established=true`, `resilience_rank=1`, `pullback=true` | `matched` |
| `trend_consolidation_rebreak` | `divergence_to_consensus` | `consolidation_rebreak=true`, `linkage_confirmed=true` | `matched` |
| `trend_turn_two` | `divergence_to_consensus` | `trend_turn_two=true`, `middle_army_linkage=true` | `matched` |
| `resilient_core_exhaustion` | `divergence_exhaustion` | `divergence_days=3`, `resilience_rank=1` | `matched` |
| `alive_theme_snake_arbitrage` | `divergence_exhaustion` | `theme_alive=true`, `snake_setup=true` | `manual_review` |
| `dead_pile_right_confirmation` | `divergence_to_consensus` | `theme_dead=true`, `right_reversal=true` | `manual_review` |
| `external_high_low_switch` | `stage_three` | `external_switch=true` | `manual_review` |

Every scenario also includes `planned_pullback_price`, `planned_breakout_price`, and `hard_stop_price` so materialized triggers are valid. Implement replay validation as:

```python
def replay_scenario(scenario, catalog_path=Path("app/data/trading_playbook_rules_v1.json")):
    as_of = datetime.fromisoformat(scenario["as_of"])
    for fact in scenario.get("facts", []):
        if datetime.fromisoformat(fact["captured_at"]) > as_of:
            raise ValueError("future fact")
    payload = json.loads(catalog_path.read_text(encoding="utf-8"))
    rule = next(row for row in payload["rules"] if row["mode_key"] == scenario["mode_key"])
    candidate = CandidateSnapshot("000001", "回放样本", "回放题材", scenario["candidate_features"])
    rows = ModeMatcher([rule]).evaluate(scenario["market_features"], candidate)
    return rows[0].status
```

The CLI accepts `--date`, `--stage`, and `--no-notify`. It exits with code 2 when a historical date is requested without `--no-notify`, loads the fixture, verifies every actual status equals `expected_status`, and prints the count.

- [ ] **Step 4: Run replay and matcher tests**

Working directory: `backend`

Run:

```bash
python -m unittest tests.test_trading_playbook_replay tests.test_trading_playbook_mode_matcher -v
python -m app.scripts.replay_trading_playbook --date 2026-07-10 --stage preclose --no-notify
```

Expected: tests PASS and CLI reports 19 evaluated scenarios with no future facts.

- [ ] **Step 5: Commit replay tooling**

```bash
git add backend/app/scripts/replay_trading_playbook.py backend/tests/fixtures/trading_playbook_scenarios.json backend/tests/test_trading_playbook_replay.py
git commit -m "test: add trading playbook replay scenarios"
```

## Task 15: Run end-to-end verification and update project documentation

**Files:**
- Modify: `README.md`
- Test: all focused backend and frontend suites.

- [ ] **Step 1: Add a README section with exact operating commands**

Document:

```markdown
## 交易预案

- 14:40 生成次日提前预案和尾盘建议。
- 15:10 生成当日执行复盘。
- 15:30 生成正式次日预案。
- 次日 08:50 刷新隔夜信息。
- 次日 09:26 结合竞价生成最终版本。
- 行动级提醒必须先在“交易预案”页面人工确认。
- 第一版只发送项目内提醒，不自动下单、不发送微信消息。

导入文字稿规则：

```powershell
cd backend
python -m app.scripts.import_trading_playbook_rules --source-root 'C:\Users\Administrator\Documents\Codex\2026-07-07\ysheba257-lgtm-xiaoe-scraper-https-github\xiaoe-scraper\videos'
```
```

- [ ] **Step 2: Run the complete focused backend suite**

Working directory: `backend`

Run:

```bash
python -m unittest tests.test_trading_playbook_models tests.test_trading_playbook_rule_catalog tests.test_trading_playbook_market_data tests.test_trading_playbook_market_state tests.test_trading_playbook_mode_matcher tests.test_trading_playbook_plan_service tests.test_trading_playbook_api tests.test_trading_playbook_scheduler tests.test_trading_playbook_alerts tests.test_trading_playbook_review tests.test_trading_playbook_replay tests.test_websocket_manager tests.test_main_lifespan -v
```

Expected: all tests PASS with no skipped trading-playbook tests.

- [ ] **Step 3: Run the full frontend source tests and production build**

Working directory: `frontend`

Run:

```bash
npm test
npm run build
```

Expected: all Node tests PASS and Vite build succeeds.

- [ ] **Step 4: Run the full backend test discovery and inspect git diff**

Working directory: `backend`

Run: `python -m unittest discover -s tests -v`

Expected: all backend tests PASS.

Working directory: repository root

Run: `git diff --check`

Expected: no whitespace errors.

- [ ] **Step 5: Commit documentation and final verification fixes**

```bash
git add README.md
git commit -m "docs: document trading playbook workflow"
```

The implementation is complete only when the five scheduled stages are visible, an auction plan can be manually confirmed, a matching live condition creates exactly one isolated `trading_plan_alert`, the 15:10 review is reconciled at 15:30, all 19 modes appear in the rule API and mode radar, and all commands above pass.
