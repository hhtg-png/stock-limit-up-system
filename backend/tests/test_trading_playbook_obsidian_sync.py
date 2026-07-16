import asyncio
import json
import os
import stat
import tempfile
import threading
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.database import Base
from app.models.trading_playbook import (
    TradingExecutionReview,
    TradingExecutionReviewPhaseSnapshot,
    TradingModeRule,
    TradingPlanVersion,
    TradingPlaybookObsidianExport,
)
from app.services.trading_playbook.obsidian_exporter import (
    TradingPlaybookObsidianExporter,
)
from app.services.trading_playbook.obsidian_sync import (
    TradingPlaybookObsidianSyncCoordinator,
)
from app.services.trading_playbook.obsidian_types import ObsidianArtifact
from app.services.obsidian_vault_writer import ObsidianVaultWriter, VaultWriteResult


FIXED_NOW = datetime(2026, 7, 16, 6, 40, tzinfo=timezone.utc)
TRADE_DATE = date(2026, 7, 16)


class MutableClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


class FakeExporter:
    def __init__(self) -> None:
        self.calls = []

    def render(self, artifact, *, generated_at):
        self.calls.append((artifact, generated_at, threading.get_ident()))
        return f"{artifact.snapshot_key}|{artifact.source_hash}|{generated_at.isoformat()}"


class FakeWriter:
    def __init__(self, vault_path: str | Path) -> None:
        self.enabled = True
        self.vault_path = vault_path
        self.auto_git_enabled = True
        self.changed = True
        self.write_error: BaseException | None = None
        self.write_calls = []
        self.commit_calls = []
        self.commit_result = {"enabled": True, "committed": True}
        self.after_write = None

    def configured_vault(self):
        raw = str(self.vault_path or "").strip()
        return Path(raw) if raw else None

    def resolve_target(self, relative_path, *, allowed_roots):
        normalized = relative_path.replace("\\", "/")
        if not any(
            normalized == root or normalized.startswith(f"{root}/")
            for root in allowed_roots
        ):
            raise ValueError("outside fake writer allowlist")
        return Path(self.vault_path).joinpath(*normalized.split("/"))

    def write_text(self, relative_path, content, *, allowed_roots):
        self.write_calls.append(
            (relative_path, content, allowed_roots, threading.get_ident())
        )
        if self.write_error is not None:
            raise self.write_error
        if self.after_write is not None:
            self.after_write()
        return VaultWriteResult(
            relative_path=relative_path,
            absolute_path=Path(self.vault_path) / relative_path,
            changed=self.changed,
        )

    def commit_paths(self, relative_paths, *, allowed_roots, message):
        self.commit_calls.append(
            (tuple(relative_paths), allowed_roots, message, threading.get_ident())
        )
        if isinstance(self.commit_result, BaseException):
            raise self.commit_result
        return dict(self.commit_result)


class FakeBuilder:
    def __init__(self, *, by_date=None, rules=()) -> None:
        self.by_date = dict(by_date or {})
        self.rules = tuple(rules)
        self.calls = []

    async def build_rule_artifacts(self, catalog_version="v2"):
        self.calls.append(("rules", catalog_version))
        return self.rules

    async def build_plan_artifact(self, plan_version_id):
        self.calls.append(("plan", plan_version_id))
        return self.by_date[("plan", plan_version_id)]

    async def build_review_artifact(self, review_id, *, phase):
        self.calls.append(("review", review_id, phase))
        return self.by_date[("review", review_id, phase)]

    async def build_alerts_artifact(self, trade_date):
        self.calls.append(("alerts", trade_date))
        return self.by_date[("alerts", trade_date)]

    async def build_daily_index_artifact(self, trade_date):
        self.calls.append(("index", trade_date))
        return self.by_date[("index", trade_date)]

    async def build_dashboard_artifact(self, trade_date):
        self.calls.append(("dashboard", trade_date))
        return self.by_date[("dashboard", trade_date)]


def artifact(
    marker: str,
    *,
    trade_date: date = TRADE_DATE,
    snapshot_key: str = "daily-index:2026-07-16",
    immutable: bool = False,
    entity_type: str = "daily_index",
    entity_id: int | None = None,
    phase: str = "reconcile",
    target_path: str = (
        "30_TradingPlaybook/Daily/Auto/2026/2026-07-16/index.md"
    ),
) -> ObsidianArtifact:
    return ObsidianArtifact(
        snapshot_key=snapshot_key,
        trade_date=trade_date,
        entity_type=entity_type,
        entity_id=entity_id,
        phase=phase,
        target_path=target_path,
        immutable=immutable,
        payload={
            "type": f"test_{entity_type}",
            "marker": marker,
            "manual_required": True,
            "auto_execute": False,
        },
    )


def plan_row(
    *,
    source_trade_date=TRADE_DATE,
    target_trade_date=TRADE_DATE,
    stage="preclose",
    version_no=1,
):
    return TradingPlanVersion(
        source_trade_date=source_trade_date,
        target_trade_date=target_trade_date,
        stage=stage,
        version_no=version_no,
        status="draft",
        market_state_json={},
        theme_ranking_json=[],
        mode_radar_json=[],
        rule_snapshot_json=[],
        risk_settings_json={},
        data_quality_json={},
        change_summary_json={},
        input_hash=f"input-{source_trade_date}-{target_trade_date}-{version_no}",
        generated_at=datetime(2026, 7, 16, 14, 40),
    )


def review_row(plan_id: int, *, trade_date=TRADE_DATE, finalized=False):
    return TradingExecutionReview(
        trade_date=trade_date,
        plan_version_id=plan_id,
        signal_review_json={},
        manual_execution_json={},
        plan_compliance_json={},
        outcome_snapshot_json={},
        data_quality_json={},
        generated_at=datetime(2026, 7, 16, 15, 10),
        finalized_at=(
            datetime(2026, 7, 16, 15, 30) if finalized else None
        ),
    )


class TradingPlaybookObsidianSyncTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        database_path = Path(self.temporary_directory.name) / "obsidian-sync.db"
        self.database_url = f"sqlite+aiosqlite:///{database_path.as_posix()}"
        self.engine = create_async_engine(
            self.database_url,
            connect_args={"timeout": 30},
            poolclass=NullPool,
        )
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        self.session_factory = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
        )
        self.clock = MutableClock(FIXED_NOW)
        self.coordinator = self._coordinator(self.session_factory)

    async def asyncTearDown(self) -> None:
        await self.engine.dispose()
        self.temporary_directory.cleanup()

    def _coordinator(
        self,
        session_factory,
        *,
        builder=None,
        exporter=None,
        writer=None,
        coordinator_class=TradingPlaybookObsidianSyncCoordinator,
    ):
        return coordinator_class(
            session_factory=session_factory,
            builder=builder if builder is not None else object(),
            exporter=exporter if exporter is not None else object(),
            writer=writer if writer is not None else object(),
            clock=self.clock,
        )

    async def _rows(self, snapshot_key: str | None = None):
        async with self.session_factory() as session:
            statement = select(TradingPlaybookObsidianExport).order_by(
                TradingPlaybookObsidianExport.snapshot_key,
                TradingPlaybookObsidianExport.snapshot_version,
            )
            if snapshot_key is not None:
                statement = statement.where(
                    TradingPlaybookObsidianExport.snapshot_key == snapshot_key
                )
            return list((await session.execute(statement)).scalars())

    async def test_status_is_stable_when_empty_disabled_or_unconfigured(self):
        missing_vault = Path(self.temporary_directory.name) / "missing-vault"
        writers = (
            FakeWriter(""),
            FakeWriter(missing_vault),
        )
        writers[0].enabled = False
        for writer in writers:
            with self.subTest(writer=writer):
                coordinator = self._coordinator(
                    self.session_factory,
                    writer=writer,
                )

                status = await coordinator.get_status()

                self.assertEqual(
                    status,
                    {
                        "enabled": bool(writer.enabled),
                        "configured": bool(str(writer.vault_path).strip()),
                        "vault_exists": False,
                        "auto_git_enabled": True,
                        "last_success_at": None,
                        "last_trade_date": None,
                        "last_phase": None,
                        "pending_count": 0,
                        "paused_count": 0,
                        "failed_count": 0,
                        "last_error": None,
                        "recent_files": [],
                        "dashboard_path": "Dashboards/交易预案.md",
                        "dashboard_openable": False,
                    },
                )
        self.assertFalse(missing_vault.exists())

    async def test_status_aggregates_rows_and_only_exposes_relative_paths(self):
        vault = Path(self.temporary_directory.name) / "vault"
        dashboard = vault / "Dashboards" / "交易预案.md"
        dashboard.parent.mkdir(parents=True)
        dashboard.write_text("dashboard", encoding="utf-8")
        writer = FakeWriter(vault)
        coordinator = self._coordinator(
            self.session_factory,
            writer=writer,
        )
        repeated = "30_TradingPlaybook/Daily/Auto/2026/repeated.md"
        now = datetime(2026, 7, 16, 15, 30)

        def row(
            marker,
            *,
            status,
            target_path,
            trade_date=TRADE_DATE,
            phase="reconcile",
            exported_at=None,
            last_error=None,
            updated_at=now,
        ):
            return TradingPlaybookObsidianExport(
                snapshot_key=f"status:{marker}",
                snapshot_version=1,
                trade_date=trade_date,
                entity_type="daily_index",
                entity_id=None,
                phase=phase,
                target_path=target_path,
                source_hash=f"{marker:0>64}"[-64:],
                snapshot_json={"payload": {"marker": marker}},
                immutable=False,
                status=status,
                attempt_no=0,
                next_attempt_at=None,
                last_error=last_error,
                git_status_json={"internal": "must-not-leak"},
                exported_at=exported_at,
                created_at=now,
                updated_at=updated_at,
            )

        async with self.session_factory() as session:
            session.add_all(
                [
                    row(
                        "old-repeat",
                        status="written",
                        target_path=repeated,
                        exported_at=now - timedelta(hours=3),
                    ),
                    row(
                        "dashboard",
                        status="written",
                        target_path="Dashboards/交易预案.md",
                        exported_at=now - timedelta(hours=2),
                        phase="after_close",
                    ),
                    row(
                        "new-repeat",
                        status="written",
                        target_path=repeated,
                        trade_date=date(2026, 7, 17),
                        phase="auction",
                        exported_at=now - timedelta(hours=1),
                    ),
                    row(
                        "pending",
                        status="pending",
                        target_path=(
                            "30_TradingPlaybook/Alerts/Auto/2026/pending.md"
                        ),
                    ),
                    row(
                        "paused",
                        status="paused",
                        target_path=(
                            "30_TradingPlaybook/Daily/Auto/2026/paused.md"
                        ),
                    ),
                    row(
                        "failed",
                        status="failed",
                        target_path=(
                            "30_TradingPlaybook/Reviews/Auto/2026/failed.md"
                        ),
                        last_error="write failed at C:\\secret\\vault\\plan.md",
                        updated_at=now + timedelta(minutes=1),
                    ),
                ]
            )
            await session.commit()
        before = [
            (item.id, item.status, item.updated_at) for item in await self._rows()
        ]

        status = await coordinator.get_status()

        self.assertTrue(status["enabled"])
        self.assertTrue(status["configured"])
        self.assertTrue(status["vault_exists"])
        self.assertTrue(status["auto_git_enabled"])
        self.assertEqual(status["pending_count"], 1)
        self.assertEqual(status["paused_count"], 1)
        self.assertEqual(status["failed_count"], 1)
        self.assertEqual(
            status["last_success_at"],
            datetime(2026, 7, 16, 14, 30, tzinfo=timezone(timedelta(hours=8))),
        )
        self.assertEqual(status["last_trade_date"], date(2026, 7, 17))
        self.assertEqual(status["last_phase"], "auction")
        self.assertEqual(
            status["recent_files"],
            [repeated, "Dashboards/交易预案.md"],
        )
        self.assertNotIn("secret", status["last_error"])
        self.assertEqual(status["dashboard_path"], "Dashboards/交易预案.md")
        self.assertTrue(status["dashboard_openable"])
        self.assertEqual(
            [(item.id, item.status, item.updated_at) for item in await self._rows()],
            before,
        )

    async def test_status_redacts_delimited_posix_absolute_paths(self):
        writer = FakeWriter(self.temporary_directory.name)
        coordinator = self._coordinator(
            self.session_factory,
            writer=writer,
        )
        now = datetime(2026, 7, 16, 15, 30)
        async with self.session_factory() as session:
            row = TradingPlaybookObsidianExport(
                snapshot_key="status:absolute-error",
                snapshot_version=1,
                trade_date=TRADE_DATE,
                entity_type="daily_index",
                entity_id=None,
                phase="reconcile",
                target_path=(
                    "30_TradingPlaybook/Daily/Auto/2026/status-error.md"
                ),
                source_hash="a" * 64,
                snapshot_json={"payload": {}},
                immutable=False,
                status="failed",
                attempt_no=1,
                next_attempt_at=None,
                last_error=None,
                git_status_json={"state": "write_failed"},
                exported_at=None,
                created_at=now,
                updated_at=now,
            )
            session.add(row)
            await session.commit()
            row_id = row.id

        errors = (
            ("failed opening '/srv/private/vault/file.md'", "srv"),
            ("failed(/home/admin/private/file.md)", "home"),
            ("failed=/opt/private/file.md", "opt"),
            ("failed='//server/share/private/file.md'", "server"),
            (r"failed=(\Users\Admin\secret.md)", "Users"),
            ("failed=file:///srv/private/file.md", "srv"),
        )
        for error, private_fragment in errors:
            with self.subTest(error=error):
                async with self.session_factory() as session:
                    await session.execute(
                        update(TradingPlaybookObsidianExport)
                        .where(TradingPlaybookObsidianExport.id == row_id)
                        .values(last_error=error)
                    )
                    await session.commit()

                status = await coordinator.get_status()

                self.assertEqual(status["last_error"], "Obsidian export failed")
                self.assertNotIn(private_fragment, repr(status))

    async def test_status_propagates_writer_errors_and_cancellation(self):
        for error in (
            RuntimeError("configured status unavailable"),
            asyncio.CancelledError(),
        ):
            writer = FakeWriter(self.temporary_directory.name)

            def fail(error=error):
                raise error

            writer.configured_vault = fail
            coordinator = self._coordinator(
                self.session_factory,
                writer=writer,
            )
            with self.subTest(error=error):
                with self.assertRaises(type(error)):
                    await coordinator.get_status()

    async def test_same_key_and_hash_reuses_the_current_row(self):
        item = artifact("same")

        first = (await self.coordinator.enqueue_artifacts([item]))[0]
        self.clock.value += timedelta(hours=2)
        second = (await self.coordinator.enqueue_artifacts([item]))[0]

        self.assertEqual(first.id, second.id)
        self.assertEqual(first.snapshot_version, 1)
        rows = await self._rows(item.snapshot_key)
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0].snapshot_json,
            {
                "payload": item.payload_json(),
                "generated_at": "2026-07-16T06:40:00Z",
            },
        )

    async def test_enqueue_stage_builds_strict_stage_batch_before_persisting(self):
        artifacts = (artifact("stage"),)
        builder = SimpleNamespace(
            build_stage_artifacts=AsyncMock(return_value=artifacts)
        )
        coordinator = self._coordinator(self.session_factory, builder=builder)
        coordinator.enqueue_artifacts = AsyncMock(return_value=("row",))

        rows = await coordinator.enqueue_stage(
            date(2026, 7, 16),
            "auction",
            plan_version_ids=(17,),
            review_ids=(),
            include_rules=True,
        )

        self.assertEqual(rows, ("row",))
        builder.build_stage_artifacts.assert_awaited_once_with(
            trade_date=date(2026, 7, 16),
            phase="auction",
            plan_version_ids=(17,),
            review_ids=(),
            include_rules=True,
        )
        coordinator.enqueue_artifacts.assert_awaited_once_with(artifacts)

    async def test_batch_freezes_one_generated_at_and_complete_payload(self):
        calls = 0

        def ticking_clock():
            nonlocal calls
            value = FIXED_NOW + timedelta(minutes=calls)
            calls += 1
            return value

        coordinator = TradingPlaybookObsidianSyncCoordinator(
            session_factory=self.session_factory,
            builder=object(),
            exporter=object(),
            writer=object(),
            clock=ticking_clock,
        )
        items = [
            artifact("one", snapshot_key="alerts:2026-07-16"),
            artifact("two", snapshot_key="dashboard:trading-playbook"),
        ]

        rows = await coordinator.enqueue_artifacts(items)

        self.assertEqual(calls, 1)
        self.assertEqual(
            [row.snapshot_json["payload"] for row in rows],
            [item.payload_json() for item in items],
        )
        self.assertEqual(
            {row.snapshot_json["generated_at"] for row in rows},
            {"2026-07-16T06:40:00Z"},
        )

    async def test_immutable_hash_conflict_is_a_failed_audit_row(self):
        original = artifact(
            "original",
            snapshot_key="plan:42",
            immutable=True,
            entity_type="plan",
            entity_id=42,
            phase="preclose",
            target_path=(
                "30_TradingPlaybook/Daily/Auto/2026/2026-07-16/"
                "preclose-v1.md"
            ),
        )
        conflicting = artifact(
            "conflicting",
            snapshot_key="plan:42",
            immutable=True,
            entity_type="plan",
            entity_id=42,
            phase="preclose",
            target_path=original.target_path,
        )

        first = (await self.coordinator.enqueue_artifacts([original]))[0]
        conflict = (await self.coordinator.enqueue_artifacts([conflicting]))[0]

        self.assertEqual(first.snapshot_version, 1)
        self.assertEqual(first.status, "pending")
        self.assertEqual(conflict.snapshot_version, 2)
        self.assertEqual(conflict.status, "failed")
        self.assertEqual(
            conflict.last_error,
            "immutable_snapshot_hash_conflict",
        )
        self.assertEqual(conflict.source_hash, conflicting.source_hash)
        self.assertEqual(conflict.snapshot_json["payload"], conflicting.payload_json())
        rows = await self._rows(original.snapshot_key)
        self.assertEqual(rows[0].source_hash, original.source_hash)
        self.assertEqual(rows[0].status, "pending")
        self.assertIsNone(rows[0].last_error)

    async def test_repeated_immutable_hashes_reuse_their_existing_rows(self):
        original = artifact("original", snapshot_key="rule:v2:mode_01", immutable=True)
        conflicting = artifact(
            "conflict",
            snapshot_key=original.snapshot_key,
            immutable=True,
        )
        first = (await self.coordinator.enqueue_artifacts([original]))[0]
        conflict = (await self.coordinator.enqueue_artifacts([conflicting]))[0]

        repeated_first = (await self.coordinator.enqueue_artifacts([original]))[0]
        repeated_conflict = (await self.coordinator.enqueue_artifacts([conflicting]))[0]

        self.assertEqual(repeated_first.id, first.id)
        self.assertEqual(repeated_conflict.id, conflict.id)
        self.assertEqual(len(await self._rows(original.snapshot_key)), 2)

    async def test_mutable_new_hash_increments_and_supersedes_unwritten_rows(self):
        first_artifact = artifact("one")
        second_artifact = artifact("two")
        first = (await self.coordinator.enqueue_artifacts([first_artifact]))[0]

        second = (await self.coordinator.enqueue_artifacts([second_artifact]))[0]

        self.assertEqual(second.snapshot_version, 2)
        rows = await self._rows(first.snapshot_key)
        self.assertEqual([row.status for row in rows], ["superseded", "pending"])
        self.assertEqual(rows[0].id, first.id)

    async def test_mutable_new_hash_supersedes_a_failed_old_row(self):
        first = (await self.coordinator.enqueue_artifacts([artifact("one")]))[0]
        async with self.session_factory() as session:
            await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(TradingPlaybookObsidianExport.id == first.id)
                .values(
                    status="failed",
                    attempt_no=2,
                    next_attempt_at=datetime(2026, 7, 16, 14, 45),
                    last_error="disk unavailable",
                )
            )
            await session.commit()

        second = (await self.coordinator.enqueue_artifacts([artifact("two")]))[0]

        self.assertEqual(second.snapshot_version, 2)
        rows = await self._rows(first.snapshot_key)
        self.assertEqual([row.status for row in rows], ["superseded", "pending"])
        self.assertEqual(
            rows[0].next_attempt_at,
            datetime(2026, 7, 16, 14, 41),
        )
        self.assertEqual(rows[0].last_error, "disk unavailable")

    async def test_mutable_new_hash_preserves_written_history(self):
        first_artifact = artifact("one")
        first = (await self.coordinator.enqueue_artifacts([first_artifact]))[0]
        async with self.session_factory() as session:
            await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(TradingPlaybookObsidianExport.id == first.id)
                .values(status="written")
            )
            await session.commit()

        second = (await self.coordinator.enqueue_artifacts([artifact("two")]))[0]

        self.assertEqual(second.snapshot_version, 2)
        rows = await self._rows(first.snapshot_key)
        self.assertEqual([row.status for row in rows], ["written", "pending"])

    async def test_mutable_return_to_an_older_hash_creates_a_new_current_version(self):
        original = artifact("one")
        changed = artifact("two")
        await self.coordinator.enqueue_artifacts([original])
        await self.coordinator.enqueue_artifacts([changed])

        current = (await self.coordinator.enqueue_artifacts([original]))[0]

        self.assertEqual(current.snapshot_version, 3)
        rows = await self._rows(original.snapshot_key)
        self.assertEqual(
            [row.status for row in rows],
            ["superseded", "superseded", "pending"],
        )

    async def test_two_concurrent_sessions_create_only_one_unique_version(self):
        coordinators = [
            self.coordinator,
            self._coordinator(self.session_factory),
        ]
        item = artifact("concurrent")
        results = await asyncio.gather(
            *(coordinator.enqueue_artifacts([item]) for coordinator in coordinators)
        )

        self.assertEqual(results[0][0].id, results[1][0].id)
        rows = await self._rows(item.snapshot_key)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].snapshot_version, 1)

    async def test_concurrent_mutable_hashes_allocate_unique_monotonic_versions(self):
        original = artifact("original")
        await self.coordinator.enqueue_artifacts([original])
        coordinators = [
            self.coordinator,
            self._coordinator(self.session_factory),
        ]

        results = await asyncio.gather(
            coordinators[0].enqueue_artifacts([artifact("change-a")]),
            coordinators[1].enqueue_artifacts([artifact("change-b")]),
        )

        self.assertEqual(
            {result[0].snapshot_version for result in results},
            {2, 3},
        )
        rows = await self._rows(original.snapshot_key)
        self.assertEqual(
            [row.snapshot_version for row in rows],
            [1, 2, 3],
        )
        self.assertEqual(
            [row.status for row in rows],
            ["superseded", "superseded", "pending"],
        )

    async def test_integrity_error_before_commit_rolls_back_and_rereads_winner(self):
        base_factory = self.session_factory

        class UniqueFailureBeforeCommitSession(AsyncSession):
            should_signal = True
            rollback_calls = 0

            async def commit(inner_self) -> None:
                if UniqueFailureBeforeCommitSession.should_signal:
                    pending = next(
                        row
                        for row in inner_self.new
                        if isinstance(row, TradingPlaybookObsidianExport)
                    )
                    values = {
                        column.name: getattr(pending, column.name)
                        for column in TradingPlaybookObsidianExport.__table__.columns
                        if column.name != "id"
                    }
                    UniqueFailureBeforeCommitSession.should_signal = False
                    await inner_self.rollback()
                    async with base_factory() as winning_session:
                        winning_session.add(TradingPlaybookObsidianExport(**values))
                        await winning_session.commit()
                    raise IntegrityError(
                        "simulated concurrent unique winner",
                        {},
                        RuntimeError("unique constraint"),
                    )
                await super(UniqueFailureBeforeCommitSession, inner_self).commit()

            async def rollback(inner_self) -> None:
                UniqueFailureBeforeCommitSession.rollback_calls += 1
                await super(UniqueFailureBeforeCommitSession, inner_self).rollback()

        signaling_factory = async_sessionmaker(
            self.engine,
            class_=UniqueFailureBeforeCommitSession,
            expire_on_commit=False,
        )
        coordinator = self._coordinator(signaling_factory)
        item = artifact("integrity-reread", snapshot_key="alerts:integrity")

        row = (await coordinator.enqueue_artifacts([item]))[0]

        self.assertFalse(UniqueFailureBeforeCommitSession.should_signal)
        self.assertGreaterEqual(UniqueFailureBeforeCommitSession.rollback_calls, 2)
        self.assertEqual(row.snapshot_version, 1)
        rows = await self._rows(item.snapshot_key)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].id, row.id)

    async def test_new_mutable_claim_waits_for_superseded_live_lease(self):
        old_lease_acquired = asyncio.Event()
        release_old_worker = asyncio.Event()

        async def old_worker():
            claimed = await self.coordinator._claim_due(limit=1)
            old_lease_acquired.set()
            await release_old_worker.wait()
            return claimed

        async def superseding_worker():
            await asyncio.wait_for(old_lease_acquired.wait(), timeout=5)
            try:
                new_row = (
                    await self.coordinator.enqueue_artifacts([artifact("new")])
                )[0]
                blocked = await self.coordinator._claim_due(limit=1)
                return new_row, blocked
            finally:
                release_old_worker.set()

        old_row = (await self.coordinator.enqueue_artifacts([artifact("old")]))[0]
        old_claim, (new_row, blocked_claim) = await asyncio.gather(
            old_worker(),
            superseding_worker(),
        )

        self.assertEqual([row.id for row in old_claim], [old_row.id])
        self.assertEqual(blocked_claim, ())
        rows = await self._rows(old_row.snapshot_key)
        self.assertEqual(rows[0].status, "superseded")
        self.assertEqual(rows[0].next_attempt_at, datetime(2026, 7, 16, 14, 41))
        self.assertEqual(rows[1].id, new_row.id)
        self.assertEqual(rows[1].status, "pending")

        self.clock.value += timedelta(seconds=61)
        after_expiry = await self.coordinator._claim_due(limit=2)
        self.assertEqual([row.id for row in after_expiry], [new_row.id])

    async def test_claim_reload_drops_row_superseded_after_claim_commit(self):
        claim_committed = asyncio.Event()
        release_reload = asyncio.Event()

        class CommitBarrierSession(AsyncSession):
            async def commit(inner_self) -> None:
                await super(CommitBarrierSession, inner_self).commit()
                claim_committed.set()
                await release_reload.wait()

        claim_factory = async_sessionmaker(
            self.engine,
            class_=CommitBarrierSession,
            expire_on_commit=False,
        )
        claim_coordinator = self._coordinator(claim_factory)
        old_row = (await self.coordinator.enqueue_artifacts([artifact("old")]))[0]

        claim_task = asyncio.create_task(claim_coordinator._claim_due(limit=1))
        await asyncio.wait_for(claim_committed.wait(), timeout=5)
        new_row = (
            await self.coordinator.enqueue_artifacts([artifact("new")])
        )[0]
        release_reload.set()
        claimed = await claim_task

        self.assertEqual(claimed, ())
        rows = await self._rows(old_row.snapshot_key)
        self.assertEqual(rows[0].status, "superseded")
        self.assertEqual(rows[0].next_attempt_at, datetime(2026, 7, 16, 14, 41))
        self.assertEqual(rows[1].id, new_row.id)

    async def test_claim_reload_requires_the_exact_lease_token(self):
        claim_committed = asyncio.Event()
        release_reload = asyncio.Event()

        class CommitBarrierSession(AsyncSession):
            async def commit(inner_self) -> None:
                await super(CommitBarrierSession, inner_self).commit()
                claim_committed.set()
                await release_reload.wait()

        claim_factory = async_sessionmaker(
            self.engine,
            class_=CommitBarrierSession,
            expire_on_commit=False,
        )
        claim_coordinator = self._coordinator(claim_factory)
        row = (await self.coordinator.enqueue_artifacts([artifact("lease")]))[0]

        claim_task = asyncio.create_task(claim_coordinator._claim_due(limit=1))
        await asyncio.wait_for(claim_committed.wait(), timeout=5)
        async with self.session_factory() as session:
            await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(TradingPlaybookObsidianExport.id == row.id)
                .values(next_attempt_at=datetime(2026, 7, 16, 14, 42))
            )
            await session.commit()
        release_reload.set()

        self.assertEqual(await claim_task, ())

    async def test_claim_reload_rejects_a_token_that_expired_after_commit(self):
        claim_committed = asyncio.Event()
        release_reload = asyncio.Event()

        class CommitBarrierSession(AsyncSession):
            async def commit(inner_self) -> None:
                await super(CommitBarrierSession, inner_self).commit()
                claim_committed.set()
                await release_reload.wait()

        claim_factory = async_sessionmaker(
            self.engine,
            class_=CommitBarrierSession,
            expire_on_commit=False,
        )
        first_worker = self._coordinator(claim_factory)
        second_worker = self._coordinator(self.session_factory)
        row = (await self.coordinator.enqueue_artifacts([artifact("expiry")]))[0]

        first_task = asyncio.create_task(first_worker._claim_due(limit=1))
        await asyncio.wait_for(claim_committed.wait(), timeout=5)
        self.clock.value = datetime(2026, 7, 16, 6, 41, 1, tzinfo=timezone.utc)
        release_reload.set()
        first_claim = await first_task

        second_claim = await second_worker._claim_due(limit=1)

        self.assertEqual(first_claim, ())
        self.assertEqual([claimed.id for claimed in second_claim], [row.id])
        self.assertEqual(
            second_claim[0].next_attempt_at,
            datetime(2026, 7, 16, 14, 42, 1),
        )

    async def test_claim_is_atomic_without_adding_a_persistent_status(self):
        row = (await self.coordinator.enqueue_artifacts([artifact("claim")]))[0]
        second = self._coordinator(self.session_factory)

        first_claim, second_claim = await asyncio.gather(
            self.coordinator._claim_due(limit=1),
            second._claim_due(limit=1),
        )

        claimed = [*first_claim, *second_claim]
        self.assertEqual([item.id for item in claimed], [row.id])
        stored = (await self._rows(row.snapshot_key))[0]
        self.assertEqual(stored.status, "pending")
        self.assertEqual(stored.next_attempt_at, datetime(2026, 7, 16, 14, 41))
        self.assertEqual(stored.git_status_json["state"], "lease_claimed")
        self.assertEqual(
            stored.git_status_json["lease_token"],
            self.coordinator._canonical_datetime(
                FIXED_NOW + timedelta(minutes=1)
            ),
        )

        self.assertEqual(await self.coordinator._claim_due(limit=1), ())
        self.clock.value += timedelta(seconds=61)
        retried = await self.coordinator._claim_due(limit=1)
        self.assertEqual([item.id for item in retried], [row.id])

    async def test_claim_excludes_conflicts_not_due_superseded_and_stale_mutable(self):
        immutable = artifact("original", snapshot_key="plan:9", immutable=True)
        original = (await self.coordinator.enqueue_artifacts([immutable]))[0]
        conflict = (
            await self.coordinator.enqueue_artifacts(
                [artifact("conflict", snapshot_key="plan:9", immutable=True)]
            )
        )[0]
        stale = (await self.coordinator.enqueue_artifacts([artifact("stale")]))[0]
        latest = (await self.coordinator.enqueue_artifacts([artifact("latest")]))[0]
        future = (
            await self.coordinator.enqueue_artifacts(
                [artifact("future", snapshot_key="alerts:future")]
            )
        )[0]
        async with self.session_factory() as session:
            await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(TradingPlaybookObsidianExport.id == conflict.id)
                .values(status="pending", last_error=None)
            )
            await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(TradingPlaybookObsidianExport.id == stale.id)
                .values(status="failed", next_attempt_at=None)
            )
            await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(TradingPlaybookObsidianExport.id == future.id)
                .values(next_attempt_at=datetime(2026, 7, 16, 15, 0))
            )
            await session.commit()

        claimed = await self.coordinator._claim_due(limit=10)

        claimed_ids = {row.id for row in claimed}
        self.assertEqual(claimed_ids, {original.id, latest.id})
        self.assertNotIn(conflict.id, claimed_ids)
        self.assertNotIn(stale.id, claimed_ids)
        self.assertNotIn(future.id, claimed_ids)

    async def test_disabled_and_unconfigured_writers_pause_without_failure(self):
        for enabled, vault_path in (
            (False, self.temporary_directory.name),
            (True, ""),
        ):
            with self.subTest(enabled=enabled, vault_path=vault_path):
                item = artifact(
                    f"pause-{enabled}-{bool(vault_path)}",
                    snapshot_key=f"alerts:pause:{enabled}:{bool(vault_path)}",
                    target_path=(
                        "30_TradingPlaybook/Alerts/Auto/2026/"
                        f"pause-{enabled}-{bool(vault_path)}.md"
                    ),
                )
                row = (await self.coordinator.enqueue_artifacts([item]))[0]
                writer = FakeWriter(vault_path)
                writer.enabled = enabled
                coordinator = self._coordinator(
                    self.session_factory,
                    exporter=FakeExporter(),
                    writer=writer,
                )

                result = await coordinator.process_due()

                stored = (await self._rows(item.snapshot_key))[0]
                self.assertEqual(stored.id, row.id)
                self.assertEqual(stored.status, "paused")
                self.assertEqual(stored.attempt_no, 0)
                self.assertIsNone(stored.last_error)
                self.assertEqual(result.pending_files, (item.target_path,))
                self.assertEqual(writer.write_calls, [])

                writer.enabled = True
                writer.vault_path = self.temporary_directory.name
                resumed = await coordinator.resume_paused()
                self.assertEqual(resumed, 1)
                stored = (await self._rows(item.snapshot_key))[0]
                self.assertEqual(stored.status, "pending")
                self.assertIsNone(stored.next_attempt_at)
                self.assertEqual(stored.attempt_no, 0)
                async with self.session_factory() as session:
                    await session.execute(
                        update(TradingPlaybookObsidianExport)
                        .where(TradingPlaybookObsidianExport.id == row.id)
                        .values(status="written")
                    )
                    await session.commit()

    async def test_write_success_skip_and_file_io_use_worker_threads(self):
        changed = artifact("changed", snapshot_key="alerts:changed")
        unchanged = artifact(
            "unchanged",
            snapshot_key="alerts:unchanged",
            target_path="30_TradingPlaybook/Alerts/Auto/2026/unchanged.md",
        )
        await self.coordinator.enqueue_artifacts([changed, unchanged])
        writer = FakeWriter(self.temporary_directory.name)
        original_write = writer.write_text

        def write_by_path(relative_path, content, *, allowed_roots):
            writer.changed = relative_path == changed.target_path
            return original_write(
                relative_path,
                content,
                allowed_roots=allowed_roots,
            )

        writer.write_text = write_by_path
        exporter = FakeExporter()
        coordinator = self._coordinator(
            self.session_factory,
            exporter=exporter,
            writer=writer,
        )
        event_loop_thread = threading.get_ident()

        result = await coordinator.process_due()

        self.assertEqual(result.written_files, (changed.target_path,))
        self.assertEqual(result.skipped_files, (unchanged.target_path,))
        self.assertEqual(result.failed_files, ())
        self.assertEqual([row.status for row in await self._rows()], ["written", "written"])
        self.assertTrue(
            all(call[3] != event_loop_thread for call in writer.write_calls)
        )
        self.assertTrue(
            all(call[3] != event_loop_thread for call in writer.commit_calls)
        )
        self.assertTrue(
            all(call[2] == event_loop_thread for call in exporter.calls)
        )
        stored_by_key = {row.snapshot_key: row for row in await self._rows()}
        self.assertEqual(
            stored_by_key[unchanged.snapshot_key].git_status_json["state"],
            "not_needed",
        )
        self.assertEqual(
            stored_by_key[changed.snapshot_key].git_status_json["state"],
            "git_complete",
        )

    async def test_real_exporter_and_writer_restore_the_frozen_snapshot(self):
        vault = Path(self.temporary_directory.name) / "vault"
        item = ObsidianArtifact(
            snapshot_key="alerts:2026-07-16",
            trade_date=TRADE_DATE,
            entity_type="alerts",
            entity_id=None,
            phase="reconcile",
            target_path="30_TradingPlaybook/Alerts/Auto/2026/2026-07-16.md",
            immutable=False,
            payload={
                "type": "trading_alert_timeline",
                "trade_date": TRADE_DATE,
                "timeline": [],
                "manual_required": True,
                "auto_execute": False,
            },
        )
        await self.coordinator.enqueue_artifacts([item])
        coordinator = self._coordinator(
            self.session_factory,
            exporter=TradingPlaybookObsidianExporter(),
            writer=ObsidianVaultWriter(
                enabled=True,
                vault_path=vault,
                auto_git_enabled=False,
            ),
        )

        result = await coordinator.process_due()

        exported = vault / Path(item.target_path)
        self.assertEqual(result.written_files, (item.target_path,))
        self.assertTrue(exported.is_file())
        content = exported.read_text(encoding="utf-8")
        self.assertIn(f'source_hash: "{item.source_hash}"', content)
        self.assertIn('generated_at: "2026-07-16T06:40:00.000000Z"', content)
        self.assertEqual((await self._rows(item.snapshot_key))[0].status, "written")
        lock_root = (
            vault
            / "30_TradingPlaybook"
            / "Daily"
            / "Auto"
            / ".sync-locks"
        )
        lock_files = list(lock_root.glob("*.lock"))
        self.assertGreaterEqual(len(lock_files), 2)
        self.assertTrue(all(path.suffix == ".lock" for path in lock_files))
        self.assertEqual(list(lock_root.glob("*.md")), [])
        if os.name != "nt":
            self.assertEqual(stat.S_IMODE(lock_root.stat().st_mode), 0o770)
            self.assertTrue(
                all(stat.S_IMODE(path.stat().st_mode) == 0o660 for path in lock_files)
            )

    async def test_lock_root_rejects_a_lexical_symlink_inside_the_vault(self):
        vault = Path(self.temporary_directory.name) / "symlink-vault"
        auto_root = vault / "30_TradingPlaybook" / "Daily" / "Auto"
        real_root = auto_root / "real-locks"
        real_root.mkdir(parents=True)
        lock_root = auto_root / ".sync-locks"
        try:
            lock_root.symlink_to(real_root, target_is_directory=True)
        except OSError as exc:
            self.skipTest(f"directory symlinks unavailable: {exc}")
        coordinator = self._coordinator(
            self.session_factory,
            exporter=FakeExporter(),
            writer=ObsidianVaultWriter(
                enabled=True,
                vault_path=vault,
                auto_git_enabled=False,
            ),
        )

        with self.assertRaises((OSError, ValueError)):
            await coordinator._acquire_named_lock("target:symlink")

        self.assertEqual(list(real_root.glob("*.lock")), [])

    @unittest.skipIf(os.name == "nt", "POSIX permission semantics")
    async def test_lock_root_rejects_world_writable_existing_state(self):
        vault = Path(self.temporary_directory.name) / "permission-vault"
        lock_root = (
            vault
            / "30_TradingPlaybook"
            / "Daily"
            / "Auto"
            / ".sync-locks"
        )
        lock_root.mkdir(parents=True)
        lock_root.chmod(0o777)
        coordinator = self._coordinator(
            self.session_factory,
            exporter=FakeExporter(),
            writer=ObsidianVaultWriter(
                enabled=True,
                vault_path=vault,
                auto_git_enabled=False,
            ),
        )

        with self.assertRaises(PermissionError):
            await coordinator._acquire_named_lock("target:world-directory")

        lock_root.chmod(0o770)
        lock_path = vault / coordinator._lock_relative_path("target:world-file")
        lock_path.write_bytes(b"\0")
        lock_path.chmod(0o666)
        with self.assertRaises(PermissionError):
            await coordinator._acquire_named_lock("target:world-file")

    async def test_lock_identity_is_case_normalized_inside_the_vault(self):
        coordinator = self._coordinator(
            self.session_factory,
            exporter=FakeExporter(),
            writer=FakeWriter(self.temporary_directory.name),
        )

        upper = coordinator._lock_relative_path(
            "TARGET:30_TradingPlaybook/Alerts/Auto/2026/CASE.md"
        )
        lower = coordinator._lock_relative_path(
            "target:30_tradingplaybook/alerts/auto/2026/case.md"
        )

        self.assertEqual(upper, lower)
        self.assertTrue(
            upper.startswith(
                "30_TradingPlaybook/Daily/Auto/.sync-locks/"
            )
        )
        self.assertTrue(upper.endswith(".lock"))

    async def test_mixed_due_batch_context_is_stable(self):
        earlier = artifact(
            "earlier-plan",
            trade_date=date(2026, 7, 15),
            snapshot_key="plan:88",
            immutable=True,
            entity_type="plan",
            entity_id=88,
            phase="preclose",
            target_path=(
                "30_TradingPlaybook/Daily/Auto/2026/2026-07-15/preclose-v1.md"
            ),
        )
        later = artifact(
            "later-alerts",
            trade_date=date(2026, 7, 17),
            snapshot_key="alerts:2026-07-17",
            entity_type="alerts",
            target_path="30_TradingPlaybook/Alerts/Auto/2026/2026-07-17.md",
        )
        await self.coordinator.enqueue_artifacts([later, earlier])
        coordinator = self._coordinator(
            self.session_factory,
            exporter=FakeExporter(),
            writer=FakeWriter(self.temporary_directory.name),
        )

        result = await coordinator.process_due()

        self.assertEqual(result.trade_date, date(2026, 7, 17))
        self.assertEqual(result.phase, "reconcile")

    async def test_failures_back_off_1_5_15_15_minutes_and_truncate_errors(self):
        item = artifact("retry", snapshot_key="alerts:retry")
        await self.coordinator.enqueue_artifacts([item])
        writer = FakeWriter(self.temporary_directory.name)
        writer.write_error = RuntimeError("x" * 5000)
        coordinator = self._coordinator(
            self.session_factory,
            exporter=FakeExporter(),
            writer=writer,
        )
        expected_delays = (1, 5, 15, 15)

        for attempt, delay in enumerate(expected_delays, start=1):
            before = self.clock.value
            result = await coordinator.process_due()
            stored = (await self._rows(item.snapshot_key))[0]
            self.assertEqual(result.failed_files, (item.target_path,))
            self.assertEqual(stored.status, "failed")
            self.assertEqual(stored.attempt_no, attempt)
            self.assertEqual(len(stored.last_error), 2000)
            self.assertEqual(
                stored.next_attempt_at,
                (before + timedelta(minutes=delay))
                .astimezone(timezone(timedelta(hours=8)))
                .replace(tzinfo=None),
            )
            self.clock.value += timedelta(minutes=delay)

    async def test_failure_text_is_control_safe(self):
        item = artifact("unsafe-error", snapshot_key="alerts:unsafe-error")
        await self.coordinator.enqueue_artifacts([item])
        writer = FakeWriter(self.temporary_directory.name)
        writer.write_error = RuntimeError("line1\nline2\t\x00secret")
        coordinator = self._coordinator(
            self.session_factory,
            exporter=FakeExporter(),
            writer=writer,
        )

        await coordinator.process_due()

        stored = (await self._rows(item.snapshot_key))[0]
        self.assertEqual(stored.last_error, "line1 line2 �secret")

    async def test_write_completion_is_fenced_when_lease_expires_during_io(self):
        item = artifact("lease-expiry", snapshot_key="alerts:lease-expiry")
        row = (await self.coordinator.enqueue_artifacts([item]))[0]
        writer = FakeWriter(self.temporary_directory.name)
        writer.after_write = lambda: setattr(
            self.clock,
            "value",
            self.clock.value + timedelta(seconds=61),
        )
        coordinator = self._coordinator(
            self.session_factory,
            exporter=FakeExporter(),
            writer=writer,
        )

        result = await coordinator.process_due()

        stored = (await self._rows(item.snapshot_key))[0]
        self.assertEqual(stored.id, row.id)
        self.assertEqual(stored.status, "pending")
        self.assertEqual(stored.attempt_no, 0)
        self.assertEqual(result.pending_files, (item.target_path,))
        self.assertEqual(result.written_files, ())

    async def test_target_lock_failure_marks_only_its_live_row(self):
        broken = artifact(
            "broken-lock",
            snapshot_key="alerts:lock-fail",
            target_path="30_TradingPlaybook/Alerts/Auto/2026/lock-fail.md",
        )
        healthy = artifact(
            "healthy-lock",
            snapshot_key="alerts:lock-healthy",
            target_path="30_TradingPlaybook/Alerts/Auto/2026/lock-healthy.md",
        )
        await self.coordinator.enqueue_artifacts([broken, healthy])

        class OneBrokenLockCoordinator(TradingPlaybookObsidianSyncCoordinator):
            async def _acquire_target_lock(inner_self, target_path):
                if target_path == broken.target_path:
                    raise OSError("lock unavailable")
                return await super()._acquire_target_lock(target_path)

        coordinator = self._coordinator(
            self.session_factory,
            exporter=FakeExporter(),
            writer=FakeWriter(self.temporary_directory.name),
            coordinator_class=OneBrokenLockCoordinator,
        )

        result = await coordinator.process_due()

        self.assertEqual(result.failed_files, (broken.target_path,))
        self.assertEqual(result.written_files, (healthy.target_path,))
        rows = {row.snapshot_key: row for row in await self._rows()}
        self.assertEqual(rows[broken.snapshot_key].status, "failed")
        self.assertEqual(rows[healthy.snapshot_key].status, "written")

    async def test_superseded_lease_is_rechecked_before_file_io(self):
        old = artifact("old", snapshot_key="alerts:lease-race")
        new = artifact("new", snapshot_key=old.snapshot_key)
        await self.coordinator.enqueue_artifacts([old])
        superseder = self.coordinator

        class SupersedingCoordinator(TradingPlaybookObsidianSyncCoordinator):
            checked = False

            async def _lease_is_active(inner_self, row_id, lease_until):
                if not inner_self.checked:
                    inner_self.checked = True
                    await superseder.enqueue_artifacts([new])
                return await super()._lease_is_active(row_id, lease_until)

        writer = FakeWriter(self.temporary_directory.name)
        coordinator = self._coordinator(
            self.session_factory,
            exporter=FakeExporter(),
            writer=writer,
            coordinator_class=SupersedingCoordinator,
        )

        result = await coordinator.process_due()

        self.assertEqual(writer.write_calls, [])
        self.assertEqual(result.pending_files, (old.target_path,))
        rows = await self._rows(old.snapshot_key)
        self.assertEqual([row.status for row in rows], ["superseded", "pending"])

    async def test_legacy_written_null_git_status_is_recovered_without_rewrite(self):
        item = artifact("legacy-git", snapshot_key="alerts:legacy-git")
        row = (await self.coordinator.enqueue_artifacts([item]))[0]
        async with self.session_factory() as session:
            await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(TradingPlaybookObsidianExport.id == row.id)
                .values(status="written", git_status_json=None)
            )
            await session.commit()
        writer = FakeWriter(self.temporary_directory.name)
        coordinator = self._coordinator(
            self.session_factory,
            exporter=FakeExporter(),
            writer=writer,
        )

        result = await coordinator.process_due()

        self.assertEqual(writer.write_calls, [])
        self.assertEqual(writer.commit_calls[0][0], (item.target_path,))
        self.assertEqual(result.git_status_json()["state"], "git_complete")
        stored = (await self._rows(item.snapshot_key))[0]
        self.assertEqual(stored.git_status_json["state"], "git_complete")

    async def test_git_failure_does_not_rollback_files_and_retries_without_rewrite(self):
        first = artifact("git-one", snapshot_key="alerts:git-one")
        second = artifact(
            "git-two",
            snapshot_key="alerts:git-two",
            target_path="30_TradingPlaybook/Alerts/Auto/2026/git-two.md",
        )
        await self.coordinator.enqueue_artifacts([first, second])
        writer = FakeWriter(self.temporary_directory.name)
        writer.commit_result = {
            "enabled": True,
            "committed": False,
            "error": "git unavailable",
        }
        coordinator = self._coordinator(
            self.session_factory,
            exporter=FakeExporter(),
            writer=writer,
        )

        failed_git = await coordinator.process_due()

        self.assertEqual(len(writer.commit_calls), 1)
        self.assertEqual(
            writer.commit_calls[0][0],
            (first.target_path, second.target_path),
        )
        rows = await self._rows()
        self.assertEqual([row.status for row in rows], ["written", "written"])
        self.assertEqual(failed_git.git_status_json()["state"], "git_error")
        self.assertTrue(
            all(
                row.git_status_json == failed_git.git_status_json()
                for row in rows
            )
        )
        write_count = len(writer.write_calls)

        writer.commit_result = {"enabled": True, "committed": True}
        retried = await coordinator.process_due()

        self.assertEqual(len(writer.write_calls), write_count)
        self.assertEqual(len(writer.commit_calls), 2)
        self.assertEqual(retried.written_files, ())
        self.assertEqual(retried.git_status_json()["state"], "git_complete")
        rows = await self._rows()
        self.assertTrue(
            all(row.git_status_json["state"] == "git_complete" for row in rows)
        )

    async def test_lost_mark_written_recovers_physical_change_and_git_intent(self):
        item = artifact("lost-mark", snapshot_key="alerts:lost-mark")
        await self.coordinator.enqueue_artifacts([item])
        writer = FakeWriter(self.temporary_directory.name)

        class LoseFirstCompletionCoordinator(
            TradingPlaybookObsidianSyncCoordinator
        ):
            lose_once = True

            async def _mark_written(inner_self, *args, **kwargs):
                if inner_self.lose_once:
                    inner_self.lose_once = False
                    return False
                return await super()._mark_written(*args, **kwargs)

        coordinator = self._coordinator(
            self.session_factory,
            exporter=FakeExporter(),
            writer=writer,
            coordinator_class=LoseFirstCompletionCoordinator,
        )

        first = await coordinator.process_due()

        self.assertEqual(first.pending_files, (item.target_path,))
        stored = (await self._rows(item.snapshot_key))[0]
        self.assertEqual(stored.status, "pending")
        self.assertEqual(stored.git_status_json["state"], "write_in_progress")
        self.assertEqual(writer.commit_calls, [])

        self.clock.value += timedelta(seconds=61)
        writer.changed = False
        second = await coordinator.process_due()

        self.assertEqual(second.skipped_files, (item.target_path,))
        self.assertEqual(len(writer.commit_calls), 1)
        self.assertEqual(writer.commit_calls[0][0], (item.target_path,))
        stored = (await self._rows(item.snapshot_key))[0]
        self.assertEqual(stored.status, "written")
        self.assertEqual(stored.git_status_json["state"], "git_complete")

    async def test_git_cancellation_leaves_pending_intent_for_git_only_retry(self):
        item = artifact("git-cancel", snapshot_key="alerts:git-cancel")
        await self.coordinator.enqueue_artifacts([item])
        writer = FakeWriter(self.temporary_directory.name)
        writer.commit_result = asyncio.CancelledError()
        coordinator = self._coordinator(
            self.session_factory,
            exporter=FakeExporter(),
            writer=writer,
        )

        with self.assertRaises(asyncio.CancelledError):
            await coordinator.process_due()

        stored = (await self._rows(item.snapshot_key))[0]
        self.assertEqual(stored.status, "written")
        self.assertEqual(stored.git_status_json["state"], "git_pending")
        write_count = len(writer.write_calls)

        writer.commit_result = {"enabled": True, "committed": True}
        retried = await coordinator.process_due()

        self.assertEqual(len(writer.write_calls), write_count)
        self.assertEqual(retried.git_status_json()["state"], "git_complete")
        stored = (await self._rows(item.snapshot_key))[0]
        self.assertEqual(stored.git_status_json["state"], "git_complete")

    async def test_repeated_git_cancellation_waits_for_commit_and_keeps_lock(self):
        item = artifact("git-double-cancel", snapshot_key="alerts:git-double-cancel")
        await self.coordinator.enqueue_artifacts([item])
        writer = FakeWriter(self.temporary_directory.name)
        entered = threading.Event()
        release = threading.Event()

        def blocking_commit(relative_paths, *, allowed_roots, message):
            entered.set()
            if not release.wait(timeout=5):
                raise RuntimeError("double cancel Git release timed out")
            return {"enabled": True, "committed": True}

        writer.commit_paths = blocking_commit
        coordinator = self._coordinator(
            self.session_factory,
            exporter=FakeExporter(),
            writer=writer,
        )
        task = asyncio.create_task(coordinator.process_due())
        self.assertTrue(await asyncio.to_thread(entered.wait, 5))

        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        contender = asyncio.create_task(
            coordinator._acquire_named_lock("git:vault")
        )
        await asyncio.sleep(0.05)
        task_finished_early = task.done()
        lock_released_early = contender.done()
        release.set()

        with self.assertRaises(asyncio.CancelledError):
            await task
        contender_lock = await asyncio.wait_for(contender, timeout=2)
        await coordinator._to_thread_fenced(contender_lock.release)
        self.assertFalse(task_finished_early)
        self.assertFalse(lock_released_early)

    async def test_git_status_store_failure_keeps_pending_intent_for_retry(self):
        item = artifact("git-store", snapshot_key="alerts:git-store")
        await self.coordinator.enqueue_artifacts([item])
        writer = FakeWriter(self.temporary_directory.name)

        class StoreFailureCoordinator(TradingPlaybookObsidianSyncCoordinator):
            fail_once = True

            async def _store_git_status(inner_self, *args, **kwargs):
                if inner_self.fail_once:
                    inner_self.fail_once = False
                    raise RuntimeError("status store unavailable")
                return await super()._store_git_status(*args, **kwargs)

        coordinator = self._coordinator(
            self.session_factory,
            exporter=FakeExporter(),
            writer=writer,
            coordinator_class=StoreFailureCoordinator,
        )

        first = await coordinator.process_due()

        stored = (await self._rows(item.snapshot_key))[0]
        self.assertEqual(first.git_status_json()["state"], "git_store_pending")
        self.assertEqual(stored.status, "written")
        self.assertEqual(stored.git_status_json["state"], "git_pending")
        write_count = len(writer.write_calls)

        await coordinator.process_due()

        self.assertEqual(len(writer.write_calls), write_count)
        self.assertEqual(len(writer.commit_calls), 2)
        stored = (await self._rows(item.snapshot_key))[0]
        self.assertEqual(stored.git_status_json["state"], "git_complete")

    async def test_git_retry_uses_shared_lock_and_rechecks_after_acquiring_it(self):
        first = artifact("git-lock-a", snapshot_key="alerts:git-lock-a")
        second = artifact(
            "git-lock-b",
            snapshot_key="alerts:git-lock-b",
            target_path="30_TradingPlaybook/Alerts/Auto/2026/git-lock-b.md",
        )
        rows = await self.coordinator.enqueue_artifacts([first, second])
        async with self.session_factory() as session:
            await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(
                    TradingPlaybookObsidianExport.id.in_(
                        [int(row.id) for row in rows]
                    )
                )
                .values(
                    status="written",
                    git_status_json={
                        "state": "git_error",
                        "error": "retry",
                    },
                )
            )
            await session.commit()
        writer = FakeWriter(self.temporary_directory.name)
        first_entered = threading.Event()
        second_entered = threading.Event()
        release = threading.Event()
        counter_lock = threading.Lock()
        active = 0
        maximum_active = 0
        call_count = 0

        def blocking_commit(relative_paths, *, allowed_roots, message):
            nonlocal active, maximum_active, call_count
            with counter_lock:
                call_count += 1
                call_number = call_count
                active += 1
                maximum_active = max(maximum_active, active)
            try:
                if call_number == 1:
                    first_entered.set()
                    if not release.wait(timeout=5):
                        raise RuntimeError("Git lock release timed out")
                else:
                    second_entered.set()
                return {"enabled": True, "committed": True}
            finally:
                with counter_lock:
                    active -= 1

        writer.commit_paths = blocking_commit
        coordinators = [
            self._coordinator(
                self.session_factory,
                exporter=FakeExporter(),
                writer=writer,
            )
            for _ in range(2)
        ]
        tasks = [
            asyncio.create_task(coordinator.process_due())
            for coordinator in coordinators
        ]
        await asyncio.to_thread(first_entered.wait, 5)
        second_was_concurrent = await asyncio.to_thread(
            second_entered.wait,
            1,
        )
        release.set()
        await asyncio.gather(*tasks)

        self.assertFalse(second_was_concurrent)
        self.assertEqual(maximum_active, 1)
        self.assertEqual(call_count, 1)
        stored = await self._rows()
        self.assertTrue(
            all(row.git_status_json["state"] == "git_complete" for row in stored)
        )

    async def test_git_error_status_is_recursively_bounded_and_control_safe(self):
        item = artifact("git-bounds", snapshot_key="alerts:git-bounds")
        await self.coordinator.enqueue_artifacts([item])
        writer = FakeWriter(self.temporary_directory.name)
        writer.commit_result = {
            "enabled": True,
            "committed": False,
            "error": "bad\n\t\x00\x7f\x85\ud800" + ("x" * 10_000),
            "stderr": {
                f"branch-{outer}": {
                    f"leaf-{inner}": ["y" * 2_000 for _ in range(20)]
                    for inner in range(20)
                }
                for outer in range(20)
            },
        }
        coordinator = self._coordinator(
            self.session_factory,
            exporter=FakeExporter(),
            writer=writer,
        )

        result = await coordinator.process_due()

        status = result.git_status_json()
        encoded = json.dumps(status, ensure_ascii=False).encode("utf-8")
        self.assertEqual(status["state"], "git_error")
        self.assertLessEqual(len(status["error"]), 2000)
        self.assertNotIn("\n", status["error"])
        self.assertNotIn("\t", status["error"])
        self.assertNotIn("\x00", status["error"])
        self.assertNotIn("\x7f", status["error"])
        self.assertNotIn("\x85", status["error"])
        self.assertNotIn("\ud800", status["error"])
        self.assertLessEqual(len(encoded), 32 * 1024)
        self.assertTrue(coordinator._git_needs_retry(status))
        stored = (await self._rows(item.snapshot_key))[0]
        self.assertEqual(stored.git_status_json, status)

    async def test_corrupt_snapshot_envelope_or_hash_fails_before_file_io(self):
        cases = (
            {"payload": artifact("bad").payload_json()},
            {"payload": [], "generated_at": "2026-07-16T06:40:00Z"},
            {"payload": artifact("bad").payload_json(), "generated_at": "naive"},
        )
        for index, snapshot_json in enumerate(cases):
            with self.subTest(index=index):
                item = artifact(
                    f"bad-{index}",
                    snapshot_key=f"alerts:bad:{index}",
                    target_path=f"30_TradingPlaybook/Alerts/Auto/2026/bad-{index}.md",
                )
                row = (await self.coordinator.enqueue_artifacts([item]))[0]
                async with self.session_factory() as session:
                    values = {"snapshot_json": snapshot_json}
                    if index == 2:
                        values["source_hash"] = "0" * 64
                    await session.execute(
                        update(TradingPlaybookObsidianExport)
                        .where(TradingPlaybookObsidianExport.id == row.id)
                        .values(**values)
                    )
                    await session.commit()
                writer = FakeWriter(self.temporary_directory.name)
                coordinator = self._coordinator(
                    self.session_factory,
                    exporter=FakeExporter(),
                    writer=writer,
                )

                result = await coordinator.process_due()

                self.assertEqual(writer.write_calls, [])
                self.assertEqual(result.failed_files, (item.target_path,))

    async def test_snapshot_hash_mismatch_fails_before_file_io(self):
        item = artifact("hash-mismatch", snapshot_key="alerts:hash-mismatch")
        row = (await self.coordinator.enqueue_artifacts([item]))[0]
        async with self.session_factory() as session:
            await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(TradingPlaybookObsidianExport.id == row.id)
                .values(source_hash="0" * 64)
            )
            await session.commit()
        writer = FakeWriter(self.temporary_directory.name)
        coordinator = self._coordinator(
            self.session_factory,
            exporter=FakeExporter(),
            writer=writer,
        )

        result = await coordinator.process_due()

        self.assertEqual(writer.write_calls, [])
        self.assertEqual(result.failed_files, (item.target_path,))

    async def test_exporter_rejects_snapshot_metadata_mismatch_before_file_io(self):
        item = ObsidianArtifact(
            snapshot_key="alerts:2026-07-16",
            trade_date=TRADE_DATE,
            entity_type="alerts",
            entity_id=None,
            phase="reconcile",
            target_path="30_TradingPlaybook/Alerts/Auto/2026/2026-07-16.md",
            immutable=False,
            payload={
                "type": "trading_alert_timeline",
                "trade_date": TRADE_DATE,
                "timeline": [],
                "manual_required": True,
                "auto_execute": False,
            },
        )
        row = (await self.coordinator.enqueue_artifacts([item]))[0]
        async with self.session_factory() as session:
            await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(TradingPlaybookObsidianExport.id == row.id)
                .values(
                    target_path=(
                        "30_TradingPlaybook/Alerts/Auto/2026/2026-07-17.md"
                    )
                )
            )
            await session.commit()
        writer = FakeWriter(self.temporary_directory.name)
        coordinator = self._coordinator(
            self.session_factory,
            exporter=TradingPlaybookObsidianExporter(),
            writer=writer,
        )

        result = await coordinator.process_due()

        self.assertEqual(writer.write_calls, [])
        self.assertEqual(
            result.failed_files,
            ("30_TradingPlaybook/Alerts/Auto/2026/2026-07-17.md",),
        )

    async def test_blocked_writer_does_not_hold_a_sqlite_business_write_lock(self):
        item = artifact("slow", snapshot_key="alerts:slow")
        await self.coordinator.enqueue_artifacts([item])
        writer = FakeWriter(self.temporary_directory.name)
        entered = threading.Event()
        release = threading.Event()
        original_write = writer.write_text

        def blocking_write(*args, **kwargs):
            entered.set()
            if not release.wait(timeout=5):
                raise RuntimeError("test writer release timed out")
            return original_write(*args, **kwargs)

        writer.write_text = blocking_write
        coordinator = self._coordinator(
            self.session_factory,
            exporter=FakeExporter(),
            writer=writer,
        )
        worker = asyncio.create_task(coordinator.process_due())
        await asyncio.to_thread(entered.wait, 5)
        try:
            async def write_business_row():
                async with self.session_factory() as session:
                    session.add(
                        plan_row(
                            source_trade_date=date(2026, 7, 20),
                            target_trade_date=date(2026, 7, 21),
                        )
                    )
                    await session.commit()

            await asyncio.wait_for(write_business_row(), timeout=1)
        finally:
            release.set()
        await worker

    async def test_target_lock_prevents_expired_old_worker_from_writing_last(self):
        old = artifact("old", snapshot_key="alerts:target-fence")
        new = artifact("new", snapshot_key=old.snapshot_key)
        await self.coordinator.enqueue_artifacts([old])
        entered_old = threading.Event()
        release_old = threading.Event()
        writes = []
        writer = FakeWriter(self.temporary_directory.name)

        def ordered_write(relative_path, content, *, allowed_roots):
            call_no = len(writes)
            if call_no == 0:
                entered_old.set()
                if not release_old.wait(timeout=5):
                    raise RuntimeError("old writer release timed out")
            writes.append(content)
            return VaultWriteResult(
                relative_path=relative_path,
                absolute_path=Path(writer.vault_path) / relative_path,
                changed=True,
            )

        writer.write_text = ordered_write
        first = self._coordinator(
            self.session_factory,
            exporter=FakeExporter(),
            writer=writer,
        )
        waiting_for_lock = asyncio.Event()

        class WaitingCoordinator(TradingPlaybookObsidianSyncCoordinator):
            async def _acquire_target_lock(inner_self, target_path):
                waiting_for_lock.set()
                return await super()._acquire_target_lock(target_path)

        second = self._coordinator(
            self.session_factory,
            exporter=FakeExporter(),
            writer=writer,
            coordinator_class=WaitingCoordinator,
        )

        old_task = asyncio.create_task(first.process_due())
        await asyncio.to_thread(entered_old.wait, 5)
        self.clock.value += timedelta(seconds=61)
        await self.coordinator.enqueue_artifacts([new])
        new_task = asyncio.create_task(second.process_due())
        await asyncio.wait_for(waiting_for_lock.wait(), timeout=2)
        self.assertFalse(new_task.done())
        release_old.set()

        old_result, new_result = await asyncio.gather(old_task, new_task)

        self.assertEqual(old_result.pending_files, (old.target_path,))
        self.assertEqual(new_result.written_files, (new.target_path,))
        self.assertEqual(len(writes), 2)
        self.assertIn(old.source_hash, writes[0])
        self.assertIn(new.source_hash, writes[1])
        rows = await self._rows(old.snapshot_key)
        self.assertEqual([row.status for row in rows], ["superseded", "written"])

    async def test_export_trade_date_rebuilds_relevant_immutable_and_mutable_facts(self):
        async with self.session_factory() as session:
            plan = plan_row()
            session.add(plan)
            await session.flush()
            review = review_row(plan.id)
            session.add(review)
            await session.commit()
            plan_id = plan.id
            review_id = review.id

        rule = artifact(
            "rule",
            snapshot_key="rule:v2:mode_test",
            immutable=True,
            entity_type="rule",
            phase="catalog",
            target_path="30_TradingPlaybook/Modes/Auto/v2/mode_test.md",
        )
        plan_artifact = artifact(
            "plan",
            snapshot_key=f"plan:{plan_id}",
            immutable=True,
            entity_type="plan",
            entity_id=plan_id,
            phase="preclose",
            target_path=(
                "30_TradingPlaybook/Daily/Auto/2026/2026-07-16/preclose-v1.md"
            ),
        )
        review_artifact = artifact(
            "review",
            snapshot_key=f"review:{review_id}:initial",
            immutable=True,
            entity_type="review",
            entity_id=review_id,
            phase="initial_review",
            target_path=(
                "30_TradingPlaybook/Reviews/Auto/2026/2026-07-16/"
                f"initial-review-{plan_id}.md"
            ),
        )
        alerts = artifact(
            "alerts",
            snapshot_key="alerts:2026-07-16",
            entity_type="alerts",
            target_path=(
                "30_TradingPlaybook/Alerts/Auto/2026/2026-07-16.md"
            ),
        )
        index = artifact("index")
        dashboard = artifact(
            "dashboard",
            snapshot_key="dashboard:trading-playbook",
            entity_type="dashboard",
            target_path="Dashboards/交易预案.md",
        )
        builder = FakeBuilder(
            rules=(rule,),
            by_date={
                ("plan", plan_id): plan_artifact,
                ("review", review_id, "initial_review"): review_artifact,
                ("alerts", TRADE_DATE): alerts,
                ("index", TRADE_DATE): index,
                ("dashboard", TRADE_DATE): dashboard,
            },
        )
        coordinator = self._coordinator(
            self.session_factory,
            builder=builder,
            exporter=FakeExporter(),
            writer=FakeWriter(self.temporary_directory.name),
        )

        result = await coordinator.export_trade_date(
            TRADE_DATE,
            include_rules=True,
            force=False,
        )

        self.assertEqual(
            builder.calls,
            [
                ("rules", "v2"),
                ("plan", plan_id),
                ("review", review_id, "initial_review"),
                ("alerts", TRADE_DATE),
                ("index", TRADE_DATE),
                ("dashboard", TRADE_DATE),
            ],
        )
        self.assertEqual(len(result.written_files), 6)
        self.assertEqual(len(await self._rows()), 6)

    async def test_export_trade_date_processes_only_its_selected_rows_under_backlog(self):
        backlog = [
            artifact(
                f"backlog-{number}",
                trade_date=date(2026, 7, 1),
                snapshot_key=f"alerts:backlog:{number:03d}",
                entity_type="alerts",
                target_path=(
                    "30_TradingPlaybook/Alerts/Auto/2026/"
                    f"backlog-{number:03d}.md"
                ),
            )
            for number in range(100)
        ]
        await self.coordinator.enqueue_artifacts(backlog)
        selected = {
            "alerts": artifact(
                "selected-alerts",
                snapshot_key="alerts:2026-07-16",
                entity_type="alerts",
                target_path=(
                    "30_TradingPlaybook/Alerts/Auto/2026/2026-07-16.md"
                ),
            ),
            "index": artifact("selected-index"),
            "dashboard": artifact(
                "selected-dashboard",
                snapshot_key="dashboard:trading-playbook",
                entity_type="dashboard",
                target_path="Dashboards/交易预案.md",
            ),
        }
        builder = FakeBuilder(
            by_date={
                ("alerts", TRADE_DATE): selected["alerts"],
                ("index", TRADE_DATE): selected["index"],
                ("dashboard", TRADE_DATE): selected["dashboard"],
            }
        )
        writer = FakeWriter(self.temporary_directory.name)
        coordinator = self._coordinator(
            self.session_factory,
            builder=builder,
            exporter=FakeExporter(),
            writer=writer,
        )

        result = await coordinator.export_trade_date(
            TRADE_DATE,
            include_rules=False,
            force=False,
        )

        expected_paths = {item.target_path for item in selected.values()}
        self.assertEqual(set(result.written_files), expected_paths)
        self.assertEqual(result.skipped_files, ())
        self.assertEqual(result.pending_files, ())
        self.assertEqual(result.failed_files, ())
        self.assertEqual(
            {call[0] for call in writer.write_calls},
            expected_paths,
        )
        stored = await self._rows()
        self.assertTrue(
            all(
                row.status == "pending"
                for row in stored
                if row.snapshot_key.startswith("alerts:backlog:")
            )
        )

    async def test_force_uses_actual_artifact_rows_and_preserves_active_lease(self):
        target_date = date(2026, 7, 17)
        async with self.session_factory() as session:
            plan = plan_row(target_trade_date=target_date)
            session.add(plan)
            await session.commit()
            plan_id = int(plan.id)
        plan_artifact = artifact(
            "force-plan",
            trade_date=target_date,
            snapshot_key=f"plan:{plan_id}",
            immutable=True,
            entity_type="plan",
            entity_id=plan_id,
            phase="preclose",
            target_path=(
                "30_TradingPlaybook/Daily/Auto/2026/2026-07-17/"
                "preclose-v1.md"
            ),
        )
        alerts = artifact(
            "force-active-alerts",
            snapshot_key="alerts:2026-07-16",
            entity_type="alerts",
            target_path="30_TradingPlaybook/Alerts/Auto/2026/2026-07-16.md",
        )
        index = artifact("force-index")
        dashboard = artifact(
            "force-dashboard",
            snapshot_key="dashboard:trading-playbook",
            entity_type="dashboard",
            target_path="Dashboards/交易预案.md",
        )
        rows = await self.coordinator.enqueue_artifacts(
            [plan_artifact, alerts, index, dashboard]
        )
        by_key = {row.snapshot_key: row for row in rows}
        live_until = datetime(2026, 7, 16, 14, 41)
        live_token = self.coordinator._canonical_datetime(
            FIXED_NOW + timedelta(minutes=1)
        )
        async with self.session_factory() as session:
            await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(
                    TradingPlaybookObsidianExport.id
                    == by_key[plan_artifact.snapshot_key].id
                )
                .values(
                    status="failed",
                    attempt_no=3,
                    next_attempt_at=datetime(2030, 1, 1),
                    last_error="backoff",
                    git_status_json={"state": "git_error", "error": "old"},
                )
            )
            await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(
                    TradingPlaybookObsidianExport.id
                    == by_key[alerts.snapshot_key].id
                )
                .values(
                    status="pending",
                    next_attempt_at=live_until,
                    git_status_json={
                        "state": "write_in_progress",
                        "lease_token": live_token,
                    },
                )
            )
            await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(
                    TradingPlaybookObsidianExport.id
                    == by_key[dashboard.snapshot_key].id
                )
                .values(status="written", git_status_json=None)
            )
            await session.commit()
        builder = FakeBuilder(
            by_date={
                ("plan", plan_id): plan_artifact,
                ("alerts", TRADE_DATE): alerts,
                ("index", TRADE_DATE): index,
                ("dashboard", TRADE_DATE): dashboard,
            }
        )
        writer = FakeWriter(self.temporary_directory.name)
        coordinator = self._coordinator(
            self.session_factory,
            builder=builder,
            exporter=FakeExporter(),
            writer=writer,
        )

        result = await coordinator.export_trade_date(
            TRADE_DATE,
            include_rules=False,
            force=True,
        )

        self.assertEqual(
            set(result.written_files),
            {plan_artifact.target_path, index.target_path},
        )
        self.assertEqual(result.pending_files, (alerts.target_path,))
        self.assertEqual(len(writer.commit_calls), 1)
        self.assertEqual(
            set(writer.commit_calls[0][0]),
            {
                plan_artifact.target_path,
                index.target_path,
                dashboard.target_path,
            },
        )
        stored = {row.snapshot_key: row for row in await self._rows()}
        self.assertEqual(stored[plan_artifact.snapshot_key].status, "written")
        self.assertEqual(stored[alerts.snapshot_key].status, "pending")
        self.assertEqual(stored[alerts.snapshot_key].next_attempt_at, live_until)
        self.assertEqual(
            stored[alerts.snapshot_key].git_status_json["state"],
            "write_in_progress",
        )
        self.assertEqual(stored[dashboard.snapshot_key].status, "written")
        self.assertEqual(
            stored[dashboard.snapshot_key].git_status_json["state"],
            "git_complete",
        )

    async def test_force_resets_failed_backoff_after_a_real_write_failure(self):
        item = artifact(
            "force-after-write-failure",
            snapshot_key="alerts:force-after-write-failure",
        )
        row = (await self.coordinator.enqueue_artifacts([item]))[0]
        writer = FakeWriter(self.temporary_directory.name)
        writer.write_error = RuntimeError("disk unavailable")
        coordinator = self._coordinator(
            self.session_factory,
            exporter=FakeExporter(),
            writer=writer,
        )

        failed = await coordinator.process_due()

        self.assertEqual(failed.failed_files, (item.target_path,))
        stored = (await self._rows(item.snapshot_key))[0]
        self.assertEqual(stored.status, "failed")
        self.assertGreater(stored.next_attempt_at, datetime(2026, 7, 16, 14, 40))
        self.assertEqual(stored.git_status_json["state"], "write_failed")

        await coordinator._reset_forced_exports([int(row.id)])

        reset = (await self._rows(item.snapshot_key))[0]
        self.assertEqual(reset.status, "pending")
        self.assertEqual(reset.attempt_no, 0)
        self.assertIsNone(reset.next_attempt_at)
        self.assertIsNone(reset.last_error)
        self.assertEqual(reset.git_status_json["state"], "git_pending")

    async def test_force_preserves_failed_row_claimed_before_lease_renewal(self):
        item = artifact(
            "force-claimed-failed",
            snapshot_key="alerts:force-claimed-failed",
        )
        row = (await self.coordinator.enqueue_artifacts([item]))[0]
        async with self.session_factory() as session:
            await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(TradingPlaybookObsidianExport.id == row.id)
                .values(
                    status="failed",
                    attempt_no=1,
                    next_attempt_at=None,
                    last_error="old failure",
                    git_status_json={
                        "state": "write_failed",
                        "error": "old failure",
                    },
                )
            )
            await session.commit()

        claimed = await self.coordinator._claim_due(limit=1)

        self.assertEqual([int(item.id) for item in claimed], [int(row.id)])
        claimed_row = claimed[0]
        expected_token = self.coordinator._canonical_datetime(
            FIXED_NOW + timedelta(minutes=1)
        )
        self.assertEqual(claimed_row.status, "failed")
        self.assertEqual(claimed_row.git_status_json["state"], "lease_claimed")
        self.assertEqual(
            claimed_row.git_status_json["lease_token"],
            expected_token,
        )
        self.assertEqual(
            claimed_row.git_status_json["previous"]["state"],
            "write_failed",
        )

        await self.coordinator._reset_forced_exports([int(row.id)])

        preserved = (await self._rows(item.snapshot_key))[0]
        self.assertEqual(preserved.status, "failed")
        self.assertEqual(preserved.attempt_no, 1)
        self.assertEqual(preserved.next_attempt_at, claimed_row.next_attempt_at)
        self.assertEqual(preserved.last_error, "old failure")
        self.assertEqual(preserved.git_status_json, claimed_row.git_status_json)

    async def test_force_reset_is_scoped_to_date_and_dashboard_export_state(self):
        requested_failed = artifact(
            "requested-failed",
            snapshot_key="alerts:force-requested",
            entity_type="alerts",
            target_path="30_TradingPlaybook/Alerts/Auto/2026/force-requested.md",
        )
        dashboard_paused = artifact(
            "dashboard-paused",
            snapshot_key="dashboard:trading-playbook",
            entity_type="dashboard",
            target_path="Dashboards/交易预案.md",
        )
        requested_written = artifact(
            "requested-written",
            snapshot_key="daily-index:force-requested",
            target_path=(
                "30_TradingPlaybook/Daily/Auto/2026/2026-07-16/force-index.md"
            ),
        )
        other_date = date(2026, 7, 18)
        unrelated_failed = artifact(
            "unrelated",
            trade_date=other_date,
            snapshot_key="alerts:force-unrelated",
            entity_type="alerts",
            target_path="30_TradingPlaybook/Alerts/Auto/2026/force-unrelated.md",
        )
        rows = await self.coordinator.enqueue_artifacts(
            [
                requested_failed,
                dashboard_paused,
                requested_written,
                unrelated_failed,
            ]
        )
        row_by_key = {row.snapshot_key: row for row in rows}
        async with self.session_factory() as session:
            for item, status in (
                (requested_failed, "failed"),
                (dashboard_paused, "paused"),
                (requested_written, "written"),
                (unrelated_failed, "failed"),
            ):
                await session.execute(
                    update(TradingPlaybookObsidianExport)
                    .where(
                        TradingPlaybookObsidianExport.id
                        == row_by_key[item.snapshot_key].id
                    )
                    .values(
                        status=status,
                        attempt_no=3,
                        next_attempt_at=datetime(2030, 1, 1),
                        last_error="old failure",
                        git_status_json={"error": "old git failure"},
                    )
                )
            await session.commit()
        coordinator = self._coordinator(
            self.session_factory,
            writer=FakeWriter(self.temporary_directory.name),
        )

        await coordinator._reset_forced_exports(
            [
                row_by_key[requested_failed.snapshot_key].id,
                row_by_key[dashboard_paused.snapshot_key].id,
                row_by_key[requested_written.snapshot_key].id,
            ]
        )

        stored = {row.snapshot_key: row for row in await self._rows()}
        self.assertEqual(stored[requested_failed.snapshot_key].status, "pending")
        self.assertEqual(stored[requested_failed.snapshot_key].attempt_no, 0)
        self.assertIsNone(stored[requested_failed.snapshot_key].last_error)
        self.assertEqual(stored[dashboard_paused.snapshot_key].status, "pending")
        self.assertEqual(stored[requested_written.snapshot_key].status, "written")
        self.assertTrue(
            all(
                stored[item.snapshot_key].git_status_json["state"]
                == "git_pending"
                for item in (
                    requested_failed,
                    dashboard_paused,
                    requested_written,
                )
            )
        )
        self.assertEqual(stored[unrelated_failed.snapshot_key].status, "failed")
        self.assertEqual(stored[unrelated_failed.snapshot_key].attempt_no, 3)
        self.assertEqual(
            stored[unrelated_failed.snapshot_key].git_status_json,
            {"error": "old git failure"},
        )

    async def test_startup_reconcile_uses_only_latest_source_and_target_dates(self):
        source_latest = date(2026, 7, 15)
        target_latest = date(2026, 7, 17)
        async with self.session_factory() as session:
            older_plan = plan_row(
                source_trade_date=date(2026, 7, 14),
                target_trade_date=date(2026, 7, 16),
                version_no=1,
            )
            latest_plan = plan_row(
                source_trade_date=source_latest,
                target_trade_date=target_latest,
                version_no=2,
            )
            session.add_all([older_plan, latest_plan])
            await session.commit()
            plan_ids = (int(older_plan.id), int(latest_plan.id))
        await self.coordinator.enqueue_artifacts(
            [
                artifact(
                    f"existing-plan-{plan_id}",
                    snapshot_key=f"plan:{plan_id}",
                    immutable=True,
                    entity_type="plan",
                    entity_id=plan_id,
                    phase="preclose",
                    target_path=(
                        "30_TradingPlaybook/Daily/Auto/2026/2026-07-16/"
                        f"existing-plan-{plan_id}.md"
                    ),
                )
                for plan_id in plan_ids
            ]
        )
        immutable = artifact(
            "resume",
            snapshot_key="rule:v2:resume",
            immutable=True,
            entity_type="rule",
            phase="catalog",
            target_path="30_TradingPlaybook/Modes/Auto/v2/resume.md",
        )
        immutable_row = (await self.coordinator.enqueue_artifacts([immutable]))[0]
        async with self.session_factory() as session:
            await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(TradingPlaybookObsidianExport.id == immutable_row.id)
                .values(
                    status="failed",
                    attempt_no=2,
                    next_attempt_at=datetime(2026, 7, 16, 14, 40),
                )
            )
            await session.commit()

        by_date = {}
        for current in (source_latest, target_latest):
            year = current.year
            iso = current.isoformat()
            by_date[("alerts", current)] = artifact(
                f"alerts-{iso}",
                trade_date=current,
                snapshot_key=f"alerts:{iso}",
                entity_type="alerts",
                target_path=(
                    f"30_TradingPlaybook/Alerts/Auto/{year}/{iso}.md"
                ),
            )
            by_date[("index", current)] = artifact(
                f"index-{iso}",
                trade_date=current,
                snapshot_key=f"daily-index:{iso}",
                target_path=(
                    f"30_TradingPlaybook/Daily/Auto/{year}/{iso}/index.md"
                ),
            )
            by_date[("dashboard", current)] = artifact(
                f"dashboard-{iso}",
                trade_date=current,
                snapshot_key="dashboard:trading-playbook",
                entity_type="dashboard",
                target_path="Dashboards/交易预案.md",
            )
        builder = FakeBuilder(by_date=by_date)
        coordinator = self._coordinator(
            self.session_factory,
            builder=builder,
            exporter=FakeExporter(),
            writer=FakeWriter(self.temporary_directory.name),
        )

        await coordinator.startup_reconcile()

        self.assertEqual(
            builder.calls,
            [
                ("alerts", source_latest),
                ("index", source_latest),
                ("dashboard", source_latest),
                ("alerts", target_latest),
                ("index", target_latest),
                ("dashboard", target_latest),
            ],
        )
        stored_immutable = (await self._rows(immutable.snapshot_key))[0]
        self.assertEqual(stored_immutable.status, "written")

    async def test_reconcile_committed_facts_enqueues_only_missing_immutable_and_latest_mutable(self):
        async with self.session_factory() as session:
            exported_plan = plan_row(version_no=1)
            missing_plan = plan_row(version_no=2)
            session.add_all([exported_plan, missing_plan])
            await session.flush()
            initial_review = review_row(exported_plan.id, finalized=False)
            final_review = review_row(missing_plan.id, finalized=True)
            session.add_all([initial_review, final_review])
            await session.commit()
            exported_plan_id = int(exported_plan.id)
            missing_plan_id = int(missing_plan.id)
            initial_review_id = int(initial_review.id)
            final_review_id = int(final_review.id)
            session.add_all(
                [
                    TradingModeRule(
                        mode_key=f"mode_{index:02d}",
                        version=2,
                        name=f"Mode {index}",
                        family="test",
                        style="test",
                        window="test",
                        automation_level="manual",
                        content_hash=f"{index:064x}",
                        enabled=True,
                    )
                    for index in range(1, 20)
                ]
            )
            await session.commit()

        already_exported = artifact(
            "already-exported-plan",
            snapshot_key=f"plan:{exported_plan_id}",
            immutable=True,
            entity_type="plan",
            entity_id=exported_plan_id,
            phase="preclose",
            target_path=(
                "30_TradingPlaybook/Daily/Auto/2026/2026-07-16/"
                "preclose-v1.md"
            ),
        )
        await self.coordinator.enqueue_artifacts([already_exported])
        rules = tuple(
            artifact(
                f"rule-{index}",
                snapshot_key=f"rule:v2:mode_{index:02d}",
                immutable=True,
                entity_type="rule",
                entity_id=index,
                phase="catalog",
                target_path=(
                    "30_TradingPlaybook/Modes/Auto/v2/"
                    f"mode_{index:02d}.md"
                ),
            )
            for index in range(1, 20)
        )
        missing_plan_artifact = artifact(
            "missing-plan",
            snapshot_key=f"plan:{missing_plan_id}",
            immutable=True,
            entity_type="plan",
            entity_id=missing_plan_id,
            phase="preclose",
            target_path=(
                "30_TradingPlaybook/Daily/Auto/2026/2026-07-16/"
                "preclose-v2.md"
            ),
        )
        initial_artifact = artifact(
            "initial-review",
            snapshot_key=f"review:{initial_review_id}:initial",
            immutable=True,
            entity_type="review",
            entity_id=initial_review_id,
            phase="initial_review",
            target_path=(
                "30_TradingPlaybook/Reviews/Auto/2026/2026-07-16/"
                f"initial-review-{exported_plan_id}.md"
            ),
        )
        final_artifact = artifact(
            "final-review",
            snapshot_key=f"review:{final_review_id}:final",
            immutable=True,
            entity_type="review",
            entity_id=final_review_id,
            phase="final_review",
            target_path=(
                "30_TradingPlaybook/Reviews/Auto/2026/2026-07-16/"
                f"final-review-{missing_plan_id}.md"
            ),
        )
        alerts = artifact(
            "reconcile-alerts",
            snapshot_key=f"alerts:{TRADE_DATE.isoformat()}",
            entity_type="alerts",
            target_path=(
                "30_TradingPlaybook/Alerts/Auto/2026/2026-07-16.md"
            ),
        )
        index = artifact("reconcile-index")
        dashboard = artifact(
            "reconcile-dashboard",
            snapshot_key="dashboard:trading-playbook",
            entity_type="dashboard",
            target_path="Dashboards/交易预案.md",
        )
        builder = FakeBuilder(
            rules=rules,
            by_date={
                ("plan", missing_plan_id): missing_plan_artifact,
                (
                    "review",
                    initial_review_id,
                    "initial_review",
                ): initial_artifact,
                (
                    "review",
                    final_review_id,
                    "final_review",
                ): final_artifact,
                ("alerts", TRADE_DATE): alerts,
                ("index", TRADE_DATE): index,
                ("dashboard", TRADE_DATE): dashboard,
            },
        )
        coordinator = self._coordinator(
            self.session_factory,
            builder=builder,
        )

        rows = await coordinator.reconcile_committed_facts()

        self.assertEqual(len(rows), 25)
        self.assertEqual(
            builder.calls,
            [
                ("rules", "v2"),
                ("plan", missing_plan_id),
                ("review", initial_review_id, "initial_review"),
                ("review", final_review_id, "final_review"),
                ("alerts", TRADE_DATE),
                ("index", TRADE_DATE),
                ("dashboard", TRADE_DATE),
            ],
        )
        stored = await self._rows()
        self.assertEqual(len(stored), 26)

        builder.calls.clear()
        await coordinator.reconcile_committed_facts()

        self.assertEqual(
            builder.calls,
            [
                ("alerts", TRADE_DATE),
                ("index", TRADE_DATE),
                ("dashboard", TRADE_DATE),
            ],
        )

    async def test_reconcile_discovers_both_review_phase_snapshots_and_only_repairs_missing_phase(self):
        async with self.session_factory() as session:
            plan = plan_row(version_no=1)
            session.add(plan)
            await session.flush()
            review = review_row(plan.id, finalized=True)
            session.add(review)
            await session.flush()
            session.add_all(
                [
                    TradingExecutionReviewPhaseSnapshot(
                        review_id=review.id,
                        phase=phase,
                        trade_date=TRADE_DATE,
                        plan_version_id=plan.id,
                        snapshot_json={"phase": phase},
                        created_at=datetime(2026, 7, 16, 15, minute),
                    )
                    for phase, minute in (
                        ("initial_review", 10),
                        ("final_review", 30),
                    )
                ]
            )
            await session.commit()
            plan_id = int(plan.id)
            review_id = int(review.id)

        await self.coordinator.enqueue_artifacts(
            [
                artifact(
                    "existing-plan",
                    snapshot_key=f"plan:{plan_id}",
                    immutable=True,
                    entity_type="plan",
                    entity_id=plan_id,
                    phase="preclose",
                    target_path=(
                        "30_TradingPlaybook/Daily/Auto/2026/"
                        "2026-07-16/existing-plan.md"
                    ),
                )
            ]
        )
        initial = artifact(
            "initial-phase-snapshot",
            snapshot_key=f"review:{review_id}:initial",
            immutable=True,
            entity_type="review",
            entity_id=review_id,
            phase="initial_review",
            target_path=(
                "30_TradingPlaybook/Reviews/Auto/2026/2026-07-16/"
                f"initial-review-{plan_id}.md"
            ),
        )
        final = artifact(
            "final-phase-snapshot",
            snapshot_key=f"review:{review_id}:final",
            immutable=True,
            entity_type="review",
            entity_id=review_id,
            phase="final_review",
            target_path=(
                "30_TradingPlaybook/Reviews/Auto/2026/2026-07-16/"
                f"final-review-{plan_id}.md"
            ),
        )
        builder = FakeBuilder(
            by_date={
                ("review", review_id, "initial_review"): initial,
                ("review", review_id, "final_review"): final,
            }
        )
        coordinator = self._coordinator(
            self.session_factory,
            builder=builder,
        )

        rows = await coordinator.reconcile_committed_facts()

        self.assertEqual(
            [call for call in builder.calls if call[0] == "review"],
            [
                ("review", review_id, "initial_review"),
                ("review", review_id, "final_review"),
            ],
        )
        self.assertEqual(
            {row.snapshot_key for row in rows},
            {initial.snapshot_key, final.snapshot_key},
        )

        async with self.session_factory() as session:
            await session.execute(
                TradingPlaybookObsidianExport.__table__.delete().where(
                    TradingPlaybookObsidianExport.snapshot_key
                    == final.snapshot_key
                )
            )
            await session.commit()
        builder.calls.clear()

        repaired = await coordinator.reconcile_committed_facts()

        self.assertEqual(
            [call for call in builder.calls if call[0] == "review"],
            [("review", review_id, "final_review")],
        )
        self.assertEqual(
            {row.snapshot_key for row in repaired},
            {final.snapshot_key},
        )

    async def test_bad_committed_fact_does_not_starve_other_missing_facts(self):
        async with self.session_factory() as session:
            broken_plan = plan_row(version_no=1)
            healthy_plan = plan_row(version_no=2)
            session.add_all([broken_plan, healthy_plan])
            await session.commit()
            broken_plan_id = int(broken_plan.id)
            healthy_plan_id = int(healthy_plan.id)

        healthy_artifact = artifact(
            "healthy-plan",
            snapshot_key=f"plan:{healthy_plan_id}",
            immutable=True,
            entity_type="plan",
            entity_id=healthy_plan_id,
            phase="preclose",
            target_path=(
                "30_TradingPlaybook/Daily/Auto/2026/2026-07-16/"
                "healthy-plan.md"
            ),
        )

        class PartiallyBrokenBuilder(FakeBuilder):
            async def build_plan_artifact(self, plan_version_id):
                self.calls.append(("plan", plan_version_id))
                if plan_version_id == broken_plan_id:
                    raise ValueError("broken historical plan")
                return self.by_date[("plan", plan_version_id)]

        builder = PartiallyBrokenBuilder(
            by_date={("plan", healthy_plan_id): healthy_artifact}
        )
        coordinator = self._coordinator(
            self.session_factory,
            builder=builder,
        )

        rows = await coordinator.reconcile_committed_facts()

        self.assertIn(("plan", broken_plan_id), builder.calls)
        self.assertIn(("plan", healthy_plan_id), builder.calls)
        self.assertIn(
            healthy_artifact.snapshot_key,
            {row.snapshot_key for row in rows},
        )
        self.assertEqual(len(await self._rows(healthy_artifact.snapshot_key)), 1)

    async def test_reconcile_committed_facts_propagates_cancellation(self):
        class CancelledBuilder(FakeBuilder):
            async def build_rule_artifacts(self, catalog_version="v2"):
                raise asyncio.CancelledError()

        async with self.session_factory() as session:
            session.add(
                TradingModeRule(
                    mode_key="cancelled_rule",
                    version=2,
                    name="Cancelled Rule",
                    family="test",
                    style="test",
                    window="test",
                    automation_level="manual",
                    content_hash="c" * 64,
                    enabled=True,
                )
            )
            await session.commit()

        coordinator = self._coordinator(
            self.session_factory,
            builder=CancelledBuilder(),
        )

        with self.assertRaises(asyncio.CancelledError):
            await coordinator.reconcile_committed_facts()

    async def test_startup_reconcile_processes_due_after_fact_scan_failure(self):
        coordinator = self._coordinator(self.session_factory)
        coordinator.reconcile_committed_facts = AsyncMock(
            side_effect=RuntimeError("fact scan unavailable")
        )
        coordinator.process_due = AsyncMock(return_value="processed")

        result = await coordinator.startup_reconcile()

        self.assertEqual(result, "processed")
        coordinator.reconcile_committed_facts.assert_awaited_once_with()
        coordinator.process_due.assert_awaited_once_with(limit=100)

    async def test_bogus_rule_exports_do_not_hide_enabled_v2_rules(self):
        async with self.session_factory() as session:
            session.add_all(
                [
                    TradingModeRule(
                        mode_key=f"real_mode_{index}",
                        version=2,
                        name=f"Real {index}",
                        family="test",
                        style="test",
                        window="test",
                        automation_level="manual",
                        content_hash=str(index) * 64,
                        enabled=True,
                    )
                    for index in range(1, 3)
                ]
            )
            await session.commit()
        bogus = tuple(
            artifact(
                f"bogus-{index}",
                snapshot_key=f"rule:v2:bogus_{index:02d}",
                immutable=True,
                entity_type="rule",
                phase="catalog",
                target_path=(
                    "30_TradingPlaybook/Modes/Auto/v2/"
                    f"bogus_{index:02d}.md"
                ),
            )
            for index in range(1, 20)
        )
        await self.coordinator.enqueue_artifacts(bogus)
        real_rules = tuple(
            artifact(
                f"real-{index}",
                snapshot_key=f"rule:v2:real_mode_{index}",
                immutable=True,
                entity_type="rule",
                phase="catalog",
                target_path=(
                    "30_TradingPlaybook/Modes/Auto/v2/"
                    f"real_mode_{index}.md"
                ),
            )
            for index in range(1, 3)
        )
        builder = FakeBuilder(rules=real_rules)
        coordinator = self._coordinator(
            self.session_factory,
            builder=builder,
        )

        rows = await coordinator.reconcile_committed_facts()

        self.assertIn(("rules", "v2"), builder.calls)
        self.assertEqual(
            {row.snapshot_key for row in rows},
            {rule.snapshot_key for rule in real_rules},
        )

    async def test_startup_reconcile_does_not_clear_an_active_immutable_lease(self):
        immutable = artifact(
            "active-startup",
            snapshot_key="rule:v2:active-startup",
            immutable=True,
            entity_type="rule",
            phase="catalog",
            target_path="30_TradingPlaybook/Modes/Auto/v2/active-startup.md",
        )
        row = (await self.coordinator.enqueue_artifacts([immutable]))[0]
        live_until = datetime(2026, 7, 16, 14, 41)
        active_status = {
            "state": "write_in_progress",
            "lease_token": "another-worker",
        }
        async with self.session_factory() as session:
            await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(TradingPlaybookObsidianExport.id == row.id)
                .values(
                    status="pending",
                    next_attempt_at=live_until,
                    git_status_json=active_status,
                )
            )
            await session.commit()
        writer = FakeWriter(self.temporary_directory.name)
        coordinator = self._coordinator(
            self.session_factory,
            builder=FakeBuilder(),
            exporter=FakeExporter(),
            writer=writer,
        )

        result = await coordinator.startup_reconcile()

        self.assertEqual(writer.write_calls, [])
        self.assertEqual(result.written_files, ())
        stored = (await self._rows(immutable.snapshot_key))[0]
        self.assertEqual(stored.status, "pending")
        self.assertEqual(stored.next_attempt_at, live_until)
        self.assertEqual(stored.git_status_json, active_status)

    async def test_cancelled_error_is_rethrown_and_does_not_mark_failed(self):
        item = artifact("cancel", snapshot_key="alerts:cancel")
        await self.coordinator.enqueue_artifacts([item])
        writer = FakeWriter(self.temporary_directory.name)
        writer.write_error = asyncio.CancelledError()
        coordinator = self._coordinator(
            self.session_factory,
            exporter=FakeExporter(),
            writer=writer,
        )

        with self.assertRaises(asyncio.CancelledError):
            await coordinator.process_due()

        stored = (await self._rows(item.snapshot_key))[0]
        self.assertEqual(stored.status, "pending")
        self.assertEqual(stored.attempt_no, 0)

    async def test_external_cancellation_waits_for_io_and_releases_target_lock(self):
        item = artifact("cancel-blocked", snapshot_key="alerts:cancel-blocked")
        await self.coordinator.enqueue_artifacts([item])
        writer = FakeWriter(self.temporary_directory.name)
        entered = threading.Event()
        release = threading.Event()
        original_write = writer.write_text

        def blocking_write(*args, **kwargs):
            entered.set()
            if not release.wait(timeout=5):
                raise RuntimeError("cancel test writer release timed out")
            return original_write(*args, **kwargs)

        writer.write_text = blocking_write
        coordinator = self._coordinator(
            self.session_factory,
            exporter=FakeExporter(),
            writer=writer,
        )
        task = asyncio.create_task(coordinator.process_due())
        self.assertTrue(await asyncio.to_thread(entered.wait, 5))

        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        contender = asyncio.create_task(
            coordinator._acquire_target_lock(item.target_path)
        )
        await asyncio.sleep(0.05)
        task_finished_early = task.done()
        lock_released_early = contender.done()
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        contender_lock = await asyncio.wait_for(contender, timeout=2)
        await coordinator._to_thread_fenced(contender_lock.release)
        self.assertFalse(task_finished_early)
        self.assertFalse(lock_released_early)

        stored = (await self._rows(item.snapshot_key))[0]
        self.assertEqual(stored.status, "pending")
        self.clock.value += timedelta(seconds=61)
        writer.write_text = original_write
        retried = await asyncio.wait_for(coordinator.process_due(), timeout=2)
        self.assertEqual(retried.written_files, (item.target_path,))

    async def test_repeated_cancellation_while_waiting_for_lock_cleans_up(self):
        coordinator = self._coordinator(
            self.session_factory,
            exporter=FakeExporter(),
            writer=FakeWriter(self.temporary_directory.name),
        )
        target_path = "30_TradingPlaybook/Alerts/Auto/2026/wait-cancel.md"
        holder = await coordinator._acquire_target_lock(target_path)
        waiting = asyncio.create_task(
            coordinator._acquire_target_lock(target_path)
        )
        await asyncio.sleep(0.05)

        waiting.cancel()
        await asyncio.sleep(0)
        waiting.cancel()
        await asyncio.sleep(0.05)
        finished_early = waiting.done()
        await coordinator._to_thread_fenced(holder.release)

        with self.assertRaises(asyncio.CancelledError):
            await waiting
        self.assertFalse(finished_early)
        probe = await asyncio.wait_for(
            coordinator._acquire_target_lock(target_path),
            timeout=2,
        )
        await coordinator._to_thread_fenced(probe.release)


if __name__ == "__main__":
    unittest.main()
