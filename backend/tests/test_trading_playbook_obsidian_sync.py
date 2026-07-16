import asyncio
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.database import Base
from app.models.trading_playbook import TradingPlaybookObsidianExport
from app.services.trading_playbook.obsidian_sync import (
    TradingPlaybookObsidianSyncCoordinator,
)
from app.services.trading_playbook.obsidian_types import ObsidianArtifact


FIXED_NOW = datetime(2026, 7, 16, 6, 40, tzinfo=timezone.utc)
TRADE_DATE = date(2026, 7, 16)


class MutableClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


def artifact(
    marker: str,
    *,
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
        trade_date=TRADE_DATE,
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

    def _coordinator(self, session_factory):
        return TradingPlaybookObsidianSyncCoordinator(
            session_factory=session_factory,
            builder=object(),
            exporter=object(),
            writer=object(),
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


if __name__ == "__main__":
    unittest.main()
