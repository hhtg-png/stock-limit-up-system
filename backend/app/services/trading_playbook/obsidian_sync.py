"""Durable idempotency and lease coordination for Obsidian snapshots."""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import case, desc, exists, or_, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import aliased

from app.models.trading_playbook import TradingPlaybookObsidianExport
from app.services.obsidian_vault_writer import ObsidianVaultWriter
from app.services.trading_playbook.obsidian_exporter import (
    TradingPlaybookObsidianExporter,
)
from app.services.trading_playbook.obsidian_snapshot_builder import (
    TradingPlaybookObsidianSnapshotBuilder,
)
from app.services.trading_playbook.obsidian_types import (
    ObsidianArtifact,
    canonical_json_bytes,
)
from app.utils.time_utils import CN_TZ


_IMMUTABLE_CONFLICT = "immutable_snapshot_hash_conflict"


class TradingPlaybookObsidianSyncCoordinator:
    """Persist frozen artifacts and grant short, cross-process write leases."""

    RETRY_DELAYS = (
        timedelta(minutes=1),
        timedelta(minutes=5),
        timedelta(minutes=15),
    )
    CLAIM_LEASE = timedelta(minutes=1)
    _ENQUEUE_ATTEMPTS = 12

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        builder: TradingPlaybookObsidianSnapshotBuilder,
        exporter: TradingPlaybookObsidianExporter,
        writer: ObsidianVaultWriter,
        clock: Callable[[], datetime],
    ) -> None:
        self.session_factory = session_factory
        self.builder = builder
        self.exporter = exporter
        self.writer = writer
        self.clock = clock

    async def enqueue_artifacts(
        self,
        artifacts: Sequence[ObsidianArtifact],
    ) -> tuple[TradingPlaybookObsidianExport, ...]:
        """Persist artifacts without replacing an existing export fact."""

        now = self._aware_now()
        generated_at = self._canonical_datetime(now)
        database_now = self._database_datetime(now)
        rows: list[TradingPlaybookObsidianExport] = []
        for artifact in artifacts:
            if not isinstance(artifact, ObsidianArtifact):
                raise TypeError("artifacts must contain ObsidianArtifact values")
            rows.append(
                await self._enqueue_one(
                    artifact,
                    generated_at=generated_at,
                    database_now=database_now,
                )
            )
        return tuple(rows)

    async def _enqueue_one(
        self,
        artifact: ObsidianArtifact,
        *,
        generated_at: str,
        database_now: datetime,
    ) -> TradingPlaybookObsidianExport:
        for _ in range(self._ENQUEUE_ATTEMPTS):
            async with self.session_factory() as session:
                if session.get_bind().dialect.name == "sqlite":
                    # SQLite cannot promote two concurrent read transactions
                    # to writers reliably. Acquiring its write reservation
                    # before version discovery gives the same serialized
                    # version allocation that PostgreSQL's unique constraint
                    # and retry loop provide.
                    await session.execute(text("BEGIN IMMEDIATE"))
                existing = list(
                    (
                        await session.execute(
                            select(TradingPlaybookObsidianExport)
                            .where(
                                TradingPlaybookObsidianExport.snapshot_key
                                == artifact.snapshot_key
                            )
                            .order_by(
                                desc(
                                    TradingPlaybookObsidianExport.snapshot_version
                                )
                            )
                        )
                    ).scalars()
                )

                reusable = self._reusable_row(existing, artifact)
                if reusable is not None:
                    return reusable

                if existing and bool(existing[0].immutable) != artifact.immutable:
                    raise ValueError(
                        "snapshot_key cannot change immutable classification"
                    )

                snapshot_version = (
                    int(existing[0].snapshot_version) + 1 if existing else 1
                )
                is_conflict = artifact.immutable and bool(existing)
                if existing and not artifact.immutable:
                    lease_cap = database_now + self.CLAIM_LEASE
                    await session.execute(
                        update(TradingPlaybookObsidianExport)
                        .where(
                            TradingPlaybookObsidianExport.snapshot_key
                            == artifact.snapshot_key,
                            TradingPlaybookObsidianExport.status.in_(
                                ("pending", "failed")
                            ),
                        )
                        .values(
                            status="superseded",
                            next_attempt_at=case(
                                (
                                    TradingPlaybookObsidianExport.next_attempt_at
                                    <= database_now,
                                    None,
                                ),
                                (
                                    TradingPlaybookObsidianExport.next_attempt_at
                                    > lease_cap,
                                    lease_cap,
                                ),
                                else_=TradingPlaybookObsidianExport.next_attempt_at,
                            ),
                            updated_at=database_now,
                        )
                    )

                row = TradingPlaybookObsidianExport(
                    snapshot_key=artifact.snapshot_key,
                    snapshot_version=snapshot_version,
                    trade_date=artifact.trade_date,
                    entity_type=artifact.entity_type,
                    entity_id=artifact.entity_id,
                    phase=artifact.phase,
                    target_path=artifact.target_path,
                    source_hash=artifact.source_hash,
                    snapshot_json={
                        "payload": artifact.payload_json(),
                        "generated_at": generated_at,
                    },
                    immutable=artifact.immutable,
                    status="failed" if is_conflict else "pending",
                    attempt_no=0,
                    next_attempt_at=None,
                    last_error=_IMMUTABLE_CONFLICT if is_conflict else None,
                    git_status_json=None,
                    exported_at=None,
                    created_at=database_now,
                    updated_at=database_now,
                )
                session.add(row)
                try:
                    await session.commit()
                except IntegrityError:
                    await session.rollback()
                    # A concurrent session won the unique snapshot version.
                    # Start from a new transaction and either reuse its row or
                    # allocate the next monotonically increasing version.
                    continue
                await session.refresh(row)
                return row

        raise RuntimeError("could not allocate a unique Obsidian snapshot version")

    @staticmethod
    def _reusable_row(
        existing: list[TradingPlaybookObsidianExport],
        artifact: ObsidianArtifact,
    ) -> TradingPlaybookObsidianExport | None:
        if not existing:
            return None
        if artifact.immutable:
            return next(
                (row for row in existing if row.source_hash == artifact.source_hash),
                None,
            )
        latest = existing[0]
        return latest if latest.source_hash == artifact.source_hash else None

    async def _claim_due(
        self,
        *,
        limit: int = 100,
    ) -> tuple[TradingPlaybookObsidianExport, ...]:
        """Lease due rows via conditional updates without inventing a status."""

        if limit <= 0:
            return ()
        now = self._database_datetime(self._aware_now())
        lease_until = now + self.CLAIM_LEASE
        async with self.session_factory() as session:
            candidate_ids = tuple(
                (
                    await session.execute(
                        self._due_select_statement(now=now, limit=limit)
                    )
                ).scalars()
            )
            claimed_ids: list[int] = []
            for row_id in candidate_ids:
                claimed = await session.execute(
                    self._claim_statement(
                        row_id=int(row_id),
                        now=now,
                        lease_until=lease_until,
                    )
                )
                if claimed.rowcount == 1:
                    claimed_ids.append(int(row_id))
            await session.commit()

        if not claimed_ids:
            return ()
        reload_now = self._database_datetime(self._aware_now())
        async with self.session_factory() as session:
            rows = list(
                (
                    await session.execute(
                        select(TradingPlaybookObsidianExport).where(
                            TradingPlaybookObsidianExport.id.in_(claimed_ids),
                            *self._active_lease_predicates(
                                now=reload_now,
                                lease_until=lease_until,
                            ),
                        )
                    )
                ).scalars()
            )
            by_id = {int(row.id): row for row in rows}
            return tuple(
                by_id[row_id] for row_id in claimed_ids if row_id in by_id
            )

    @classmethod
    def _due_select_statement(cls, *, now: datetime, limit: int):
        return (
            select(TradingPlaybookObsidianExport.id)
            .where(*cls._claim_predicates(now=now))
            .order_by(
                TradingPlaybookObsidianExport.next_attempt_at,
                TradingPlaybookObsidianExport.created_at,
                TradingPlaybookObsidianExport.id,
            )
            .limit(limit)
        )

    @classmethod
    def _claim_statement(
        cls,
        *,
        row_id: int,
        now: datetime,
        lease_until: datetime,
    ):
        return (
            update(TradingPlaybookObsidianExport)
            .where(
                TradingPlaybookObsidianExport.id == row_id,
                *cls._claim_predicates(now=now),
            )
            .values(next_attempt_at=lease_until, updated_at=now)
        )

    @classmethod
    def _claim_predicates(cls, *, now: datetime) -> tuple[Any, ...]:
        return (
            *cls._current_writer_predicates(now=now),
            or_(
                TradingPlaybookObsidianExport.next_attempt_at.is_(None),
                TradingPlaybookObsidianExport.next_attempt_at <= now,
            ),
        )

    @classmethod
    def _active_lease_predicates(
        cls,
        *,
        now: datetime,
        lease_until: datetime,
    ) -> tuple[Any, ...]:
        """Validate a token against a freshly read clock and current row.

        Task 8 must reuse these predicates immediately before file I/O and
        again for its conditional completion update, passing a newly sampled
        database wall clock rather than the original claim time.
        """

        return (
            *cls._current_writer_predicates(now=now),
            TradingPlaybookObsidianExport.next_attempt_at == lease_until,
            TradingPlaybookObsidianExport.next_attempt_at > now,
        )

    @classmethod
    def _current_writer_predicates(
        cls,
        *,
        now: datetime,
    ) -> tuple[Any, ...]:
        newer = aliased(TradingPlaybookObsidianExport)
        older = aliased(TradingPlaybookObsidianExport)
        has_newer_version = exists(
            select(1).where(
                newer.snapshot_key
                == TradingPlaybookObsidianExport.snapshot_key,
                newer.snapshot_version
                > TradingPlaybookObsidianExport.snapshot_version,
            )
        )
        has_older_live_lease = exists(
            select(1).where(
                older.snapshot_key
                == TradingPlaybookObsidianExport.snapshot_key,
                older.snapshot_version
                < TradingPlaybookObsidianExport.snapshot_version,
                older.status == "superseded",
                older.next_attempt_at.is_not(None),
                older.next_attempt_at > now,
            )
        )
        return (
            TradingPlaybookObsidianExport.status.in_(("pending", "failed")),
            or_(
                TradingPlaybookObsidianExport.last_error.is_(None),
                TradingPlaybookObsidianExport.last_error != _IMMUTABLE_CONFLICT,
            ),
            or_(
                TradingPlaybookObsidianExport.immutable.is_(False),
                TradingPlaybookObsidianExport.snapshot_version == 1,
            ),
            or_(
                TradingPlaybookObsidianExport.immutable.is_(True),
                ~has_newer_version,
            ),
            or_(
                TradingPlaybookObsidianExport.immutable.is_(True),
                ~has_older_live_lease,
            ),
        )

    def _aware_now(self) -> datetime:
        now = self.clock()
        if not isinstance(now, datetime):
            raise TypeError("clock must return datetime")
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("clock must return a timezone-aware datetime")
        return now

    @staticmethod
    def _canonical_datetime(value: datetime) -> str:
        normalized = json.loads(canonical_json_bytes({"value": value}))
        return str(normalized["value"])

    @staticmethod
    def _database_datetime(value: datetime) -> datetime:
        return value.astimezone(CN_TZ).replace(tzinfo=None)


__all__ = ("TradingPlaybookObsidianSyncCoordinator",)
