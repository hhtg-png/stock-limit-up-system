"""Durable idempotency and lease coordination for Obsidian snapshots."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import tempfile
from collections.abc import Callable, Sequence
from datetime import date, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any

from sqlalchemy import case, desc, exists, func, or_, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import aliased

from app.models.trading_playbook import (
    TradingExecutionReview,
    TradingPlanVersion,
    TradingPlaybookObsidianExport,
)
from app.services.obsidian_vault_writer import ObsidianVaultWriter
from app.services.trading_playbook.obsidian_exporter import (
    TradingPlaybookObsidianExporter,
)
from app.services.trading_playbook.obsidian_snapshot_builder import (
    TradingPlaybookObsidianSnapshotBuilder,
)
from app.services.trading_playbook.obsidian_types import (
    ObsidianArtifact,
    ObsidianSyncBatchResult,
    TRADING_PLAYBOOK_ALLOWED_ROOTS,
    canonical_json_bytes,
)
from app.utils.time_utils import CN_TZ


_IMMUTABLE_CONFLICT = "immutable_snapshot_hash_conflict"
_MAX_ERROR_LENGTH = 2000


class _TargetFileLock:
    """A crash-released, cross-process lock for one Vault target path."""

    def __init__(self, handle) -> None:
        self._handle = handle

    @classmethod
    def acquire(cls, vault: Path, target_path: str) -> _TargetFileLock:
        identity = f"{vault.resolve(strict=False)}\0{target_path}".encode("utf-8")
        digest = hashlib.sha256(identity).hexdigest()
        lock_root = Path(tempfile.gettempdir()) / "stock-limit-up-obsidian-locks"
        lock_root.mkdir(parents=True, exist_ok=True)
        handle = (lock_root / f"{digest}.lock").open("a+b")
        try:
            if os.name == "nt":
                import msvcrt

                handle.seek(0, os.SEEK_END)
                if handle.tell() == 0:
                    handle.write(b"\0")
                    handle.flush()
                handle.seek(0)
                while True:
                    try:
                        msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
                        break
                    except OSError as exc:
                        if exc.errno not in {13, 36}:
                            raise
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        except BaseException:
            handle.close()
            raise
        return cls(handle)

    def release(self) -> None:
        handle = self._handle
        if handle is None:
            return
        self._handle = None
        try:
            if os.name == "nt":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


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
                                ("pending", "failed", "paused")
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

    async def process_due(self, *, limit: int = 100) -> ObsidianSyncBatchResult:
        """Write one durable batch while fencing every filesystem mutation."""

        if limit <= 0:
            return self._empty_result()
        if not await self._writer_available():
            paused = await self._pause_due(limit=limit)
            return self._result_for_rows(
                paused,
                pending_files=tuple(row.target_path for row in paused),
                git_status=self._no_git_status(),
            )

        await self.resume_paused()
        claimed = await self._claim_due(limit=limit)
        if not claimed:
            return await self._retry_failed_git(limit=limit)

        written: list[str] = []
        skipped: list[str] = []
        pending: list[str] = []
        failed: list[str] = []
        changed_row_ids: list[int] = []
        for row in claimed:
            lease_until = row.next_attempt_at
            if not isinstance(lease_until, datetime):
                pending.append(row.target_path)
                continue
            try:
                restored = self._artifact_from_row(row)
                content = self.exporter.render(
                    restored[0],
                    generated_at=restored[1],
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                marked = await self._mark_failed(
                    row_id=int(row.id),
                    lease_until=lease_until,
                    attempt_no=int(row.attempt_no),
                    error=exc,
                )
                (failed if marked else pending).append(row.target_path)
                continue

            if not await self._lease_is_active(int(row.id), lease_until):
                pending.append(row.target_path)
                continue
            try:
                outcome = await self._write_with_fence(
                    row=row,
                    lease_until=lease_until,
                    content=content,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                marked = await self._mark_failed(
                    row_id=int(row.id),
                    lease_until=lease_until,
                    attempt_no=int(row.attempt_no),
                    error=exc,
                )
                (failed if marked else pending).append(row.target_path)
                continue
            if outcome == "written":
                written.append(row.target_path)
                changed_row_ids.append(int(row.id))
            elif outcome == "skipped":
                skipped.append(row.target_path)
            elif outcome == "failed":
                failed.append(row.target_path)
            else:
                pending.append(row.target_path)

        changed_paths = tuple(dict.fromkeys(written))
        if changed_paths:
            trade_date, phase = self._batch_context(claimed)
            git_status = await self._commit_paths(
                changed_paths,
                trade_date=trade_date,
                phase=phase,
            )
            await self._store_git_status(changed_row_ids, git_status)
        else:
            git_status = self._no_git_status()
        return self._result_for_rows(
            claimed,
            written_files=tuple(written),
            skipped_files=tuple(skipped),
            pending_files=tuple(pending),
            failed_files=tuple(failed),
            git_status=git_status,
        )

    async def resume_paused(self) -> int:
        """Return current paused snapshots to pending after configuration recovers."""

        if not await self._writer_available():
            return 0
        now = self._database_datetime(self._aware_now())
        newer = aliased(TradingPlaybookObsidianExport)
        has_newer_version = exists(
            select(1).where(
                newer.snapshot_key
                == TradingPlaybookObsidianExport.snapshot_key,
                newer.snapshot_version
                > TradingPlaybookObsidianExport.snapshot_version,
            )
        )
        async with self.session_factory() as session:
            await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(
                    TradingPlaybookObsidianExport.status == "paused",
                    TradingPlaybookObsidianExport.immutable.is_(False),
                    has_newer_version,
                )
                .values(
                    status="superseded",
                    next_attempt_at=None,
                    updated_at=now,
                )
            )
            resumed = await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(TradingPlaybookObsidianExport.status == "paused")
                .values(
                    status="pending",
                    next_attempt_at=None,
                    updated_at=now,
                )
            )
            await session.commit()
            return int(resumed.rowcount or 0)

    async def export_trade_date(
        self,
        trade_date: date,
        *,
        include_rules: bool = False,
        force: bool = False,
    ) -> ObsidianSyncBatchResult:
        """Rebuild every current database fact relevant to one trade date."""

        if type(trade_date) is not date:
            raise ValueError("trade_date must be a date")
        if type(include_rules) is not bool or type(force) is not bool:
            raise ValueError("include_rules and force must be booleans")
        if force:
            await self._reset_forced_exports(trade_date)

        plan_ids, reviews = await self._fact_references_for_date(trade_date)
        artifacts: list[ObsidianArtifact] = []
        if include_rules:
            artifacts.extend(await self.builder.build_rule_artifacts("v2"))
        for plan_id in plan_ids:
            artifacts.append(await self.builder.build_plan_artifact(plan_id))
        for review_id, finalized in reviews:
            artifacts.append(
                await self.builder.build_review_artifact(
                    review_id,
                    phase="final_review" if finalized else "initial_review",
                )
            )
        artifacts.extend(await self._build_mutable_artifacts(trade_date))
        await self.enqueue_artifacts(artifacts)
        return await self.process_due(limit=max(100, len(artifacts)))

    async def startup_reconcile(self) -> ObsidianSyncBatchResult:
        """Resume immutable work and refresh only the two latest date axes."""

        await self._resume_unfinished_immutable()
        dates = await self._latest_plan_dates()
        artifact_count = 0
        for trade_date in dates:
            artifacts = await self._build_mutable_artifacts(trade_date)
            artifact_count += len(artifacts)
            await self.enqueue_artifacts(artifacts)
        return await self.process_due(limit=max(100, artifact_count))

    async def _pause_due(
        self,
        *,
        limit: int,
    ) -> tuple[TradingPlaybookObsidianExport, ...]:
        now = self._database_datetime(self._aware_now())
        async with self.session_factory() as session:
            candidate_ids = tuple(
                (
                    await session.execute(
                        self._due_select_statement(now=now, limit=limit)
                    )
                ).scalars()
            )
            if not candidate_ids:
                return ()
            paused_ids: list[int] = []
            for row_id in candidate_ids:
                result = await session.execute(
                    update(TradingPlaybookObsidianExport)
                    .where(
                        TradingPlaybookObsidianExport.id == int(row_id),
                        *self._claim_predicates(now=now),
                    )
                    .values(
                        status="paused",
                        next_attempt_at=None,
                        updated_at=now,
                    )
                )
                if result.rowcount == 1:
                    paused_ids.append(int(row_id))
            await session.commit()
        if not paused_ids:
            return ()
        async with self.session_factory() as session:
            rows = list(
                (
                    await session.execute(
                        select(TradingPlaybookObsidianExport).where(
                            TradingPlaybookObsidianExport.id.in_(paused_ids)
                        )
                    )
                ).scalars()
            )
        by_id = {int(row.id): row for row in rows}
        return tuple(by_id[row_id] for row_id in paused_ids if row_id in by_id)

    def _artifact_from_row(
        self,
        row: TradingPlaybookObsidianExport,
    ) -> tuple[ObsidianArtifact, datetime]:
        envelope = row.snapshot_json
        if type(envelope) is not dict or set(envelope) != {
            "payload",
            "generated_at",
        }:
            raise ValueError("snapshot_json must be the exact persisted envelope")
        payload = envelope["payload"]
        generated_text = envelope["generated_at"]
        if type(payload) is not dict:
            raise ValueError("snapshot payload must be a JSON object")
        if type(generated_text) is not str or not generated_text:
            raise ValueError("snapshot generated_at must be a canonical timestamp")
        try:
            generated_at = datetime.fromisoformat(
                generated_text[:-1] + "+00:00"
                if generated_text.endswith("Z")
                else generated_text
            )
        except ValueError as exc:
            raise ValueError("snapshot generated_at is invalid") from exc
        if generated_at.tzinfo is None or generated_at.utcoffset() is None:
            raise ValueError("snapshot generated_at must be timezone-aware")
        if self._canonical_datetime(generated_at) != generated_text:
            raise ValueError("snapshot generated_at is not canonical")
        if type(row.trade_date) is not date:
            raise ValueError("snapshot trade_date is invalid")
        if type(row.immutable) is not bool:
            raise ValueError("snapshot immutable flag is invalid")
        if row.entity_id is not None and (
            isinstance(row.entity_id, bool) or not isinstance(row.entity_id, int)
        ):
            raise ValueError("snapshot entity_id is invalid")
        restored = ObsidianArtifact(
            snapshot_key=row.snapshot_key,
            trade_date=row.trade_date,
            entity_type=row.entity_type,
            entity_id=row.entity_id,
            phase=row.phase,
            target_path=row.target_path,
            immutable=row.immutable,
            payload=payload,
        )
        if restored.source_hash != row.source_hash:
            raise ValueError("snapshot source_hash does not match its payload")
        return restored, generated_at

    async def _lease_is_active(
        self,
        row_id: int,
        lease_until: datetime,
    ) -> bool:
        now = self._database_datetime(self._aware_now())
        async with self.session_factory() as session:
            active_id = await session.scalar(
                select(TradingPlaybookObsidianExport.id).where(
                    TradingPlaybookObsidianExport.id == row_id,
                    *self._active_lease_predicates(
                        now=now,
                        lease_until=lease_until,
                    ),
                )
            )
            return active_id is not None

    async def _write_with_fence(
        self,
        *,
        row: TradingPlaybookObsidianExport,
        lease_until: datetime,
        content: str,
    ) -> str:
        """Fence one target without holding a SQLite write lock across I/O.

        The cross-process file lock serializes the resource.  Only after the
        lock is held do we renew the exact database token in a short committed
        transaction, so a worker whose lease expired while waiting cannot
        write late.  Completion is another short conditional transaction.
        """

        target_lock = await self._acquire_target_lock(row.target_path)
        try:
            renewed_until = await self._renew_lease(
                row_id=int(row.id),
                lease_until=lease_until,
            )
            if renewed_until is None:
                return "pending"
            try:
                result = await self._to_thread_fenced(
                    self.writer.write_text,
                    row.target_path,
                    content,
                    allowed_roots=TRADING_PLAYBOOK_ALLOWED_ROOTS,
                )
                changed = getattr(result, "changed", None)
                if type(changed) is not bool:
                    raise TypeError("writer result changed must be a boolean")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                marked = await self._mark_failed(
                    row_id=int(row.id),
                    lease_until=renewed_until,
                    attempt_no=int(row.attempt_no),
                    error=exc,
                )
                return "failed" if marked else "pending"

            completed = await self._mark_written(
                row_id=int(row.id),
                lease_until=renewed_until,
            )
            if not completed:
                return "pending"
            return "written" if changed else "skipped"
        finally:
            try:
                await self._to_thread_fenced(target_lock.release)
            except asyncio.CancelledError:
                raise
            except Exception:
                # release() always closes its handle in a finally block, so an
                # unlock diagnostic must not undo a completed durable write.
                pass

    async def _renew_lease(
        self,
        *,
        row_id: int,
        lease_until: datetime,
    ) -> datetime | None:
        now = self._database_datetime(self._aware_now())
        renewed_until = now + self.CLAIM_LEASE
        async with self.session_factory() as session:
            renewed = await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(
                    TradingPlaybookObsidianExport.id == row_id,
                    *self._active_lease_predicates(
                        now=now,
                        lease_until=lease_until,
                    ),
                )
                .values(next_attempt_at=renewed_until, updated_at=now)
            )
            await session.commit()
            return renewed_until if renewed.rowcount == 1 else None

    async def _mark_written(
        self,
        *,
        row_id: int,
        lease_until: datetime,
    ) -> bool:
        completed_at = self._database_datetime(self._aware_now())
        async with self.session_factory() as session:
            completed = await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(
                    TradingPlaybookObsidianExport.id == row_id,
                    *self._active_lease_predicates(
                        now=completed_at,
                        lease_until=lease_until,
                    ),
                )
                .values(
                    status="written",
                    next_attempt_at=None,
                    last_error=None,
                    exported_at=completed_at,
                    updated_at=completed_at,
                )
            )
            await session.commit()
            return completed.rowcount == 1

    async def _acquire_target_lock(self, target_path: str) -> _TargetFileLock:
        vault = await self._to_thread_fenced(self.writer.configured_vault)
        if vault is None:
            raise ValueError("Obsidian Vault path is not configured")
        task = asyncio.create_task(
            asyncio.to_thread(_TargetFileLock.acquire, vault, target_path)
        )
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError as cancelled:
            # If cancellation arrived while the OS lock was blocked, wait for
            # acquisition and release it before propagating cancellation.
            try:
                lock = await task
            except BaseException:
                raise cancelled
            try:
                await self._to_thread_fenced(lock.release)
            finally:
                raise cancelled

    async def _mark_failed(
        self,
        *,
        row_id: int,
        lease_until: datetime,
        attempt_no: int,
        error: Exception,
    ) -> bool:
        async with self.session_factory() as session:
            marked = await self._mark_failed_in_session(
                session,
                row_id=row_id,
                lease_until=lease_until,
                attempt_no=attempt_no,
                error=error,
            )
            await session.commit()
            return marked

    async def _mark_failed_in_session(
        self,
        session: AsyncSession,
        *,
        row_id: int,
        lease_until: datetime,
        attempt_no: int,
        error: Exception,
    ) -> bool:
        now = self._database_datetime(self._aware_now())
        next_attempt_no = attempt_no + 1
        delay = self.RETRY_DELAYS[min(next_attempt_no - 1, len(self.RETRY_DELAYS) - 1)]
        result = await session.execute(
            update(TradingPlaybookObsidianExport)
            .where(
                TradingPlaybookObsidianExport.id == row_id,
                *self._active_lease_predicates(
                    now=now,
                    lease_until=lease_until,
                ),
            )
            .values(
                status="failed",
                attempt_no=next_attempt_no,
                next_attempt_at=now + delay,
                last_error=self._safe_error(error),
                updated_at=now,
            )
        )
        return result.rowcount == 1

    async def _commit_paths(
        self,
        paths: tuple[str, ...],
        *,
        trade_date: date,
        phase: str,
    ) -> dict[str, Any]:
        try:
            raw_status = await self._to_thread_fenced(
                self.writer.commit_paths,
                paths,
                allowed_roots=TRADING_PLAYBOOK_ALLOWED_ROOTS,
                message=f"obsidian: export trading playbook {trade_date} {phase}",
            )
            return self._json_git_status(raw_status)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return {
                "enabled": bool(getattr(self.writer, "auto_git_enabled", False)),
                "committed": False,
                "error": self._safe_error(exc),
            }

    async def _store_git_status(
        self,
        row_ids: Sequence[int],
        status: dict[str, Any],
    ) -> None:
        unique_ids = tuple(dict.fromkeys(int(row_id) for row_id in row_ids))
        if not unique_ids:
            return
        now = self._database_datetime(self._aware_now())
        async with self.session_factory() as session:
            await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(
                    TradingPlaybookObsidianExport.id.in_(unique_ids),
                    TradingPlaybookObsidianExport.status == "written",
                )
                .values(git_status_json=status, updated_at=now)
            )
            await session.commit()

    async def _retry_failed_git(self, *, limit: int) -> ObsidianSyncBatchResult:
        async with self.session_factory() as session:
            candidates = list(
                (
                    await session.execute(
                        select(TradingPlaybookObsidianExport)
                        .where(
                            TradingPlaybookObsidianExport.status == "written",
                            TradingPlaybookObsidianExport.git_status_json.is_not(None),
                        )
                        .order_by(
                            TradingPlaybookObsidianExport.trade_date,
                            TradingPlaybookObsidianExport.id,
                        )
                    )
                ).scalars()
            )
        retry_rows = tuple(
            row
            for row in candidates
            if self._git_failed(row.git_status_json)
        )[:limit]
        if not retry_rows:
            return self._empty_result()
        paths = tuple(dict.fromkeys(row.target_path for row in retry_rows))
        trade_date, phase = self._batch_context(retry_rows)
        status = await self._commit_paths(
            paths,
            trade_date=trade_date,
            phase=phase,
        )
        await self._store_git_status(
            [int(row.id) for row in retry_rows],
            status,
        )
        return self._result_for_rows(retry_rows, git_status=status)

    async def _fact_references_for_date(
        self,
        trade_date: date,
    ) -> tuple[tuple[int, ...], tuple[tuple[int, bool], ...]]:
        async with self.session_factory() as session:
            plan_ids = tuple(
                int(plan_id)
                for plan_id in (
                    await session.scalars(
                        select(TradingPlanVersion.id)
                        .where(
                            or_(
                                TradingPlanVersion.source_trade_date == trade_date,
                                TradingPlanVersion.target_trade_date == trade_date,
                            )
                        )
                        .order_by(TradingPlanVersion.id)
                    )
                ).all()
            )
            review_rows = tuple(
                (int(review_id), finalized_at is not None)
                for review_id, finalized_at in (
                    await session.execute(
                        select(
                            TradingExecutionReview.id,
                            TradingExecutionReview.finalized_at,
                        )
                        .where(TradingExecutionReview.trade_date == trade_date)
                        .order_by(TradingExecutionReview.id)
                    )
                ).all()
            )
        return plan_ids, review_rows

    async def _build_mutable_artifacts(
        self,
        trade_date: date,
    ) -> tuple[ObsidianArtifact, ...]:
        return (
            await self.builder.build_alerts_artifact(trade_date),
            await self.builder.build_daily_index_artifact(trade_date),
            await self.builder.build_dashboard_artifact(trade_date),
        )

    async def _reset_forced_exports(self, trade_date: date) -> None:
        now = self._database_datetime(self._aware_now())
        relevant = or_(
            TradingPlaybookObsidianExport.trade_date == trade_date,
            TradingPlaybookObsidianExport.snapshot_key
            == "dashboard:trading-playbook",
        )
        async with self.session_factory() as session:
            await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(
                    relevant,
                    TradingPlaybookObsidianExport.status.in_(("failed", "paused")),
                    or_(
                        TradingPlaybookObsidianExport.last_error.is_(None),
                        TradingPlaybookObsidianExport.last_error
                        != _IMMUTABLE_CONFLICT,
                    ),
                )
                .values(
                    status="pending",
                    attempt_no=0,
                    next_attempt_at=None,
                    last_error=None,
                    updated_at=now,
                )
            )
            await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(relevant)
                .values(git_status_json=None, updated_at=now)
            )
            await session.commit()

    async def _resume_unfinished_immutable(self) -> None:
        now = self._database_datetime(self._aware_now())
        async with self.session_factory() as session:
            await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(
                    TradingPlaybookObsidianExport.immutable.is_(True),
                    TradingPlaybookObsidianExport.status.in_(
                        ("pending", "failed", "paused")
                    ),
                    or_(
                        TradingPlaybookObsidianExport.last_error.is_(None),
                        TradingPlaybookObsidianExport.last_error
                        != _IMMUTABLE_CONFLICT,
                    ),
                )
                .values(status="pending", next_attempt_at=None, updated_at=now)
            )
            await session.commit()

    async def _latest_plan_dates(self) -> tuple[date, ...]:
        async with self.session_factory() as session:
            source_date, target_date = (
                await session.execute(
                    select(
                        func.max(TradingPlanVersion.source_trade_date),
                        func.max(TradingPlanVersion.target_trade_date),
                    )
                )
            ).one()
        return tuple(sorted({item for item in (source_date, target_date) if item}))

    async def _to_thread_fenced(self, function: Callable[..., Any], *args, **kwargs):
        task = asyncio.create_task(
            asyncio.to_thread(partial(function, *args, **kwargs))
        )
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            # A cancelled await does not stop its worker thread.  Wait until
            # the filesystem/Git call really exits before releasing the
            # target resource, then preserve cancellation for the caller.
            try:
                await task
            except BaseException:
                pass
            raise

    async def _writer_available(self) -> bool:
        if not bool(getattr(self.writer, "enabled", False)):
            return False
        try:
            return (
                await self._to_thread_fenced(self.writer.configured_vault)
            ) is not None
        except (OSError, ValueError):
            return False

    def _result_for_rows(
        self,
        rows: Sequence[TradingPlaybookObsidianExport],
        *,
        written_files: tuple[str, ...] = (),
        skipped_files: tuple[str, ...] = (),
        pending_files: tuple[str, ...] = (),
        failed_files: tuple[str, ...] = (),
        git_status: dict[str, Any] | None = None,
    ) -> ObsidianSyncBatchResult:
        trade_date, phase = self._batch_context(rows)
        return ObsidianSyncBatchResult(
            trade_date=trade_date,
            phase=phase,
            written_files=written_files,
            skipped_files=skipped_files,
            pending_files=pending_files,
            failed_files=failed_files,
            git_status=git_status if git_status is not None else self._no_git_status(),
        )

    def _empty_result(self) -> ObsidianSyncBatchResult:
        return self._result_for_rows(())

    def _batch_context(
        self,
        rows: Sequence[TradingPlaybookObsidianExport],
    ) -> tuple[date, str]:
        if not rows:
            return self._aware_now().astimezone(CN_TZ).date(), "reconcile"
        trade_date = max(row.trade_date for row in rows)
        phases = {row.phase for row in rows}
        # A due scan can mix schedules and dates.  The newest represented date
        # plus reconcile for mixed phases is stable across query/claim order.
        phase = next(iter(phases)) if len(phases) == 1 else "reconcile"
        return trade_date, phase

    def _no_git_status(self) -> dict[str, Any]:
        return {
            "enabled": bool(getattr(self.writer, "auto_git_enabled", False)),
            "committed": False,
            "reason": "no_written_files",
        }

    @staticmethod
    def _safe_error(error: BaseException) -> str:
        try:
            text_value = str(error)
        except Exception:
            text_value = type(error).__name__
        safe_text = "".join(
            " " if character in "\r\n\t" else character
            if ord(character) >= 32
            else "�"
            for character in text_value
        )
        return safe_text[:_MAX_ERROR_LENGTH]

    @staticmethod
    def _git_failed(status: object) -> bool:
        return type(status) is dict and bool(status.get("error"))

    @classmethod
    def _json_git_status(cls, status: object) -> dict[str, Any]:
        if type(status) is not dict:
            raise TypeError("Git status must be a JSON object")
        try:
            encoded = json.dumps(
                status,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            decoded = json.loads(encoded)
        except (TypeError, ValueError) as exc:
            raise ValueError("Git status must contain strict JSON values") from exc
        if type(decoded) is not dict:
            raise TypeError("Git status must be a JSON object")
        return decoded

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
