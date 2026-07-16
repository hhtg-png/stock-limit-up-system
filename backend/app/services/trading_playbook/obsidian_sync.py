"""Durable idempotency and lease coordination for Obsidian snapshots."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import stat
import unicodedata
from collections.abc import Callable, Sequence
from datetime import date, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any

from loguru import logger
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
from app.services.trading_playbook.rule_catalog import EXPECTED_RULE_COUNT
from app.utils.time_utils import CN_TZ


_IMMUTABLE_CONFLICT = "immutable_snapshot_hash_conflict"
_MAX_ERROR_LENGTH = 2000
_MAX_GIT_JSON_DEPTH = 4
_MAX_GIT_JSON_ITEMS = 16
_MAX_GIT_JSON_STRING = 512
_MAX_GIT_JSON_BYTES = 32 * 1024
_LOCK_ROOT = "30_TradingPlaybook/Daily/Auto/.sync-locks"


class _TargetFileLock:
    """A crash-released lock stored inside the shared Vault.

    POSIX lock state is group-shareable (0770 directories and 0660 files).
    This coordinates same-group accounts and hosts only when the shared
    filesystem honors POSIX/Windows advisory byte-range locks (for example a
    correctly configured SMB share).  It fails closed on lexical symlinks and
    untrusted existing state, but cannot claim cross-host correctness for a
    filesystem that ignores advisory locks or changes path components outside
    the operating system's open-time protections.
    """

    def __init__(self, handle) -> None:
        self._handle = handle

    @classmethod
    def acquire(
        cls,
        writer: ObsidianVaultWriter,
        relative_path: str,
    ) -> _TargetFileLock:
        resolved_target = writer.resolve_target(
            relative_path,
            allowed_roots=TRADING_PLAYBOOK_ALLOWED_ROOTS,
        )
        vault = writer.configured_vault()
        if vault is None:
            raise ValueError("Obsidian Vault path is not configured")
        vault_path = Path(vault)
        lexical_target = vault_path.joinpath(*relative_path.split("/"))
        cls._reject_lexical_symlinks(vault_path, lexical_target.parent)
        if os.name == "nt":
            lexical_target.parent.mkdir(parents=True, mode=0o770, exist_ok=True)
            cls._reject_lexical_symlinks(vault_path, lexical_target.parent)
            cls._validate_trusted_path(lexical_target.parent, directory=True)
            if lexical_target.exists():
                cls._validate_trusted_path(lexical_target, directory=False)
            descriptor = os.open(lexical_target, os.O_RDWR | os.O_CREAT, 0o660)
        else:
            vault_path.mkdir(parents=True, mode=0o770, exist_ok=True)
            descriptor = cls._open_posix_descriptor(vault_path, lexical_target)
        try:
            target = writer.resolve_target(
                relative_path,
                allowed_roots=TRADING_PLAYBOOK_ALLOWED_ROOTS,
            )
            cls._reject_lexical_symlinks(vault_path, lexical_target)
            if (
                target != resolved_target
                or target != lexical_target.resolve(strict=False)
            ):
                raise ValueError("Obsidian sync lock path changed during validation")
            handle = os.fdopen(descriptor, "r+b", buffering=0)
        except BaseException:
            os.close(descriptor)
            raise
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

    @staticmethod
    def _reject_lexical_symlinks(vault: Path, target: Path) -> None:
        current = vault
        if current.is_symlink():
            raise ValueError("Obsidian Vault lock path cannot contain a symlink")
        try:
            relative_parts = target.relative_to(vault).parts
        except ValueError as exc:
            raise ValueError("Obsidian sync lock escapes the Vault") from exc
        for part in relative_parts:
            current = current / part
            if current.is_symlink():
                raise ValueError("Obsidian Vault lock path cannot contain a symlink")

    @classmethod
    def _open_posix_descriptor(cls, vault: Path, target: Path) -> int:
        required_flags = ("O_DIRECTORY", "O_NOFOLLOW")
        if any(not hasattr(os, name) for name in required_flags):
            raise OSError("secure openat locking is unavailable")
        directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        current = os.open(vault, directory_flags)
        try:
            cls._validate_descriptor(current, directory=True)
            parent_parts = target.parent.relative_to(vault).parts
            for index, part in enumerate(parent_parts):
                created = False
                try:
                    following = os.open(
                        part,
                        directory_flags,
                        dir_fd=current,
                    )
                except FileNotFoundError:
                    os.mkdir(part, 0o770, dir_fd=current)
                    created = True
                    following = os.open(
                        part,
                        directory_flags,
                        dir_fd=current,
                    )
                try:
                    cls._validate_descriptor(following, directory=True)
                    info = os.fstat(following)
                    if created or (
                        index == len(parent_parts) - 1
                        and info.st_uid == os.geteuid()
                    ):
                        os.fchmod(following, 0o770)
                except BaseException:
                    os.close(following)
                    raise
                os.close(current)
                current = following
            descriptor = os.open(
                target.name,
                os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW,
                0o660,
                dir_fd=current,
            )
            try:
                cls._validate_descriptor(descriptor, directory=False)
                if os.fstat(descriptor).st_uid == os.geteuid():
                    os.fchmod(descriptor, 0o660)
            except BaseException:
                os.close(descriptor)
                raise
            return descriptor
        finally:
            os.close(current)

    @staticmethod
    def _validate_trusted_path(path: Path, *, directory: bool) -> None:
        if os.name == "nt":
            if directory and not path.is_dir():
                raise ValueError("Obsidian sync lock parent is not a directory")
            if not directory and not path.is_file():
                raise ValueError("Obsidian sync lock is not a regular file")
            return
        info = os.lstat(path)
        expected = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
        if not expected:
            raise ValueError("Obsidian sync lock state has an invalid file type")
        if info.st_mode & stat.S_IWOTH:
            raise PermissionError("Obsidian sync lock state is world-writable")
        trusted_groups = set(os.getgroups()) | {os.getegid()}
        if info.st_uid != os.geteuid() and info.st_gid not in trusted_groups:
            raise PermissionError("Obsidian sync lock state has untrusted ownership")

    @classmethod
    def _validate_descriptor(cls, descriptor: int, *, directory: bool) -> None:
        if os.name == "nt":
            return
        info = os.fstat(descriptor)
        expected = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(
            info.st_mode
        )
        if not expected:
            raise ValueError("Obsidian sync lock state has an invalid file type")
        if info.st_mode & stat.S_IWOTH:
            raise PermissionError("Obsidian sync lock state is world-writable")
        trusted_groups = set(os.getgroups()) | {os.getegid()}
        if info.st_uid != os.geteuid() and info.st_gid not in trusted_groups:
            raise PermissionError("Obsidian sync lock state has untrusted ownership")

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

    async def enqueue_stage(
        self,
        trade_date: date,
        phase: str,
        plan_version_ids: Sequence[int] = (),
        review_ids: Sequence[int] = (),
        include_rules: bool = False,
    ) -> tuple[TradingPlaybookObsidianExport, ...]:
        """Freeze and persist one committed business phase as one batch."""

        artifacts = await self.builder.build_stage_artifacts(
            trade_date=trade_date,
            phase=phase,
            plan_version_ids=plan_version_ids,
            review_ids=review_ids,
            include_rules=include_rules,
        )
        return await self.enqueue_artifacts(artifacts)

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
                selected = (
                    await session.execute(
                        select(
                            TradingPlaybookObsidianExport.git_status_json
                        ).where(
                            TradingPlaybookObsidianExport.id == int(row_id),
                            *self._claim_predicates(now=now),
                        )
                    )
                ).first()
                if selected is None:
                    continue
                claimed = await session.execute(
                    self._claim_statement(
                        row_id=int(row_id),
                        now=now,
                        lease_until=lease_until,
                        git_status=self._lease_claimed_intent(
                            lease_until=lease_until,
                            previous_git_status=selected[0],
                        ),
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

    async def _claim_selected(
        self,
        row_ids: Sequence[int],
    ) -> tuple[TradingPlaybookObsidianExport, ...]:
        normalized_ids = tuple(dict.fromkeys(int(row_id) for row_id in row_ids))
        if not normalized_ids:
            return ()
        now = self._database_datetime(self._aware_now())
        lease_until = now + self.CLAIM_LEASE
        claimed_ids: list[int] = []
        async with self.session_factory() as session:
            for row_id in normalized_ids:
                selected = (
                    await session.execute(
                        select(
                            TradingPlaybookObsidianExport.git_status_json
                        ).where(
                            TradingPlaybookObsidianExport.id == row_id,
                            *self._claim_predicates(now=now),
                        )
                    )
                ).first()
                if selected is None:
                    continue
                claimed = await session.execute(
                    self._claim_statement(
                        row_id=row_id,
                        now=now,
                        lease_until=lease_until,
                        git_status=self._lease_claimed_intent(
                            lease_until=lease_until,
                            previous_git_status=selected[0],
                        ),
                    )
                )
                if claimed.rowcount == 1:
                    claimed_ids.append(row_id)
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
        return tuple(by_id[row_id] for row_id in claimed_ids if row_id in by_id)

    async def _load_selected_rows(
        self,
        row_ids: Sequence[int],
    ) -> tuple[TradingPlaybookObsidianExport, ...]:
        normalized_ids = tuple(dict.fromkeys(int(row_id) for row_id in row_ids))
        if not normalized_ids:
            return ()
        async with self.session_factory() as session:
            rows = list(
                (
                    await session.execute(
                        select(TradingPlaybookObsidianExport).where(
                            TradingPlaybookObsidianExport.id.in_(normalized_ids)
                        )
                    )
                ).scalars()
            )
        by_id = {int(row.id): row for row in rows}
        return tuple(by_id[row_id] for row_id in normalized_ids if row_id in by_id)

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
        git_status: dict[str, Any],
    ):
        return (
            update(TradingPlaybookObsidianExport)
            .where(
                TradingPlaybookObsidianExport.id == row_id,
                *cls._claim_predicates(now=now),
            )
            .values(
                next_attempt_at=lease_until,
                git_status_json=git_status,
                updated_at=now,
            )
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
        await self._ensure_persistent_git_intents()
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
            elif outcome == "skipped":
                skipped.append(row.target_path)
            elif outcome == "failed":
                failed.append(row.target_path)
            else:
                pending.append(row.target_path)

        _, git_status = await self._commit_git_intents(
            row_ids=[int(row.id) for row in claimed],
            allowed_paths=None,
            limit=limit,
        )
        return self._result_for_rows(
            claimed,
            written_files=tuple(written),
            skipped_files=tuple(skipped),
            pending_files=tuple(pending),
            failed_files=tuple(failed),
            git_status=git_status,
        )

    async def _process_selected(
        self,
        row_ids: Sequence[int],
        *,
        trade_date: date,
        phase: str = "reconcile",
    ) -> ObsidianSyncBatchResult:
        normalized_ids = tuple(dict.fromkeys(int(row_id) for row_id in row_ids))
        await self._ensure_persistent_git_intents()
        if not await self._writer_available():
            await self._pause_selected_due(normalized_ids)
            rows = await self._load_selected_rows(normalized_ids)
            pending = tuple(
                row.target_path
                for row in rows
                if row.status not in {"written", "superseded"}
            )
            return self._selected_result(
                trade_date=trade_date,
                phase=phase,
                pending_files=pending,
                git_status=self._no_git_status(),
            )

        await self._resume_selected_paused(normalized_ids)
        claimed = await self._claim_selected(normalized_ids)
        written: list[str] = []
        skipped: list[str] = []
        failed: list[str] = []
        pending: list[str] = []
        handled_ids: set[int] = set()
        for row in claimed:
            row_id = int(row.id)
            handled_ids.add(row_id)
            lease_until = row.next_attempt_at
            if not isinstance(lease_until, datetime):
                pending.append(row.target_path)
                continue
            try:
                artifact, generated_at = self._artifact_from_row(row)
                content = self.exporter.render(
                    artifact,
                    generated_at=generated_at,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                marked = await self._mark_failed(
                    row_id=row_id,
                    lease_until=lease_until,
                    attempt_no=int(row.attempt_no),
                    error=exc,
                )
                (failed if marked else pending).append(row.target_path)
                continue
            if not await self._lease_is_active(row_id, lease_until):
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
                    row_id=row_id,
                    lease_until=lease_until,
                    attempt_no=int(row.attempt_no),
                    error=exc,
                )
                (failed if marked else pending).append(row.target_path)
                continue
            if outcome == "written":
                written.append(row.target_path)
            elif outcome == "skipped":
                skipped.append(row.target_path)
            elif outcome == "failed":
                failed.append(row.target_path)
            else:
                pending.append(row.target_path)

        selected_rows = await self._load_selected_rows(normalized_ids)
        for row in selected_rows:
            if int(row.id) in handled_ids:
                continue
            if row.status not in {"written", "superseded"}:
                pending.append(row.target_path)
        _, git_status = await self._commit_git_intents(
            row_ids=normalized_ids,
            allowed_paths=None,
            limit=max(100, len(normalized_ids)),
        )
        return self._selected_result(
            trade_date=trade_date,
            phase=phase,
            written_files=tuple(dict.fromkeys(written)),
            skipped_files=tuple(dict.fromkeys(skipped)),
            pending_files=tuple(dict.fromkeys(pending)),
            failed_files=tuple(dict.fromkeys(failed)),
            git_status=git_status,
        )

    async def _pause_selected_due(self, row_ids: Sequence[int]) -> None:
        if not row_ids:
            return
        now = self._database_datetime(self._aware_now())
        async with self.session_factory() as session:
            for row_id in row_ids:
                await session.execute(
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
            await session.commit()

    async def _resume_selected_paused(self, row_ids: Sequence[int]) -> None:
        if not row_ids:
            return
        now = self._database_datetime(self._aware_now())
        async with self.session_factory() as session:
            await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(
                    TradingPlaybookObsidianExport.id.in_(tuple(row_ids)),
                    TradingPlaybookObsidianExport.status == "paused",
                )
                .values(status="pending", next_attempt_at=None, updated_at=now)
            )
            await session.commit()

    @staticmethod
    def _selected_result(
        *,
        trade_date: date,
        phase: str,
        written_files: tuple[str, ...] = (),
        skipped_files: tuple[str, ...] = (),
        pending_files: tuple[str, ...] = (),
        failed_files: tuple[str, ...] = (),
        git_status: dict[str, Any],
    ) -> ObsidianSyncBatchResult:
        return ObsidianSyncBatchResult(
            trade_date=trade_date,
            phase=phase,
            written_files=written_files,
            skipped_files=skipped_files,
            pending_files=pending_files,
            failed_files=failed_files,
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
        rows = await self.enqueue_artifacts(artifacts)
        row_ids = tuple(int(row.id) for row in rows)
        if force:
            await self._reset_forced_exports(row_ids)
        return await self._process_selected(
            row_ids,
            trade_date=trade_date,
            phase="reconcile",
        )

    async def startup_reconcile(self) -> ObsidianSyncBatchResult:
        """Discover missed committed facts, then resume every due export."""

        rows = await self.reconcile_committed_facts()
        return await self.process_due(limit=max(100, len(rows)))

    async def reconcile_committed_facts(
        self,
    ) -> tuple[TradingPlaybookObsidianExport, ...]:
        """Freeze exports omitted after a business commit or process crash.

        Immutable plan/review/rule facts are rebuilt only when they have no
        persisted export. Mutable daily views are refreshed for the latest
        source and target plan dates on every pass.
        """

        immutable_export = aliased(TradingPlaybookObsidianExport)
        plan_is_exported = exists(
            select(1).where(
                immutable_export.immutable.is_(True),
                immutable_export.entity_type == "plan",
                immutable_export.entity_id == TradingPlanVersion.id,
            )
        )
        initial_review_is_exported = exists(
            select(1).where(
                immutable_export.immutable.is_(True),
                immutable_export.entity_type == "review",
                immutable_export.entity_id == TradingExecutionReview.id,
                immutable_export.phase == "initial_review",
            )
        )
        final_review_is_exported = exists(
            select(1).where(
                immutable_export.immutable.is_(True),
                immutable_export.entity_type == "review",
                immutable_export.entity_id == TradingExecutionReview.id,
                immutable_export.phase == "final_review",
            )
        )
        async with self.session_factory() as session:
            exported_v2_rule_keys = {
                snapshot_key
                for snapshot_key in (
                    await session.scalars(
                        select(TradingPlaybookObsidianExport.snapshot_key).where(
                            TradingPlaybookObsidianExport.immutable.is_(True),
                            TradingPlaybookObsidianExport.entity_type == "rule",
                            TradingPlaybookObsidianExport.snapshot_key.like(
                                "rule:v2:%"
                            ),
                        )
                    )
                ).all()
            }
            missing_plan_ids = tuple(
                int(plan_id)
                for plan_id in (
                    await session.scalars(
                        select(TradingPlanVersion.id)
                        .where(~plan_is_exported)
                        .order_by(TradingPlanVersion.id)
                    )
                ).all()
            )
            review_rows = tuple(
                (
                    int(review_id),
                    "final_review"
                    if finalized_at is not None
                    else "initial_review",
                )
                for review_id, finalized_at in (
                    await session.execute(
                        select(
                            TradingExecutionReview.id,
                            TradingExecutionReview.finalized_at,
                        )
                        .where(
                            or_(
                                (
                                    TradingExecutionReview.finalized_at.is_(None)
                                    & ~initial_review_is_exported
                                ),
                                (
                                    TradingExecutionReview.finalized_at.is_not(None)
                                    & ~final_review_is_exported
                                ),
                            )
                        )
                        .order_by(TradingExecutionReview.id)
                    )
                ).all()
            )

        rows: list[TradingPlaybookObsidianExport] = []

        async def enqueue_one(artifact: ObsidianArtifact, label: str) -> None:
            try:
                rows.extend(await self.enqueue_artifacts((artifact,)))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    "Trading playbook Obsidian compensation enqueue failed "
                    "for {}: {}",
                    label,
                    exc,
                )

        async def build_one(awaitable, label: str) -> None:
            try:
                built = await awaitable
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    "Trading playbook Obsidian compensation build failed "
                    "for {}: {}",
                    label,
                    exc,
                )
                return
            await enqueue_one(built, label)

        if len(exported_v2_rule_keys) < EXPECTED_RULE_COUNT:
            try:
                rules = await self.builder.build_rule_artifacts("v2")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    "Trading playbook Obsidian compensation build failed "
                    "for rules v2: {}",
                    exc,
                )
            else:
                for rule in rules:
                    if rule.snapshot_key not in exported_v2_rule_keys:
                        await enqueue_one(rule, rule.snapshot_key)
        for plan_id in missing_plan_ids:
            await build_one(
                self.builder.build_plan_artifact(plan_id),
                f"plan {plan_id}",
            )
        for review_id, phase in review_rows:
            await build_one(
                self.builder.build_review_artifact(review_id, phase=phase),
                f"review {review_id} {phase}",
            )
        for trade_date in await self._latest_plan_dates():
            mutable_builders = (
                ("alerts", self.builder.build_alerts_artifact),
                ("daily index", self.builder.build_daily_index_artifact),
                ("dashboard", self.builder.build_dashboard_artifact),
            )
            for label, build in mutable_builders:
                await build_one(
                    build(trade_date),
                    f"{label} {trade_date.isoformat()}",
                )
        return tuple(rows)

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
            renewed = await self._renew_lease(
                row_id=int(row.id),
                lease_until=lease_until,
            )
            if renewed is None:
                return "pending"
            renewed_until, previous_git_status = renewed
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
                changed=changed,
                previous_git_status=previous_git_status,
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
    ) -> tuple[datetime, dict[str, Any] | None] | None:
        now = self._database_datetime(self._aware_now())
        renewed_until = now + self.CLAIM_LEASE
        async with self.session_factory() as session:
            selected = (
                await session.execute(
                    select(
                        TradingPlaybookObsidianExport.id,
                        TradingPlaybookObsidianExport.git_status_json,
                    ).where(
                        TradingPlaybookObsidianExport.id == row_id,
                        *self._active_lease_predicates(
                            now=now,
                            lease_until=lease_until,
                        ),
                    )
                )
            ).first()
            if selected is None:
                return None
            previous_git_status = self._active_git_previous(
                selected[1],
                lease_until=lease_until,
                active_states={"lease_claimed"},
            )
            lease_token = self._lease_token(renewed_until)
            write_in_progress = self._fit_git_mapping(
                {
                    "state": "write_in_progress",
                    "lease_token": lease_token,
                    "previous": previous_git_status,
                }
            )
            renewed = await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(
                    TradingPlaybookObsidianExport.id == row_id,
                    *self._active_lease_predicates(
                        now=now,
                        lease_until=lease_until,
                    ),
                )
                .values(
                    next_attempt_at=renewed_until,
                    git_status_json=write_in_progress,
                    updated_at=now,
                )
            )
            await session.commit()
            if renewed.rowcount != 1:
                return None
            return renewed_until, previous_git_status

    @classmethod
    def _lease_claimed_intent(
        cls,
        *,
        lease_until: datetime,
        previous_git_status: object,
    ) -> dict[str, Any]:
        previous = cls._bounded_git_mapping(previous_git_status)
        if previous is not None and previous.get("state") == "lease_claimed":
            previous = cls._bounded_git_mapping(previous.get("previous"))
        return cls._fit_git_mapping(
            {
                "state": "lease_claimed",
                "lease_token": cls._lease_token(lease_until),
                "previous": previous,
            }
        )

    @classmethod
    def _active_git_previous(
        cls,
        status: object,
        *,
        lease_until: datetime,
        active_states: set[str],
    ) -> dict[str, Any] | None:
        bounded = cls._bounded_git_mapping(status)
        if (
            bounded is not None
            and bounded.get("state") in active_states
            and bounded.get("lease_token") == cls._lease_token(lease_until)
        ):
            return cls._bounded_git_mapping(bounded.get("previous"))
        return bounded

    @classmethod
    def _lease_token(cls, lease_until: datetime) -> str:
        aware = lease_until
        if aware.tzinfo is None or aware.utcoffset() is None:
            aware = CN_TZ.localize(aware)
        return cls._canonical_datetime(aware)

    async def _mark_written(
        self,
        *,
        row_id: int,
        lease_until: datetime,
        changed: bool,
        previous_git_status: dict[str, Any] | None,
    ) -> bool:
        completed_at = self._database_datetime(self._aware_now())
        git_intent = self._written_git_intent(
            changed=changed,
            previous_git_status=previous_git_status,
        )
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
                    git_status_json=git_intent,
                    exported_at=completed_at,
                    updated_at=completed_at,
                )
            )
            await session.commit()
            return completed.rowcount == 1

    async def _acquire_target_lock(self, target_path: str) -> _TargetFileLock:
        return await self._acquire_named_lock(f"target:{target_path}")

    async def _acquire_named_lock(self, identity: str) -> _TargetFileLock:
        relative_path = self._lock_relative_path(identity)
        task = asyncio.create_task(
            asyncio.to_thread(
                _TargetFileLock.acquire,
                self.writer,
                relative_path,
            )
        )
        result, error, cancellation = await self._drain_fenced_task(task)
        if cancellation is not None:
            if isinstance(result, _TargetFileLock):
                release_task = asyncio.create_task(
                    asyncio.to_thread(result.release)
                )
                await self._drain_fenced_task(
                    release_task,
                    cancellation=cancellation,
                )
            raise cancellation
        if error is not None:
            raise error
        if not isinstance(result, _TargetFileLock):
            raise TypeError("Obsidian lock acquisition returned an invalid value")
        return result

    @staticmethod
    def _lock_relative_path(identity: str) -> str:
        normalized = identity.replace("\\", "/").casefold().encode("utf-8")
        digest = hashlib.sha256(normalized).hexdigest()
        return f"{_LOCK_ROOT}/{digest}.lock"

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
        selected = (
            await session.execute(
                select(
                    TradingPlaybookObsidianExport.git_status_json
                ).where(
                    TradingPlaybookObsidianExport.id == row_id,
                    *self._active_lease_predicates(
                        now=now,
                        lease_until=lease_until,
                    ),
                )
            )
        ).first()
        if selected is None:
            return False
        safe_error = self._safe_error(error)
        git_status = self._fit_git_mapping(
            {
                "state": "write_failed",
                "error": safe_error,
                "previous": self._active_git_previous(
                    selected[0],
                    lease_until=lease_until,
                    active_states={"lease_claimed", "write_in_progress"},
                ),
            }
        )
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
                last_error=safe_error,
                git_status_json=git_status,
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
            return self._normalize_git_result(raw_status)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return self._normalize_git_result(
                {
                    "enabled": bool(
                        getattr(self.writer, "auto_git_enabled", False)
                    ),
                    "committed": False,
                    "error": self._safe_error(exc),
                }
            )

    async def _store_git_status(
        self,
        row_ids: Sequence[int],
        status: dict[str, Any],
    ) -> None:
        unique_ids = tuple(dict.fromkeys(int(row_id) for row_id in row_ids))
        if not unique_ids:
            return
        bounded_status = self._bounded_git_mapping(status)
        if bounded_status is None:
            bounded_status = {
                "state": "git_error",
                "error": "empty Git status",
            }
        now = self._database_datetime(self._aware_now())
        async with self.session_factory() as session:
            await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(
                    TradingPlaybookObsidianExport.id.in_(unique_ids),
                    TradingPlaybookObsidianExport.status == "written",
                )
                .values(git_status_json=bounded_status, updated_at=now)
            )
            await session.commit()

    async def _ensure_persistent_git_intents(self) -> None:
        now = self._database_datetime(self._aware_now())
        async with self.session_factory() as session:
            await session.execute(
                update(TradingPlaybookObsidianExport)
                .where(
                    TradingPlaybookObsidianExport.status == "written",
                    TradingPlaybookObsidianExport.git_status_json.is_(None),
                )
                .values(
                    git_status_json={
                        "state": "git_pending",
                        "reason": "legacy_write_uncertain",
                    },
                    updated_at=now,
                )
            )
            await session.commit()

    async def _load_git_intent_rows(
        self,
        *,
        row_ids: Sequence[int] | None,
        allowed_paths: tuple[str, ...] | None,
        limit: int,
    ) -> tuple[TradingPlaybookObsidianExport, ...]:
        async with self.session_factory() as session:
            statement = (
                select(TradingPlaybookObsidianExport)
                .where(TradingPlaybookObsidianExport.status == "written")
                .order_by(
                    TradingPlaybookObsidianExport.trade_date,
                    TradingPlaybookObsidianExport.id,
                )
            )
            if row_ids is not None:
                normalized_ids = tuple(dict.fromkeys(int(item) for item in row_ids))
                if not normalized_ids:
                    return ()
                statement = statement.where(
                    TradingPlaybookObsidianExport.id.in_(normalized_ids)
                )
            rows = list((await session.execute(statement)).scalars())
        allowed = set(allowed_paths) if allowed_paths is not None else None
        selected = [
            row
            for row in rows
            if self._git_needs_retry(row.git_status_json)
            and (allowed is None or row.target_path in allowed)
        ]
        return tuple(selected[:limit])

    async def _commit_git_intents(
        self,
        *,
        row_ids: Sequence[int] | None,
        allowed_paths: tuple[str, ...] | None,
        limit: int,
    ) -> tuple[tuple[TradingPlaybookObsidianExport, ...], dict[str, Any]]:
        target_lock: _TargetFileLock | None = None
        try:
            target_lock = await self._acquire_named_lock("git:vault")
            rows = await self._load_git_intent_rows(
                row_ids=row_ids,
                allowed_paths=allowed_paths,
                limit=limit,
            )
            if not rows:
                return (), self._no_git_status()
            paths = tuple(dict.fromkeys(row.target_path for row in rows))
            trade_date, phase = self._batch_context(rows)
            status = await self._commit_paths(
                paths,
                trade_date=trade_date,
                phase=phase,
            )
            try:
                await self._store_git_status(
                    [int(row.id) for row in rows],
                    status,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                return rows, self._fit_git_mapping(
                    {
                        "state": "git_store_pending",
                        "error": self._safe_error(exc),
                        "result": status,
                    }
                )
            return rows, status
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            rows = await self._load_git_intent_rows(
                row_ids=row_ids,
                allowed_paths=allowed_paths,
                limit=limit,
            )
            status = self._normalize_git_result(
                {
                    "enabled": bool(
                        getattr(self.writer, "auto_git_enabled", False)
                    ),
                    "committed": False,
                    "error": self._safe_error(exc),
                }
            )
            if rows:
                try:
                    await self._store_git_status(
                        [int(row.id) for row in rows],
                        status,
                    )
                except Exception:
                    pass
            return rows, status
        finally:
            if target_lock is not None:
                try:
                    await self._to_thread_fenced(target_lock.release)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass

    async def _retry_failed_git(self, *, limit: int) -> ObsidianSyncBatchResult:
        retry_rows, status = await self._commit_git_intents(
            row_ids=None,
            allowed_paths=None,
            limit=limit,
        )
        if not retry_rows:
            return self._empty_result()
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

    async def _reset_forced_exports(self, row_ids: Sequence[int]) -> None:
        now = self._database_datetime(self._aware_now())
        rows = await self._load_selected_rows(row_ids)
        async with self.session_factory() as session:
            for row in rows:
                status = row.status
                git_state = (
                    row.git_status_json.get("state")
                    if type(row.git_status_json) is dict
                    else None
                )
                has_future_token = (
                    isinstance(row.next_attempt_at, datetime)
                    and row.next_attempt_at > now
                )
                marker_state = git_state in {
                    "lease_claimed",
                    "write_in_progress",
                }
                marker_matches = bool(
                    marker_state
                    and isinstance(row.next_attempt_at, datetime)
                    and type(row.git_status_json) is dict
                    and row.git_status_json.get("lease_token")
                    == self._lease_token(row.next_attempt_at)
                )
                is_legacy_pending_lease = bool(
                    has_future_token
                    and status == "pending"
                    and not marker_state
                )
                is_active = has_future_token and (
                    marker_matches or is_legacy_pending_lease
                )
                if is_active or status == "superseded":
                    continue
                if row.last_error == _IMMUTABLE_CONFLICT:
                    continue
                token_predicate = (
                    TradingPlaybookObsidianExport.next_attempt_at.is_(None)
                    if row.next_attempt_at is None
                    else TradingPlaybookObsidianExport.next_attempt_at
                    == row.next_attempt_at
                )
                values: dict[str, Any] = {
                    "git_status_json": {
                        "state": "git_pending",
                        "reason": "force_requested",
                    },
                    "updated_at": now,
                }
                if status in {"failed", "paused", "pending"}:
                    values.update(
                        status="pending",
                        attempt_no=0,
                        next_attempt_at=None,
                        last_error=None,
                    )
                await session.execute(
                    update(TradingPlaybookObsidianExport)
                    .where(
                        TradingPlaybookObsidianExport.id == int(row.id),
                        TradingPlaybookObsidianExport.status == status,
                        token_predicate,
                    )
                    .values(**values)
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
        result, error, cancellation = await self._drain_fenced_task(task)
        if cancellation is not None:
            raise cancellation
        if error is not None:
            raise error
        return result

    @staticmethod
    async def _drain_fenced_task(
        task: asyncio.Task,
        *,
        cancellation: asyncio.CancelledError | None = None,
    ) -> tuple[Any, BaseException | None, asyncio.CancelledError | None]:
        """Drain an uninterruptible resource task under repeated cancellation."""

        first_cancellation = cancellation
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError as exc:
                if first_cancellation is None:
                    first_cancellation = exc
            except BaseException:
                break
        try:
            return task.result(), None, first_cancellation
        except BaseException as exc:
            return None, exc, first_cancellation

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
            "state": "not_attempted",
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
        return TradingPlaybookObsidianSyncCoordinator._safe_text(
            text_value,
            limit=_MAX_ERROR_LENGTH,
        )

    @staticmethod
    def _safe_text(value: str, *, limit: int) -> str:
        safe_characters: list[str] = []
        for character in value:
            if character in "\r\n\t":
                safe_characters.append(" ")
            elif unicodedata.category(character) in {"Cc", "Cs"}:
                safe_characters.append("�")
            else:
                safe_characters.append(character)
        safe_text = "".join(safe_characters)
        return safe_text[:limit]

    @classmethod
    def _bounded_git_mapping(
        cls,
        status: object,
    ) -> dict[str, Any] | None:
        if status is None:
            return None
        if type(status) is not dict:
            return {
                "state": "git_error",
                "error": "invalid stored Git status",
            }
        bounded = cls._bounded_json_value(status, depth=0)
        assert type(bounded) is dict
        return cls._fit_git_mapping(bounded)

    @classmethod
    def _fit_git_mapping(cls, status: dict[str, Any]) -> dict[str, Any]:
        encoded = json.dumps(
            status,
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        if len(encoded) <= _MAX_GIT_JSON_BYTES:
            return status
        state = status.get("state")
        overflow = {
            "state": "git_error",
            "error": "Git status exceeded the 32768-byte storage limit",
            "retryable": True,
        }
        if state in {"lease_claimed", "write_in_progress"}:
            return {
                "state": state,
                "lease_token": cls._safe_text(
                    str(status.get("lease_token", "")),
                    limit=_MAX_GIT_JSON_STRING,
                ),
                "previous": overflow,
            }
        if state == "write_failed":
            return {
                "state": "write_failed",
                "error": cls._safe_text(
                    str(status.get("error", overflow["error"])),
                    limit=_MAX_ERROR_LENGTH,
                ),
                "previous": overflow,
            }
        return {
            **overflow,
            "enabled": bool(status.get("enabled", True)),
            "committed": False,
        }

    @classmethod
    def _bounded_json_value(cls, value: object, *, depth: int) -> Any:
        if value is None or type(value) is bool:
            return value
        if type(value) is int:
            return value if value.bit_length() <= 63 else "integer_out_of_range"
        if type(value) is float:
            if not math.isfinite(value):
                return "nonfinite_number"
            return value
        if type(value) is str:
            return cls._safe_text(value, limit=_MAX_GIT_JSON_STRING)
        if depth >= _MAX_GIT_JSON_DEPTH:
            return "truncated_depth"
        if type(value) is dict:
            bounded: dict[str, Any] = {}
            for index, (key, item) in enumerate(value.items()):
                if index >= _MAX_GIT_JSON_ITEMS:
                    bounded["_truncated_items"] = True
                    break
                raw_key = key if type(key) is str else type(key).__name__
                safe_key = cls._safe_text(raw_key, limit=128)
                bounded[safe_key] = cls._bounded_json_value(
                    item,
                    depth=depth + 1,
                )
            return bounded
        if type(value) in (list, tuple):
            items = [
                cls._bounded_json_value(item, depth=depth + 1)
                for item in value[:_MAX_GIT_JSON_ITEMS]
            ]
            if len(value) > _MAX_GIT_JSON_ITEMS:
                items.append("truncated_items")
            return items
        return cls._safe_text(type(value).__name__, limit=128)

    @classmethod
    def _normalize_git_result(cls, status: object) -> dict[str, Any]:
        bounded = cls._bounded_git_mapping(status)
        if bounded is None:
            bounded = {"error": "empty Git status"}
        raw_error = bounded.get("error")
        if raw_error:
            return cls._fit_git_mapping(
                {
                    "state": "git_error",
                    "enabled": bool(bounded.get("enabled", True)),
                    "committed": False,
                    "error": cls._safe_text(
                        str(raw_error),
                        limit=_MAX_ERROR_LENGTH,
                    ),
                    "result": bounded,
                }
            )
        return cls._fit_git_mapping(
            {
                "state": "git_complete",
                "enabled": bool(bounded.get("enabled", False)),
                "committed": bool(bounded.get("committed", False)),
                "result": bounded,
            }
        )

    @staticmethod
    def _git_needs_retry(status: object) -> bool:
        if status is None:
            return True
        if type(status) is not dict:
            return True
        return status.get("state") in {
            "git_pending",
            "git_error",
            "write_in_progress",
            "write_failed",
        } or bool(status.get("error"))

    @classmethod
    def _written_git_intent(
        cls,
        *,
        changed: bool,
        previous_git_status: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if changed:
            return {"state": "git_pending", "reason": "content_changed"}
        if previous_git_status is None:
            return {"state": "not_needed", "reason": "content_identical"}
        if cls._git_needs_retry(previous_git_status):
            return {
                "state": "git_pending",
                "reason": "previous_write_uncertain",
            }
        if previous_git_status is not None:
            return previous_git_status
        raise AssertionError("unreachable Git intent state")

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
