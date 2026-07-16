"""Atomic cross-process claims for independently retryable playbook phases."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

from sqlalchemy import or_, select, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.models.trading_playbook import TradingPlaybookJobClaim


@dataclass(frozen=True)
class TradingPlaybookClaimToken:
    job_key: str
    owner: str
    attempt_no: int


class TradingPlaybookJobClaimService:
    """Acquire, complete, or release a phase lease using database DML only."""

    def __init__(self, *, lease_seconds: int = 300) -> None:
        self.lease_seconds = max(int(lease_seconds), 1)

    async def claim(
        self,
        db,
        *,
        job_key: str,
        job_type: str,
        phase: str,
        owner: str,
        now: datetime,
        source_trade_date: Optional[date] = None,
        target_trade_date: Optional[date] = None,
        stage: Optional[str] = None,
        generation_key: Optional[str] = None,
    ) -> Optional[TradingPlaybookClaimToken]:
        values = {
            "job_key": self._required(job_key, "job_key", 255),
            "job_type": self._required(job_type, "job_type", 40),
            "phase": self._required(phase, "phase", 40),
            "source_trade_date": source_trade_date,
            "target_trade_date": target_trade_date,
            "stage": stage,
            "generation_key": generation_key,
            "owner": self._required(owner, "owner", 80),
            "status": "running",
            "attempt_no": 1,
            "lease_expires_at": now + timedelta(seconds=self.lease_seconds),
            "completed_at": None,
            "last_error": None,
            "created_at": now,
            "updated_at": now,
        }
        dialect = db.get_bind().dialect.name
        if dialect == "sqlite":
            statement = sqlite_insert(TradingPlaybookJobClaim).values(**values)
        elif dialect == "postgresql":
            statement = postgresql_insert(TradingPlaybookJobClaim).values(**values)
        else:
            raise RuntimeError(f"unsupported playbook claim dialect: {dialect}")
        statement = statement.on_conflict_do_nothing(index_elements=["job_key"])
        inserted = await db.execute(statement)
        if inserted.rowcount == 1:
            await db.commit()
            return TradingPlaybookClaimToken(values["job_key"], values["owner"], 1)

        takeover = await db.execute(
            update(TradingPlaybookJobClaim)
            .where(
                TradingPlaybookJobClaim.job_key == values["job_key"],
                TradingPlaybookJobClaim.status != "completed",
                or_(
                    TradingPlaybookJobClaim.lease_expires_at.is_(None),
                    TradingPlaybookJobClaim.lease_expires_at <= now,
                ),
            )
            .values(
                owner=values["owner"],
                status="running",
                attempt_no=TradingPlaybookJobClaim.attempt_no + 1,
                lease_expires_at=values["lease_expires_at"],
                last_error=None,
                updated_at=now,
            )
        )
        if takeover.rowcount != 1:
            await db.rollback()
            return None
        await db.commit()
        row = (
            await db.execute(
                select(TradingPlaybookJobClaim).where(
                    TradingPlaybookJobClaim.job_key == values["job_key"]
                )
            )
        ).scalar_one()
        return TradingPlaybookClaimToken(row.job_key, row.owner, row.attempt_no)

    async def get_status(self, db, job_key: str) -> Optional[str]:
        """Return the persisted claim status without changing claim state."""

        return await db.scalar(
            select(TradingPlaybookJobClaim.status).where(
                TradingPlaybookJobClaim.job_key
                == self._required(job_key, "job_key", 255)
            )
        )

    async def complete(
        self,
        db,
        token: TradingPlaybookClaimToken,
        *,
        now: datetime,
    ) -> bool:
        result = await db.execute(
            update(TradingPlaybookJobClaim)
            .where(*self._token_predicates(token))
            .values(
                status="completed",
                completed_at=now,
                lease_expires_at=None,
                last_error=None,
                updated_at=now,
            )
        )
        await db.commit()
        return result.rowcount == 1

    async def renew(
        self,
        db,
        token: TradingPlaybookClaimToken,
        *,
        now: datetime,
    ) -> bool:
        """Extend only a still-live lease owned by this exact attempt."""
        result = await db.execute(
            update(TradingPlaybookJobClaim)
            .where(
                *self._token_predicates(token),
                TradingPlaybookJobClaim.lease_expires_at > now,
            )
            .values(
                lease_expires_at=now + timedelta(seconds=self.lease_seconds),
                updated_at=now,
            )
        )
        await db.commit()
        return result.rowcount == 1

    async def fail(
        self,
        db,
        token: TradingPlaybookClaimToken,
        *,
        now: datetime,
        error: Exception | str,
    ) -> bool:
        result = await db.execute(
            update(TradingPlaybookJobClaim)
            .where(*self._token_predicates(token))
            .values(
                status="retry",
                lease_expires_at=now,
                last_error=str(error)[:2000],
                updated_at=now,
            )
        )
        await db.commit()
        return result.rowcount == 1

    @staticmethod
    def _token_predicates(token: TradingPlaybookClaimToken):
        return (
            TradingPlaybookJobClaim.job_key == token.job_key,
            TradingPlaybookJobClaim.owner == token.owner,
            TradingPlaybookJobClaim.attempt_no == token.attempt_no,
            TradingPlaybookJobClaim.status == "running",
        )

    @staticmethod
    def _required(value: str, name: str, limit: int) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError(f"{name} is required")
        if len(normalized) > limit:
            raise ValueError(f"{name} exceeds {limit} characters")
        return normalized


__all__ = [
    "TradingPlaybookClaimToken",
    "TradingPlaybookJobClaimService",
]
