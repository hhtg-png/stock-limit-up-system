"""Immutable daily trading-plan version generation and confirmation."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import math
import re
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError, OperationalError

from app.config import settings as app_settings
from app.models.trading_playbook import (
    TradingPlanCandidate,
    TradingPlanVersion,
    TradingPlaybookSettings,
)

from .domain import MarketSnapshot, ModeEvaluation


CHINA_TZ = ZoneInfo("Asia/Shanghai")
_PLAN_STAGES = {"preclose", "after_close", "overnight", "auction"}
_EVALUATION_STATUSES = {"matched", "waiting", "manual_review", "not_matched"}
_RISK_LEVELS = {"avoid", "watch", "trial", "confirmed"}
_ACTION_SCOPES = {"target", "tail"}
_RULE_HASH = re.compile(r"[0-9a-f]{64}")
_STAGE_SEQUENCE = ("preclose", "after_close", "overnight", "auction")


def _finite_number(value: Any) -> Optional[float]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _json_safe(value: Any, *, path: str = "value") -> Any:
    """Return a strict, detached JSON value without silently losing evidence."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} must not contain NaN or Infinity")
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        result: Dict[str, Any] = {}
        keys = list(value)
        if any(not isinstance(key, str) for key in keys):
            raise ValueError(f"{path} contains a non-string object key")
        for key in sorted(keys):
            result[key] = _json_safe(value[key], path=f"{path}.{key}")
        return result
    if isinstance(value, (list, tuple)):
        return [
            _json_safe(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    raise ValueError(f"{path} contains a non-JSON value: {type(value).__name__}")


def _canonical_json(value: Any) -> str:
    return json.dumps(
        _json_safe(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _now_cn() -> datetime:
    return datetime.now(CHINA_TZ)


def _china_iso(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        value = value.replace(tzinfo=CHINA_TZ)
    else:
        value = value.astimezone(CHINA_TZ)
    return value.isoformat()


class TradingPlanService:
    """Create immutable plan versions and controlled child revisions."""

    _generation_locks: Dict[Tuple[int, date, str], asyncio.Lock] = {}

    async def generate(
        self,
        db,
        snapshot: MarketSnapshot,
        evaluations: Iterable[ModeEvaluation],
        stock_names: Mapping[str, str],
        rule_snapshot: Optional[Sequence[Mapping[str, Any]]] = None,
    ) -> Dict[str, Any]:
        snapshot_payload, candidate_sources = self._snapshot_payload(snapshot)
        radar = self._normalize_radar(evaluations)
        normalized_rules = self._normalize_rule_snapshot(rule_snapshot, radar)
        normalized_names = self._stock_names(stock_names, radar)
        self._validate_rule_coverage(normalized_rules, radar)

        lock_key = (id(db.bind), snapshot.target_trade_date, snapshot.stage)
        lock = self._generation_locks.setdefault(lock_key, asyncio.Lock())
        async with lock:
            for attempt in range(3):
                created_settings = False
                risk_settings = None
                try:
                    settings_row, created_settings = await self._get_or_create_settings(
                        db
                    )
                    risk_settings = self._risk_settings(settings_row)
                    input_hash = self._input_hash(
                        snapshot_payload,
                        radar,
                        normalized_rules,
                        risk_settings,
                        normalized_names,
                    )
                    existing = await self._find_same_input(
                        db,
                        snapshot.target_trade_date,
                        snapshot.stage,
                        input_hash,
                    )
                    if existing is not None:
                        if created_settings:
                            await db.commit()
                        return await self.serialize(db, existing)

                    parent = await self._latest_parent_plan(
                        db,
                        snapshot.target_trade_date,
                        snapshot.stage,
                    )
                    selected = self._select_candidates(
                        snapshot,
                        radar,
                        risk_settings,
                        candidate_sources,
                    )
                    version_no = await self._next_version_no(
                        db,
                        snapshot.target_trade_date,
                        snapshot.stage,
                    )
                    plan = TradingPlanVersion(
                        source_trade_date=snapshot.source_trade_date,
                        target_trade_date=snapshot.target_trade_date,
                        stage=snapshot.stage,
                        version_no=version_no,
                        parent_plan_version_id=parent.id if parent else None,
                        status="draft",
                        market_state_json=copy.deepcopy(
                            snapshot_payload["market_features"]
                        ),
                        theme_ranking_json=copy.deepcopy(
                            snapshot_payload["theme_rankings"]
                        ),
                        mode_radar_json=copy.deepcopy(radar),
                        rule_snapshot_json=copy.deepcopy(normalized_rules),
                        risk_settings_json=copy.deepcopy(risk_settings),
                        data_quality_json=copy.deepcopy(snapshot_payload["quality"]),
                        change_summary_json=await self._change_summary(
                            db,
                            snapshot.target_trade_date,
                            radar,
                        ),
                        input_hash=input_hash,
                        generated_at=_now_cn(),
                    )
                    db.add(plan)
                    await db.flush()
                    for rank, (primary, supporting) in enumerate(selected, start=1):
                        db.add(
                            self._candidate_from_evaluation(
                                plan.id,
                                snapshot,
                                primary,
                                supporting,
                                normalized_names,
                                candidate_sources,
                                rank,
                                risk_settings,
                            )
                        )
                    await db.commit()
                    return await self.serialize(db, plan)
                except IntegrityError:
                    await db.rollback()
                    if risk_settings is None:
                        if attempt == 2:
                            raise RuntimeError(
                                "could not create singleton playbook settings"
                            )
                        continue
                    existing = await self._find_same_input(
                        db,
                        snapshot.target_trade_date,
                        snapshot.stage,
                        self._input_hash(
                            snapshot_payload,
                            radar,
                            normalized_rules,
                            risk_settings,
                            normalized_names,
                        ),
                    )
                    if existing is not None:
                        return await self.serialize(db, existing)
                    if attempt == 2:
                        raise RuntimeError("could not allocate a unique plan version")
                except Exception:
                    await db.rollback()
                    raise
        raise RuntimeError("could not generate plan")

    @staticmethod
    def _snapshot_payload(
        snapshot: MarketSnapshot,
    ) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
        if not isinstance(snapshot, MarketSnapshot):
            raise ValueError("snapshot must be a MarketSnapshot")
        if snapshot.stage not in _PLAN_STAGES:
            raise ValueError("invalid plan stage")
        if not isinstance(snapshot.source_trade_date, date) or not isinstance(
            snapshot.target_trade_date, date
        ):
            raise ValueError("snapshot trade dates are required")
        if not isinstance(snapshot.as_of, datetime):
            raise ValueError("snapshot as_of must be a datetime")
        if not isinstance(snapshot.quality.as_of, datetime):
            raise ValueError("snapshot quality as_of must be a datetime")

        candidate_rows = []
        candidate_sources: Dict[str, Dict[str, Any]] = {}
        for candidate in snapshot.candidates:
            code = str(candidate.stock_code or "").strip()
            if not code or code in candidate_sources:
                raise ValueError("snapshot candidates require unique stock_code")
            row = {
                "stock_code": code,
                "stock_name": str(candidate.stock_name or ""),
                "theme_name": str(candidate.theme_name or ""),
                "features": _json_safe(
                    candidate.features,
                    path=f"snapshot.candidates.{code}.features",
                ),
                "evidence": _json_safe(
                    candidate.evidence,
                    path=f"snapshot.candidates.{code}.evidence",
                ),
            }
            candidate_rows.append(row)
            candidate_sources[code] = row
        candidate_rows.sort(key=lambda item: item["stock_code"])

        theme_rows = _json_safe(
            snapshot.theme_rankings,
            path="snapshot.theme_rankings",
        )
        if not isinstance(theme_rows, list):
            raise ValueError("snapshot theme rankings must be a list")
        for row in theme_rows:
            if not isinstance(row, dict):
                raise ValueError("snapshot theme ranking rows must be mappings")
            rank = row.get("rank")
            if rank is not None and (
                isinstance(rank, bool)
                or not isinstance(rank, int)
                or rank < 1
            ):
                raise ValueError("snapshot theme rank must be a positive integer")
            if "theme_name" in row and not isinstance(row["theme_name"], str):
                raise ValueError("snapshot theme_name must be text")
        theme_rows.sort(
            key=lambda item: (
                item.get("rank") is None if isinstance(item, dict) else True,
                item.get("rank") or 0 if isinstance(item, dict) else 0,
                str(item.get("theme_name") or "") if isinstance(item, dict) else "",
                _canonical_json(item),
            )
        )
        payload = {
            "source_trade_date": snapshot.source_trade_date.isoformat(),
            "target_trade_date": snapshot.target_trade_date.isoformat(),
            "stage": snapshot.stage,
            "as_of": snapshot.as_of.isoformat(),
            "market_features": _json_safe(
                snapshot.market_features,
                path="snapshot.market_features",
            ),
            "candidates": candidate_rows,
            "theme_rankings": theme_rows,
            "quality": {
                "status": str(snapshot.quality.status or ""),
                "as_of": snapshot.quality.as_of.isoformat(),
                "source": str(snapshot.quality.source or ""),
                "stale": bool(snapshot.quality.stale),
                "warnings": _json_safe(
                    snapshot.quality.warnings,
                    path="snapshot.quality.warnings",
                ),
            },
        }
        _canonical_json(payload)
        return payload, candidate_sources

    @classmethod
    def _normalize_radar(
        cls,
        evaluations: Iterable[ModeEvaluation],
    ) -> List[Dict[str, Any]]:
        rows = []
        for evaluation in list(evaluations):
            if not isinstance(evaluation, ModeEvaluation):
                raise ValueError("evaluations must contain ModeEvaluation rows")
            mode_key = str(evaluation.mode_key or "").strip()
            stock_code = str(evaluation.stock_code or "").strip()
            score = _finite_number(evaluation.score)
            if not mode_key or not stock_code or score is None:
                raise ValueError("evaluation identity and finite score are required")
            if evaluation.status not in _EVALUATION_STATUSES:
                raise ValueError("invalid evaluation status")
            if evaluation.risk_level not in _RISK_LEVELS:
                raise ValueError("invalid evaluation risk level")
            if evaluation.action_scope not in _ACTION_SCOPES:
                raise ValueError("invalid action scope")
            if (
                isinstance(evaluation.rule_version, bool)
                or not isinstance(evaluation.rule_version, int)
                or evaluation.rule_version < 1
            ):
                raise ValueError("evaluation rule_version must be positive")
            if _RULE_HASH.fullmatch(str(evaluation.rule_hash or "")) is None:
                raise ValueError("evaluation rule_hash must be a sha256 hash")
            row = {
                "mode_key": mode_key,
                "stock_code": stock_code,
                "status": evaluation.status,
                "score": score,
                "role": str(evaluation.role or ""),
                "risk_level": evaluation.risk_level,
                "entry_trigger": _json_safe(
                    evaluation.entry_trigger,
                    path=f"radar.{mode_key}.{stock_code}.entry_trigger",
                ),
                "invalidation": _json_safe(
                    evaluation.invalidation,
                    path=f"radar.{mode_key}.{stock_code}.invalidation",
                ),
                "exit_trigger": _json_safe(
                    evaluation.exit_trigger,
                    path=f"radar.{mode_key}.{stock_code}.exit_trigger",
                ),
                "evidence": _json_safe(
                    evaluation.evidence,
                    path=f"radar.{mode_key}.{stock_code}.evidence",
                ),
                "rule_version": evaluation.rule_version,
                "rule_hash": evaluation.rule_hash,
                "action_scope": evaluation.action_scope,
            }
            rows.append(row)
        rows.sort(
            key=lambda item: (
                -item["score"],
                item["stock_code"],
                item["mode_key"],
                item["status"],
            )
        )
        return rows

    @staticmethod
    def _normalize_rule_snapshot(
        rule_snapshot: Optional[Sequence[Mapping[str, Any]]],
        radar: Sequence[Mapping[str, Any]],
    ) -> List[Dict[str, Any]]:
        if rule_snapshot is None:
            derived: Dict[str, Dict[str, Any]] = {}
            for evaluation in radar:
                row = {
                    "mode_key": evaluation["mode_key"],
                    "version": evaluation["rule_version"],
                    "content_hash": evaluation["rule_hash"],
                }
                previous = derived.get(row["mode_key"])
                if previous is not None and previous != row:
                    raise ValueError("conflicting rule metadata in evaluations")
                derived[row["mode_key"]] = row
            rule_snapshot = list(derived.values())
        if not isinstance(rule_snapshot, (list, tuple)) or not rule_snapshot:
            raise ValueError("complete rule_snapshot is required")

        normalized = []
        seen = set()
        for index, source in enumerate(rule_snapshot):
            if not isinstance(source, Mapping):
                raise ValueError("rule_snapshot rows must be mappings")
            row = _json_safe(source, path=f"rule_snapshot[{index}]")
            mode_key = row.get("mode_key")
            version = row.get("version")
            content_hash = row.get("content_hash")
            if not isinstance(mode_key, str) or not mode_key.strip():
                raise ValueError("rule_snapshot mode_key is required")
            if (
                isinstance(version, bool)
                or not isinstance(version, int)
                or version < 1
            ):
                raise ValueError("rule_snapshot version must be positive")
            if (
                not isinstance(content_hash, str)
                or _RULE_HASH.fullmatch(content_hash) is None
            ):
                raise ValueError("rule_snapshot content_hash must be sha256")
            mode_key = mode_key.strip()
            if mode_key in seen:
                raise ValueError(f"duplicate rule_snapshot mode_key: {mode_key}")
            seen.add(mode_key)
            row["mode_key"] = mode_key
            normalized.append(row)
        normalized.sort(
            key=lambda item: (
                item["mode_key"],
                item["version"],
                item["content_hash"],
            )
        )
        _canonical_json(normalized)
        return normalized

    @staticmethod
    def _validate_rule_coverage(
        rule_snapshot: Sequence[Mapping[str, Any]],
        radar: Sequence[Mapping[str, Any]],
    ) -> None:
        by_mode = {row["mode_key"]: row for row in rule_snapshot}
        for evaluation in radar:
            rule = by_mode.get(evaluation["mode_key"])
            if rule is None:
                raise ValueError(
                    f"rule_snapshot missing mode: {evaluation['mode_key']}"
                )
            if (
                rule["version"] != evaluation["rule_version"]
                or rule["content_hash"] != evaluation["rule_hash"]
            ):
                raise ValueError(
                    f"rule_snapshot mismatch for mode: {evaluation['mode_key']}"
                )

    @staticmethod
    def _stock_names(
        stock_names: Mapping[str, str],
        radar: Sequence[Mapping[str, Any]],
    ) -> Dict[str, str]:
        if not isinstance(stock_names, Mapping):
            raise ValueError("stock_names must be a mapping")
        relevant = {row["stock_code"] for row in radar}
        result = {}
        for code in sorted(relevant):
            name = stock_names.get(code, "")
            if name is not None and not isinstance(name, str):
                raise ValueError("stock name must be text")
            result[code] = str(name or "").strip()
        return result

    @staticmethod
    def _input_hash(
        snapshot_payload: Mapping[str, Any],
        radar: Sequence[Mapping[str, Any]],
        rule_snapshot: Sequence[Mapping[str, Any]],
        risk_settings: Mapping[str, Any],
        stock_names: Mapping[str, str],
    ) -> str:
        canonical = _canonical_json(
            {
                "snapshot": snapshot_payload,
                "mode_radar": radar,
                "rule_snapshot": rule_snapshot,
                "risk_settings": risk_settings,
                "stock_names": stock_names,
            }
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @staticmethod
    async def _get_or_create_settings(db) -> Tuple[TradingPlaybookSettings, bool]:
        row = await db.get(TradingPlaybookSettings, 1)
        if row is not None:
            return row, False
        row = TradingPlaybookSettings(
            id=1,
            enabled=bool(app_settings.TRADING_PLAYBOOK_ENABLED),
            trial_position_pct=app_settings.TRADING_PLAYBOOK_TRIAL_POSITION_PCT,
            confirmed_position_pct=(
                app_settings.TRADING_PLAYBOOK_CONFIRMED_POSITION_PCT
            ),
            hard_stop_pct=app_settings.TRADING_PLAYBOOK_HARD_STOP_PCT,
            max_action_candidates=(
                app_settings.TRADING_PLAYBOOK_MAX_ACTION_CANDIDATES
            ),
            in_app_enabled=True,
            wechat_enabled=False,
            channel_config_json={},
            updated_at=_now_cn(),
        )
        db.add(row)
        await db.flush()
        return row, True

    @staticmethod
    def _risk_settings(settings_row: TradingPlaybookSettings) -> Dict[str, Any]:
        trial = _finite_number(settings_row.trial_position_pct)
        confirmed = _finite_number(settings_row.confirmed_position_pct)
        hard_stop = _finite_number(settings_row.hard_stop_pct)
        maximum = settings_row.max_action_candidates
        if (
            trial is None
            or confirmed is None
            or trial < 0
            or confirmed < 0
            or trial > confirmed
            or confirmed > 100
        ):
            raise ValueError("invalid playbook position settings")
        if hard_stop is None or not 0 < hard_stop <= 20:
            raise ValueError("invalid playbook hard stop setting")
        if (
            isinstance(maximum, bool)
            or not isinstance(maximum, int)
            or not 1 <= maximum <= 3
        ):
            raise ValueError("max_action_candidates must be between 1 and 3")
        return {
            "trial": trial,
            "confirmed": confirmed,
            "hard_stop": hard_stop,
            "max_candidates": maximum,
            "source_refs": [
                {
                    "source_key": "03-loss-qa",
                    "excerpt": "候选不超过三只，开仓和退出条件必须预先写清，并执行刚性止损",
                },
                {
                    "source_key": "04-trading-plan",
                    "excerpt": "交易前形成书面计划，盘后区分信号、执行与结果",
                },
            ],
        }

    @classmethod
    def _select_candidates(
        cls,
        snapshot: MarketSnapshot,
        radar: Sequence[Mapping[str, Any]],
        risk_settings: Mapping[str, Any],
        candidate_sources: Mapping[str, Mapping[str, Any]],
    ) -> List[Tuple[Dict[str, Any], List[str]]]:
        global_unsafe = cls._globally_unsafe(snapshot)
        eligible = []
        for source in radar:
            row = dict(source)
            if row["status"] != "matched" or row["risk_level"] not in {
                "trial",
                "confirmed",
            }:
                continue
            if global_unsafe and row["risk_level"] == "confirmed":
                continue
            if row["risk_level"] == "confirmed" and cls._evidence_unsafe(
                snapshot,
                row,
                candidate_sources.get(row["stock_code"], {}),
            ):
                continue
            if cls._reference_price(row["entry_trigger"]) is None:
                continue
            eligible.append(row)

        by_stock: Dict[str, List[Dict[str, Any]]] = {}
        for row in eligible:
            by_stock.setdefault(row["stock_code"], []).append(row)
        selected = []
        for code, rows in by_stock.items():
            rows.sort(key=lambda item: (-item["score"], item["mode_key"]))
            primary = rows[0]
            supporting = sorted(
                {row["mode_key"] for row in rows[1:] if row["mode_key"] != primary["mode_key"]}
            )
            selected.append((primary, supporting))
        selected.sort(
            key=lambda item: (
                -item[0]["score"],
                item[0]["stock_code"],
                item[0]["mode_key"],
            )
        )
        return selected[: int(risk_settings["max_candidates"])]

    @classmethod
    def _evidence_unsafe(
        cls,
        snapshot: MarketSnapshot,
        row: Mapping[str, Any],
        candidate_source: Mapping[str, Any],
    ) -> bool:
        evidence = row.get("evidence")
        if not isinstance(evidence, list):
            return True
        structured = any(
            isinstance(item, Mapping)
            and item.get("source") in {"mode_requirement", "mode_risk"}
            for item in evidence
        )
        risk_rows = []
        required_candidate_fields = set()
        for item in evidence:
            if not isinstance(item, Mapping):
                return True
            source = item.get("source")
            inherently_relevant = source in {"mode_requirement", "mode_risk"}
            explicitly_relevant = item.get("required") is True or item.get(
                "relevant"
            ) is True
            relevant = inherently_relevant or explicitly_relevant
            if source == "mode_requirement":
                if item.get("result") != "matched":
                    return True
                feature = item.get("feature")
                if isinstance(feature, str) and feature.startswith("candidate."):
                    required_candidate_fields.add(feature.split(".", 1)[1])
            elif source == "mode_risk":
                risk_rows.append(item)

            if relevant:
                if cls._evidence_row_unsafe(item, snapshot.as_of, strict=True):
                    return True
            elif not structured and cls._evidence_row_unsafe(
                item,
                snapshot.as_of,
                strict=False,
            ):
                return True

        if structured and (
            not risk_rows
            or any(
                str(item.get("quality") or "").lower()
                not in {"ready", "computed", "ok"}
                for item in risk_rows
            )
        ):
            return True

        features = candidate_source.get("features", {})
        if not isinstance(features, Mapping):
            return bool(required_candidate_fields)
        if features.get("_snapshot_stale", False) is True:
            return True
        if features.get("_point_in_time_valid", True) is not True:
            return True
        quality_map = features.get("_feature_quality", {})
        for field in required_candidate_fields:
            if field not in features or not isinstance(quality_map, Mapping):
                return True
            if str(quality_map.get(field) or "").lower() not in {
                "ready",
                "computed",
                "ok",
            }:
                return True
        source_evidence = candidate_source.get("evidence", [])
        if isinstance(source_evidence, list):
            for item in source_evidence:
                if (
                    isinstance(item, Mapping)
                    and (
                        item.get("required") is True
                        or item.get("relevant") is True
                    )
                    and cls._evidence_row_unsafe(
                        item,
                        snapshot.as_of,
                        strict=True,
                    )
                ):
                    return True
        return False

    @classmethod
    def _evidence_row_unsafe(
        cls,
        evidence: Mapping[str, Any],
        snapshot_as_of: datetime,
        *,
        strict: bool,
    ) -> bool:
        if evidence.get("stale") is True or evidence.get("future") is True:
            return True
        if evidence.get("valid") is False:
            return True
        if evidence.get("point_in_time_valid") is False:
            return True
        quality = evidence.get("quality")
        if quality is not None:
            normalized_quality = str(quality).strip().lower()
            if strict and normalized_quality not in {"ready", "computed", "ok"}:
                return True
            if not strict and normalized_quality in {
                "stale",
                "future",
                "invalid",
                "error",
                "unavailable",
            }:
                return True
        elif (
            strict
            and evidence.get("source") != "mode_requirement"
            and (
                evidence.get("required") is True
                or evidence.get("relevant") is True
            )
        ):
            return True
        for key in (
            "captured_at",
            "as_of",
            "available_at",
            "observed_at",
            "timestamp",
        ):
            if key in evidence and cls._future_evidence_time(
                evidence[key],
                snapshot_as_of,
            ):
                return True
        return False

    @staticmethod
    def _future_evidence_time(value: Any, snapshot_as_of: datetime) -> bool:
        if isinstance(value, datetime):
            captured = value
        elif isinstance(value, str):
            try:
                captured = datetime.fromisoformat(value)
            except ValueError:
                return True
        else:
            return True
        if (captured.utcoffset() is None) != (snapshot_as_of.utcoffset() is None):
            return True
        return captured > snapshot_as_of

    @staticmethod
    def _globally_unsafe(snapshot: MarketSnapshot) -> bool:
        quality = snapshot.quality
        market = snapshot.market_features
        if quality.stale or quality.status not in {"ready", "degraded"}:
            return True
        if (quality.as_of.utcoffset() is None) != (
            snapshot.as_of.utcoffset() is None
        ):
            return True
        if quality.as_of > snapshot.as_of:
            return True
        if market.get("_point_in_time_valid", True) is not True:
            return True
        if market.get("_snapshot_stale", False) is True:
            return True
        if not market:
            return True
        if market.get("style") in {None, "", "unknown"}:
            return True
        if market.get("window") in {None, "", "unknown"}:
            return True
        return False

    @staticmethod
    def _reference_price(entry_trigger: Mapping[str, Any]) -> Optional[float]:
        if not isinstance(entry_trigger, Mapping):
            return None
        price = _finite_number(entry_trigger.get("reference_price"))
        return price if price is not None and price > 0 else None

    @classmethod
    def _candidate_from_evaluation(
        cls,
        plan_id: int,
        snapshot: MarketSnapshot,
        row: Mapping[str, Any],
        supporting: Sequence[str],
        stock_names: Mapping[str, str],
        candidate_sources: Mapping[str, Mapping[str, Any]],
        rank: int,
        risk_settings: Mapping[str, Any],
    ) -> TradingPlanCandidate:
        reference_price = cls._reference_price(row["entry_trigger"])
        if reference_price is None:
            raise ValueError("candidate reference_price must be finite and positive")
        risk_level = row["risk_level"]
        if risk_level not in {"trial", "confirmed"}:
            raise ValueError("formal candidate risk level must be actionable")
        action_date = snapshot.target_trade_date
        if snapshot.stage == "preclose" and row["action_scope"] == "tail":
            action_date = snapshot.source_trade_date

        source = candidate_sources.get(row["stock_code"], {})
        features = source.get("features", {}) if isinstance(source, Mapping) else {}
        recognition_keys = {
            "fastest_rank",
            "highest_rank",
            "hardest_rank",
            "resilience_rank",
            "influence_rank",
            "recognition_score",
            "recognition_rank",
            "recognition_evidence",
            "recognition_quality",
            "recognition_missing_fields",
        }
        recognition = {
            key: copy.deepcopy(value)
            for key, value in features.items()
            if key in recognition_keys
        }
        invalidation = copy.deepcopy(row["invalidation"])
        invalidation["price_lte"] = round(
            reference_price * (1 - float(risk_settings["hard_stop"]) / 100),
            2,
        )
        stock_name = stock_names.get(row["stock_code"]) or str(
            source.get("stock_name") or row["stock_code"]
        )
        return TradingPlanCandidate(
            plan_version_id=plan_id,
            stock_code=row["stock_code"],
            stock_name=stock_name,
            action_trade_date=action_date,
            theme_name=str(source.get("theme_name") or ""),
            primary_mode_key=row["mode_key"],
            supporting_mode_keys_json=list(supporting),
            role=row["role"],
            rank=rank,
            recognition_json=recognition,
            entry_trigger_json=copy.deepcopy(row["entry_trigger"]),
            invalidation_json=invalidation,
            exit_trigger_json=copy.deepcopy(row["exit_trigger"]),
            risk_level=risk_level,
            position_reference=float(risk_settings[risk_level]),
            evidence_json=copy.deepcopy(row["evidence"]),
            manual_overrides_json={},
            status="waiting",
        )

    @staticmethod
    async def _find_same_input(
        db,
        target_trade_date: date,
        stage: str,
        input_hash: str,
    ) -> Optional[TradingPlanVersion]:
        return await db.scalar(
            select(TradingPlanVersion)
            .where(
                TradingPlanVersion.target_trade_date == target_trade_date,
                TradingPlanVersion.stage == stage,
                TradingPlanVersion.input_hash == input_hash,
            )
            .order_by(TradingPlanVersion.version_no)
            .limit(1)
        )

    @staticmethod
    async def _next_version_no(db, target_trade_date: date, stage: str) -> int:
        highest = await db.scalar(
            select(func.max(TradingPlanVersion.version_no)).where(
                TradingPlanVersion.target_trade_date == target_trade_date,
                TradingPlanVersion.stage == stage,
            )
        )
        return int(highest or 0) + 1

    @staticmethod
    async def _latest_parent_plan(
        db,
        target_trade_date: date,
        stage: str,
    ) -> Optional[TradingPlanVersion]:
        try:
            stage_index = _STAGE_SEQUENCE.index(stage)
        except ValueError as exc:
            raise ValueError("invalid plan stage") from exc
        eligible_stages = _STAGE_SEQUENCE[: stage_index + 1]
        rows = (
            await db.scalars(
                select(TradingPlanVersion).where(
                    TradingPlanVersion.target_trade_date == target_trade_date,
                    TradingPlanVersion.stage.in_(eligible_stages),
                )
            )
        ).all()
        if not rows:
            return None
        stage_rank = {name: index for index, name in enumerate(_STAGE_SEQUENCE)}
        return max(
            rows,
            key=lambda row: (
                stage_rank[row.stage],
                row.version_no,
                row.id,
            ),
        )

    @staticmethod
    async def _change_summary(
        db,
        target_trade_date: date,
        radar: Sequence[Mapping[str, Any]],
    ) -> Dict[str, Any]:
        previous = await db.scalar(
            select(TradingPlanVersion)
            .where(TradingPlanVersion.target_trade_date == target_trade_date)
            .order_by(
                TradingPlanVersion.generated_at.desc(),
                TradingPlanVersion.id.desc(),
            )
            .limit(1)
        )
        current_matches = sorted(
            {
                (row["stock_code"], row["mode_key"])
                for row in radar
                if row["status"] == "matched"
            }
        )
        previous_matches = set()
        if previous is not None:
            previous_matches = {
                (row.get("stock_code"), row.get("mode_key"))
                for row in (previous.mode_radar_json or [])
                if isinstance(row, Mapping) and row.get("status") == "matched"
            }
        return {
            "previous_plan_version_id": previous.id if previous else None,
            "added_matches": [
                {"stock_code": code, "mode_key": mode}
                for code, mode in current_matches
                if (code, mode) not in previous_matches
            ],
            "removed_matches": [
                {"stock_code": code, "mode_key": mode}
                for code, mode in sorted(previous_matches)
                if (code, mode) not in set(current_matches)
            ],
        }

    async def serialize(self, db, plan_or_id) -> Optional[Dict[str, Any]]:
        plan = plan_or_id
        if not isinstance(plan, TradingPlanVersion):
            plan = await db.get(TradingPlanVersion, plan_or_id)
        if plan is None:
            return None
        candidates = (
            await db.scalars(
                select(TradingPlanCandidate)
                .where(TradingPlanCandidate.plan_version_id == plan.id)
                .order_by(
                    TradingPlanCandidate.rank,
                    TradingPlanCandidate.stock_code,
                    TradingPlanCandidate.primary_mode_key,
                )
            )
        ).all()
        payload = {
            "id": plan.id,
            "source_trade_date": plan.source_trade_date.isoformat(),
            "target_trade_date": plan.target_trade_date.isoformat(),
            "stage": plan.stage,
            "version_no": plan.version_no,
            "parent_plan_version_id": plan.parent_plan_version_id,
            "status": plan.status,
            "market_state_json": copy.deepcopy(plan.market_state_json or {}),
            "theme_ranking_json": copy.deepcopy(plan.theme_ranking_json or []),
            "mode_radar_json": copy.deepcopy(plan.mode_radar_json or []),
            "rule_snapshot_json": copy.deepcopy(plan.rule_snapshot_json or []),
            "risk_settings_json": copy.deepcopy(plan.risk_settings_json or {}),
            "data_quality_json": copy.deepcopy(plan.data_quality_json or {}),
            "change_summary_json": copy.deepcopy(plan.change_summary_json or {}),
            "input_hash": plan.input_hash,
            "generated_at": _china_iso(plan.generated_at),
            "confirmed_at": _china_iso(plan.confirmed_at)
            if plan.confirmed_at
            else None,
            "confirmed_by": plan.confirmed_by,
            "candidates": [self._serialize_candidate(row) for row in candidates],
        }
        payload.update(
            {
                "market_state": copy.deepcopy(payload["market_state_json"]),
                "theme_rankings": copy.deepcopy(payload["theme_ranking_json"]),
                "mode_radar": copy.deepcopy(payload["mode_radar_json"]),
                "rule_snapshot": copy.deepcopy(payload["rule_snapshot_json"]),
                "risk_settings": copy.deepcopy(payload["risk_settings_json"]),
                "data_quality": copy.deepcopy(payload["data_quality_json"]),
            }
        )
        return payload

    @staticmethod
    def _serialize_candidate(candidate: TradingPlanCandidate) -> Dict[str, Any]:
        return {
            "id": candidate.id,
            "plan_version_id": candidate.plan_version_id,
            "stock_code": candidate.stock_code,
            "stock_name": candidate.stock_name,
            "action_trade_date": candidate.action_trade_date.isoformat(),
            "theme_name": candidate.theme_name,
            "primary_mode_key": candidate.primary_mode_key,
            "supporting_mode_keys_json": copy.deepcopy(
                candidate.supporting_mode_keys_json or []
            ),
            "role": candidate.role,
            "rank": candidate.rank,
            "recognition_json": copy.deepcopy(candidate.recognition_json or {}),
            "entry_trigger_json": copy.deepcopy(
                candidate.entry_trigger_json or {}
            ),
            "invalidation_json": copy.deepcopy(candidate.invalidation_json or {}),
            "exit_trigger_json": copy.deepcopy(candidate.exit_trigger_json or {}),
            "risk_level": candidate.risk_level,
            "position_reference": candidate.position_reference,
            "evidence_json": copy.deepcopy(candidate.evidence_json or []),
            "manual_overrides_json": copy.deepcopy(
                candidate.manual_overrides_json or {}
            ),
            "status": candidate.status,
        }

    async def revise(
        self,
        db,
        plan_id: int,
        changes: Mapping[str, Any],
    ) -> TradingPlanVersion:
        parent = await db.get(TradingPlanVersion, plan_id)
        if parent is None:
            raise ValueError("plan not found")
        if parent.status not in {"draft", "confirmed", "active"}:
            raise ValueError("plan cannot be revised")
        parent_candidates = await self._load_candidates(db, parent.id)
        normalized_changes = self._normalize_revision_changes(
            parent,
            parent_candidates,
            changes,
        )
        lock_key = (id(db.bind), parent.target_trade_date, parent.stage)
        lock = self._generation_locks.setdefault(lock_key, asyncio.Lock())
        async with lock:
            for attempt in range(3):
                try:
                    current_parent = await db.get(TradingPlanVersion, plan_id)
                    if current_parent is None or current_parent.status not in {
                        "draft",
                        "confirmed",
                        "active",
                    }:
                        raise ValueError("plan cannot be revised")
                    current_candidates = await self._load_candidates(
                        db,
                        current_parent.id,
                    )
                    version_no = await self._next_version_no(
                        db,
                        current_parent.target_trade_date,
                        current_parent.stage,
                    )
                    child = self._clone_plan_for_revision(
                        current_parent,
                        normalized_changes,
                        version_no,
                    )
                    db.add(child)
                    await db.flush()
                    overrides = normalized_changes["overrides_by_candidate_id"]
                    for candidate in current_candidates:
                        db.add(
                            self._clone_candidate_for_revision(
                                candidate,
                                child.id,
                                overrides.get(candidate.id),
                                child.risk_settings_json,
                            )
                        )
                    await db.commit()
                    return child
                except IntegrityError:
                    await db.rollback()
                    if attempt == 2:
                        raise RuntimeError(
                            "could not allocate a unique revision version"
                        )
                except Exception:
                    await db.rollback()
                    raise
        raise RuntimeError("could not revise plan")

    @staticmethod
    async def _load_candidates(db, plan_id: int) -> List[TradingPlanCandidate]:
        return list(
            (
                await db.scalars(
                    select(TradingPlanCandidate)
                    .where(TradingPlanCandidate.plan_version_id == plan_id)
                    .order_by(
                        TradingPlanCandidate.rank,
                        TradingPlanCandidate.stock_code,
                        TradingPlanCandidate.primary_mode_key,
                    )
                )
            ).all()
        )

    @classmethod
    def _normalize_revision_changes(
        cls,
        parent: TradingPlanVersion,
        candidates: Sequence[TradingPlanCandidate],
        changes: Mapping[str, Any],
    ) -> Dict[str, Any]:
        if not isinstance(changes, Mapping):
            raise ValueError("revision changes must be a mapping")
        detached = copy.deepcopy(dict(changes))
        unknown_top = set(detached) - {"change_note", "candidate_overrides"}
        if unknown_top:
            raise ValueError(f"unknown revision fields: {sorted(unknown_top)}")
        note = detached.get("change_note")
        if not isinstance(note, str) or not 1 <= len(note.strip()) <= 500:
            raise ValueError("change_note is required")
        raw_overrides = detached.get("candidate_overrides", [])
        if not isinstance(raw_overrides, list):
            raise ValueError("candidate_overrides must be a list")
        if len(candidates) > 3 or len({row.stock_code for row in candidates}) != len(
            candidates
        ):
            raise ValueError("parent candidate set violates the three-stock limit")

        by_id = {row.id: row for row in candidates}
        by_identity = {
            (row.stock_code, row.primary_mode_key): row for row in candidates
        }
        if len(by_identity) != len(candidates):
            raise ValueError("parent candidate identities are not unique")
        normalized_by_id: Dict[int, Dict[str, Any]] = {}
        audit_rows = []
        allowed = {
            "candidate_id",
            "stock_code",
            "primary_mode_key",
            "action_trade_date",
            "entry_trigger",
            "invalidation",
            "exit_trigger",
            "manual_note",
        }
        for index, source in enumerate(raw_overrides):
            if not isinstance(source, Mapping):
                raise ValueError("candidate override must be a mapping")
            override = copy.deepcopy(dict(source))
            unknown = set(override) - allowed
            if unknown:
                raise ValueError(f"unknown candidate override fields: {sorted(unknown)}")
            has_id = "candidate_id" in override
            has_code = "stock_code" in override
            has_mode = "primary_mode_key" in override
            if has_id and (has_code or has_mode):
                raise ValueError("candidate override must use one locator")
            if has_id:
                candidate_id = override["candidate_id"]
                if isinstance(candidate_id, bool) or not isinstance(candidate_id, int):
                    raise ValueError("candidate_id must be an integer")
                candidate = by_id.get(candidate_id)
            elif has_code and has_mode:
                code = str(override["stock_code"] or "").strip()
                mode = str(override["primary_mode_key"] or "").strip()
                candidate = by_identity.get((code, mode))
            else:
                raise ValueError("candidate override locator is required")
            if candidate is None:
                raise ValueError("candidate override target not found")
            if candidate.id in normalized_by_id:
                raise ValueError("duplicate candidate override")

            normalized: Dict[str, Any] = {}
            if "action_trade_date" in override:
                action_date = cls._revision_date(override["action_trade_date"])
                if action_date not in {
                    parent.source_trade_date,
                    parent.target_trade_date,
                }:
                    raise ValueError("action_trade_date must be a plan trade date")
                normalized["action_trade_date"] = action_date
            for source_key, target_key in (
                ("entry_trigger", "entry_trigger_json"),
                ("invalidation", "invalidation_json"),
                ("exit_trigger", "exit_trigger_json"),
            ):
                if source_key in override:
                    normalized[target_key] = cls._normalize_trigger_override(
                        override[source_key],
                        source_key,
                    )
            if "manual_note" in override:
                manual_note = override["manual_note"]
                if (
                    not isinstance(manual_note, str)
                    or not 1 <= len(manual_note.strip()) <= 500
                ):
                    raise ValueError("manual_note must be non-empty text")
                normalized["manual_note"] = manual_note.strip()
            if not normalized:
                raise ValueError("candidate override contains no changes")
            normalized_by_id[candidate.id] = normalized
            audit_rows.append(
                {
                    "candidate_id": candidate.id,
                    "stock_code": candidate.stock_code,
                    "primary_mode_key": candidate.primary_mode_key,
                    "fields": sorted(normalized),
                }
            )
        return {
            "change_note": note.strip(),
            "overrides_by_candidate_id": normalized_by_id,
            "audit_rows": audit_rows,
        }

    @staticmethod
    def _revision_date(value: Any) -> date:
        if isinstance(value, datetime):
            raise ValueError("action_trade_date must be a date")
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            try:
                return date.fromisoformat(value)
            except ValueError as exc:
                raise ValueError("invalid action_trade_date") from exc
        raise ValueError("invalid action_trade_date")

    @staticmethod
    def _normalize_trigger_override(value: Any, field: str) -> Dict[str, Any]:
        if not isinstance(value, Mapping):
            raise ValueError(f"{field} must be a mapping")
        if field == "invalidation" and "price_lte" in value:
            raise ValueError(
                "invalidation.price_lte is a hard stop and cannot be overridden"
            )
        allowed = {
            "label",
            "reference_price",
            "price_gte",
            "price_lte",
            "change_pct_gte",
            "change_pct_lte",
            "sealed",
            "open_count_gte",
        }
        unknown = set(value) - allowed
        if unknown:
            raise ValueError(f"unknown {field} fields: {sorted(unknown)}")
        normalized = _json_safe(value, path=field)
        for key, raw in normalized.items():
            if key == "label":
                if not isinstance(raw, str) or len(raw) > 500:
                    raise ValueError(f"invalid {field}.label")
            elif key == "sealed":
                if not isinstance(raw, bool):
                    raise ValueError(f"invalid {field}.sealed")
            elif key == "open_count_gte":
                if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
                    raise ValueError(f"invalid {field}.open_count_gte")
            else:
                number = _finite_number(raw)
                if number is None:
                    raise ValueError(f"invalid {field}.{key}")
                if key in {"reference_price", "price_gte", "price_lte"} and number <= 0:
                    raise ValueError(f"invalid {field}.{key}")
                normalized[key] = number
        return normalized

    @staticmethod
    def _clone_plan_for_revision(
        parent: TradingPlanVersion,
        normalized_changes: Mapping[str, Any],
        version_no: int,
    ) -> TradingPlanVersion:
        hash_payload = {
            "parent_plan_version_id": parent.id,
            "parent_input_hash": parent.input_hash,
            "change_note": normalized_changes["change_note"],
            "candidate_overrides": [
                {
                    "candidate_id": candidate_id,
                    "values": {
                        key: (
                            value.isoformat()
                            if isinstance(value, date)
                            else copy.deepcopy(value)
                        )
                        for key, value in sorted(values.items())
                    },
                }
                for candidate_id, values in sorted(
                    normalized_changes["overrides_by_candidate_id"].items()
                )
            ],
        }
        input_hash = hashlib.sha256(
            _canonical_json(hash_payload).encode("utf-8")
        ).hexdigest()
        return TradingPlanVersion(
            source_trade_date=parent.source_trade_date,
            target_trade_date=parent.target_trade_date,
            stage=parent.stage,
            version_no=version_no,
            parent_plan_version_id=parent.id,
            status="draft",
            market_state_json=copy.deepcopy(parent.market_state_json or {}),
            theme_ranking_json=copy.deepcopy(parent.theme_ranking_json or []),
            mode_radar_json=copy.deepcopy(parent.mode_radar_json or []),
            rule_snapshot_json=copy.deepcopy(parent.rule_snapshot_json or []),
            risk_settings_json=copy.deepcopy(parent.risk_settings_json or {}),
            data_quality_json=copy.deepcopy(parent.data_quality_json or {}),
            change_summary_json={
                "manual": True,
                "change_note": normalized_changes["change_note"],
                "candidate_overrides": copy.deepcopy(
                    normalized_changes["audit_rows"]
                ),
            },
            input_hash=input_hash,
            generated_at=_now_cn(),
            confirmed_at=None,
            confirmed_by=None,
        )

    @classmethod
    def _clone_candidate_for_revision(
        cls,
        parent: TradingPlanCandidate,
        child_plan_id: int,
        override: Optional[Mapping[str, Any]],
        risk_settings: Mapping[str, Any],
    ) -> TradingPlanCandidate:
        values = {
            "action_trade_date": parent.action_trade_date,
            "entry_trigger_json": copy.deepcopy(parent.entry_trigger_json or {}),
            "invalidation_json": copy.deepcopy(parent.invalidation_json or {}),
            "exit_trigger_json": copy.deepcopy(parent.exit_trigger_json or {}),
            "manual_overrides_json": copy.deepcopy(
                parent.manual_overrides_json or {}
            ),
        }
        if override:
            if "action_trade_date" in override:
                values["action_trade_date"] = override["action_trade_date"]
            for key in (
                "entry_trigger_json",
                "invalidation_json",
                "exit_trigger_json",
            ):
                if key in override:
                    merged = copy.deepcopy(values[key])
                    merged.update(copy.deepcopy(override[key]))
                    values[key] = merged
            audit = copy.deepcopy(values["manual_overrides_json"])
            for key, value in override.items():
                if key == "action_trade_date":
                    audit[key] = value.isoformat()
                elif key == "manual_note":
                    audit[key] = value
            values["manual_overrides_json"] = audit

        reference_price = cls._reference_price(values["entry_trigger_json"])
        hard_stop = _finite_number(risk_settings.get("hard_stop"))
        if reference_price is None or hard_stop is None or not 0 < hard_stop <= 20:
            raise ValueError("revised candidate must retain valid risk prices")
        values["invalidation_json"]["price_lte"] = round(
            reference_price * (1 - hard_stop / 100),
            2,
        )
        if override:
            audit = values["manual_overrides_json"]
            for key in (
                "entry_trigger_json",
                "exit_trigger_json",
            ):
                if key in override:
                    audit[key] = copy.deepcopy(values[key])
            if (
                "entry_trigger_json" in override
                or "invalidation_json" in override
            ):
                audit["invalidation_json"] = copy.deepcopy(
                    values["invalidation_json"]
                )
        return TradingPlanCandidate(
            plan_version_id=child_plan_id,
            stock_code=parent.stock_code,
            stock_name=parent.stock_name,
            action_trade_date=values["action_trade_date"],
            theme_name=parent.theme_name,
            primary_mode_key=parent.primary_mode_key,
            supporting_mode_keys_json=copy.deepcopy(
                parent.supporting_mode_keys_json or []
            ),
            role=parent.role,
            rank=parent.rank,
            recognition_json=copy.deepcopy(parent.recognition_json or {}),
            entry_trigger_json=values["entry_trigger_json"],
            invalidation_json=values["invalidation_json"],
            exit_trigger_json=values["exit_trigger_json"],
            risk_level=parent.risk_level,
            position_reference=parent.position_reference,
            evidence_json=copy.deepcopy(parent.evidence_json or []),
            manual_overrides_json=values["manual_overrides_json"],
            status=parent.status,
        )

    async def confirm(
        self,
        db,
        plan_id: int,
        confirmed_by: str,
    ) -> TradingPlanVersion:
        if (
            not isinstance(confirmed_by, str)
            or not 1 <= len(confirmed_by.strip()) <= 80
        ):
            raise ValueError("confirmed_by is required")
        plan = await db.get(TradingPlanVersion, plan_id)
        if plan is None:
            raise ValueError("plan not found")
        lock_key = (id(db.bind), plan.target_trade_date, "__confirm__")
        lock = self._generation_locks.setdefault(lock_key, asyncio.Lock())
        async with lock:
            try:
                await db.refresh(plan)
                if plan.status not in {"draft", "confirmed"}:
                    raise ValueError("plan cannot be confirmed")
                active_plans = (
                    await db.scalars(
                        select(TradingPlanVersion).where(
                            TradingPlanVersion.target_trade_date
                            == plan.target_trade_date,
                            TradingPlanVersion.status == "active",
                            TradingPlanVersion.id != plan.id,
                        )
                    )
                ).all()
                for active in active_plans:
                    active.status = "superseded"
                plan.status = "active"
                plan.confirmed_at = _now_cn()
                plan.confirmed_by = confirmed_by.strip()
                await db.commit()
                return plan
            except IntegrityError as exc:
                await db.rollback()
                raise ValueError(
                    "another plan is already active for this target trade date"
                ) from exc
            except OperationalError as exc:
                await db.rollback()
                if "locked" in str(exc).lower():
                    raise ValueError(
                        "confirmation conflict; another worker is updating this date"
                    ) from exc
                raise
            except Exception:
                await db.rollback()
                raise


__all__ = ["TradingPlanService"]
