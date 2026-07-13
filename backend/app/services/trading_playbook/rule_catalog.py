"""Load, validate, verify, and seed transcript-derived trading rules."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import TradingModeRule, TradingRuleSource


EXPECTED_CATALOG_VERSION = 1
EXPECTED_SOURCE_COUNT = 8
EXPECTED_RULE_COUNT = 19


def canonical_rule_content_hash(rule: dict[str, Any]) -> str:
    """Hash immutable rule content while ignoring matcher runtime metadata."""
    payload = {
        key: value
        for key, value in rule.items()
        if key not in {"version", "content_hash"}
    }
    canonical_rule = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical_rule.encode("utf-8")).hexdigest()


class RuleCatalog:
    """Access and persist one immutable version of the trading rule catalog."""

    def __init__(self, catalog_path: Path):
        self.catalog_path = Path(catalog_path)

    def load(self) -> dict[str, Any]:
        """Read and validate the version-one JSON catalog."""
        with self.catalog_path.open("r", encoding="utf-8") as handle:
            catalog = json.load(handle)

        if not isinstance(catalog, dict):
            raise ValueError("catalog root must be a JSON object")
        if catalog.get("catalog_version") != EXPECTED_CATALOG_VERSION:
            raise ValueError("catalog_version must be 1")

        sources = catalog.get("sources")
        rules = catalog.get("rules")
        if not isinstance(sources, list) or len(sources) != EXPECTED_SOURCE_COUNT:
            raise ValueError("catalog must contain exactly 8 sources")
        if not isinstance(rules, list) or len(rules) != EXPECTED_RULE_COUNT:
            raise ValueError("catalog must contain exactly 19 rules")

        source_keys = [source.get("source_key") for source in sources]
        if any(not key for key in source_keys):
            raise ValueError("every source must have a non-empty source_key")
        if len(source_keys) != len(set(source_keys)):
            raise ValueError("source_key values must be unique")

        mode_keys = [rule.get("mode_key") for rule in rules]
        if any(not key for key in mode_keys):
            raise ValueError("every rule must have a non-empty mode_key")
        if len(mode_keys) != len(set(mode_keys)):
            raise ValueError("mode_key values must be unique")

        known_source_keys = set(source_keys)
        for rule in rules:
            refs = rule.get("source_refs")
            if not isinstance(refs, list) or not refs:
                raise ValueError(
                    f'rule {rule["mode_key"]} must have non-empty source_refs'
                )
            for source_ref in refs:
                source_key = source_ref.get("source_key")
                if source_key not in known_source_keys:
                    raise ValueError(
                        f'rule {rule["mode_key"]} references unknown source_key: '
                        f"{source_key}"
                    )
                excerpt = source_ref.get("excerpt")
                if not isinstance(excerpt, str) or not excerpt.strip():
                    raise ValueError(
                        f'rule {rule["mode_key"]} has a blank source excerpt'
                    )

        return catalog

    def verify_sources(
        self,
        source_root: Path,
        sources: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Attach a raw-byte SHA-256 hash and availability status to sources."""
        source_root = Path(source_root)
        verified_sources = []
        for source in sources:
            verified_source = dict(source)
            transcript_path = source_root / source["source_path"]
            if transcript_path.is_file():
                verified_source["content_hash"] = hashlib.sha256(
                    transcript_path.read_bytes()
                ).hexdigest()
                verified_source["status"] = "ready"
            else:
                verified_source["content_hash"] = ""
                verified_source["status"] = "missing"
            verified_sources.append(verified_source)
        return verified_sources

    async def seed(self, db: AsyncSession, source_root: Path) -> dict[str, int]:
        """Seed verified sources and immutable rule versions atomically."""
        try:
            catalog = self.load()
            sources = self.verify_sources(source_root, catalog["sources"])
            missing_paths = [
                source["source_path"]
                for source in sources
                if source["status"] == "missing"
            ]
            if missing_paths:
                raise FileNotFoundError(
                    "missing transcripts: " + ", ".join(missing_paths)
                )

            version = catalog["catalog_version"]
            hashed_rules = []
            for rule in catalog["rules"]:
                content_hash = canonical_rule_content_hash(rule)
                hashed_rules.append((rule, content_hash))

            existing_rules = (
                await db.scalars(
                    select(TradingModeRule).where(
                        TradingModeRule.version == version
                    )
                )
            ).all()
            existing_by_mode = {
                rule.mode_key: rule.content_hash for rule in existing_rules
            }
            incoming_by_mode = {
                rule["mode_key"]: content_hash
                for rule, content_hash in hashed_rules
            }
            if existing_by_mode:
                if set(existing_by_mode) != set(incoming_by_mode):
                    raise ValueError(
                        "immutable rule set changed without catalog version bump"
                    )
                for mode_key, content_hash in incoming_by_mode.items():
                    if existing_by_mode[mode_key] != content_hash:
                        raise ValueError(
                            "immutable rule changed without catalog version bump: "
                            f"{mode_key}"
                        )

            for source in sources:
                source_insert = sqlite_insert(TradingRuleSource).values(
                    source_key=source["source_key"],
                    source_path=source["source_path"],
                    source_title=source["source_title"],
                    content_hash=source["content_hash"],
                    status=source["status"],
                )
                await db.execute(
                    source_insert.on_conflict_do_nothing(
                        index_elements=["source_key", "content_hash"]
                    )
                )

            for rule, content_hash in hashed_rules:
                if rule["mode_key"] in existing_by_mode:
                    continue

                db.add(
                    TradingModeRule(
                        mode_key=rule["mode_key"],
                        version=version,
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
                        source_refs_json=rule["source_refs"],
                        enabled=True,
                        content_hash=content_hash,
                    )
                )

            await db.commit()
            return {
                "sources": len(sources),
                "rules": len(catalog["rules"]),
            }
        except Exception:
            await db.rollback()
            raise
