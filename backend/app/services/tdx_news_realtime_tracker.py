"""Track realtime TDX news items and emit only newly seen entries."""
from __future__ import annotations

import copy
from typing import Any, Dict, List, Set


class TdxNewsRealtimeTracker:
    def __init__(self, *, max_seen: int = 1000):
        self.max_seen = max_seen
        self._primed = False
        self._seen_keys: Set[str] = set()
        self._seen_order: List[str] = []

    def reset(self):
        self._primed = False
        self._seen_keys.clear()
        self._seen_order.clear()

    def has_seen(self, key: str) -> bool:
        return key in self._seen_keys

    def collect_new_items(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        new_items: List[Dict[str, Any]] = []
        current_keys: List[str] = []

        for item in items:
            key = self.item_key(item)
            if not key:
                continue
            current_keys.append(key)
            if self._primed and key not in self._seen_keys:
                new_items.append(copy.deepcopy(item))

        for key in current_keys:
            self._mark_seen(key)

        self._primed = True
        self._prune_seen()
        return list(reversed(new_items))

    def item_key(self, item: Dict[str, Any]) -> str:
        news_id = str(item.get("news_id") or "").strip()
        if news_id:
            return news_id
        source = str(item.get("source") or "").strip()
        time = str(item.get("time") or item.get("published_at") or "").strip()
        title = str(item.get("title") or "").strip()
        return "|".join(part for part in [source, time, title] if part)

    def _mark_seen(self, key: str):
        if key in self._seen_keys:
            return
        self._seen_keys.add(key)
        self._seen_order.append(key)

    def _prune_seen(self):
        if len(self._seen_order) <= self.max_seen:
            return
        remove_count = len(self._seen_order) - self.max_seen
        for key in self._seen_order[:remove_count]:
            self._seen_keys.discard(key)
        self._seen_order = self._seen_order[remove_count:]
