from collections.abc import Mapping
from typing import Any


def action_quality_ready(quality: Any) -> bool:
    if not isinstance(quality, Mapping) or quality.get("status") != "ready":
        return False
    return "stale" not in quality or quality["stale"] is False
