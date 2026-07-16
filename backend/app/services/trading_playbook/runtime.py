"""Shared runtime registration contract for staged playbook services."""

from __future__ import annotations

from threading import RLock
from typing import Any


class TradingPlaybookRuntime:
    """Process-local registry installed by later startup tasks.

    Task 8 owns the stable registration contract.  Task 9 installs the single
    orchestrator and Task 11 installs the review service.  Tests must call
    ``reset`` so registered doubles cannot leak between applications.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._orchestrator: Any | None = None
        self._review_service: Any | None = None

    def install_orchestrator(self, orchestrator: Any) -> None:
        if not callable(getattr(orchestrator, "build_stage", None)):
            raise TypeError("orchestrator must provide build_stage")
        with self._lock:
            self._orchestrator = orchestrator

    def install_review_service(self, review_service: Any) -> None:
        if not callable(
            getattr(review_service, "update_manual_execution", None)
        ):
            raise TypeError(
                "review service must provide update_manual_execution"
            )
        with self._lock:
            self._review_service = review_service

    def get_orchestrator(self) -> Any | None:
        with self._lock:
            return self._orchestrator

    def get_review_service(self) -> Any | None:
        with self._lock:
            return self._review_service

    def reset(self) -> None:
        """Clear registrations during shutdown and isolated tests."""
        with self._lock:
            self._orchestrator = None
            self._review_service = None


trading_playbook_runtime = TradingPlaybookRuntime()


__all__ = ["TradingPlaybookRuntime", "trading_playbook_runtime"]
