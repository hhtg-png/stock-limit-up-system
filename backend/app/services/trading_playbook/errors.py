"""Stable domain exceptions shared by playbook services and transports."""


class InvalidRequestError(ValueError):
    """The caller can correct the supplied request."""


class InvalidTransitionError(ValueError):
    """A valid request conflicts with the current persisted state."""


class PlaybookNotFoundError(ValueError):
    """The requested playbook resource does not exist."""


class UpstreamUnavailableError(RuntimeError):
    """Required market data or an internal pipeline boundary is unavailable."""


class UnsafePlanDataError(RuntimeError):
    """Persisted plan data cannot be safely exposed."""


__all__ = [
    "InvalidRequestError",
    "InvalidTransitionError",
    "PlaybookNotFoundError",
    "UnsafePlanDataError",
    "UpstreamUnavailableError",
]
