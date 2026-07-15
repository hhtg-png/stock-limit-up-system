from __future__ import annotations

import os
import re
import subprocess
import tempfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any


_WINDOWS_ILLEGAL_CHARACTERS = frozenset('<>:"\\|?*')
_WINDOWS_RESERVED_BASENAMES = frozenset(
    {"CON", "PRN", "AUX", "NUL"}
    | {f"COM{number}" for number in range(1, 10)}
    | {f"LPT{number}" for number in range(1, 10)}
)
_WINDOWS_DRIVE_PATH = re.compile(r"^[A-Za-z]:")


@dataclass(frozen=True)
class VaultWriteResult:
    relative_path: str
    absolute_path: Path
    changed: bool


class ObsidianVaultWriter:
    """Perform allowlisted, atomic writes and path-limited Git commits in a Vault."""

    def __init__(
        self,
        *,
        enabled: bool,
        vault_path: str | Path,
        auto_git_enabled: bool,
        command_runner: Callable[..., Any] = subprocess.run,
    ) -> None:
        self.enabled = bool(enabled)
        self.vault_path = vault_path
        self.auto_git_enabled = bool(auto_git_enabled)
        self.command_runner = command_runner

    def configured_vault(self) -> Path | None:
        raw_path = str(self.vault_path or "").strip()
        if not raw_path:
            return None
        return Path(raw_path).expanduser().resolve(strict=False)

    def ensure_vault(self) -> Path | None:
        if not self.enabled:
            return None
        vault = self.configured_vault()
        if vault is None:
            return None
        vault.mkdir(parents=True, exist_ok=True)
        return vault

    def resolve_target(self, relative_path: str, *, allowed_roots: tuple[str, ...]) -> Path:
        normalized_path = self._validate_relative_path(relative_path, allowed_roots=allowed_roots)
        vault = self.configured_vault()
        if vault is None:
            raise ValueError("Obsidian Vault path is not configured")

        target = vault.joinpath(*normalized_path.split("/"))
        resolved_target = target.resolve(strict=False)
        if not self._is_relative_to(resolved_target, vault):
            raise ValueError(f"Vault path escapes through a symlink or junction: {relative_path}")
        allowed_locations = [vault.joinpath(*root.split("/")) for root in allowed_roots]
        if not any(self._is_relative_to(resolved_target, root) for root in allowed_locations):
            raise ValueError(f"Vault path escapes its allowed root through a symlink or junction: {relative_path}")
        return resolved_target

    def write_text(
        self,
        relative_path: str,
        content: str,
        *,
        allowed_roots: tuple[str, ...],
    ) -> VaultWriteResult:
        vault = self.ensure_vault()
        if vault is None:
            raise ValueError("Obsidian Vault is disabled or unconfigured")

        target = self.resolve_target(relative_path, allowed_roots=allowed_roots)
        normalized_content = content.replace("\r\n", "\n").replace("\r", "\n")
        encoded_content = normalized_content.encode("utf-8")
        if target.is_file() and target.read_bytes() == encoded_content:
            return VaultWriteResult(relative_path=relative_path, absolute_path=target, changed=False)

        target.parent.mkdir(parents=True, exist_ok=True)
        target = self.resolve_target(relative_path, allowed_roots=allowed_roots)
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="",
                dir=target.parent,
                prefix=f".{target.name}.",
                suffix=".tmp",
                delete=False,
            ) as temporary_file:
                temporary_path = Path(temporary_file.name)
                temporary_file.write(normalized_content)
            os.replace(temporary_path, target)
        except Exception:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
            raise

        return VaultWriteResult(relative_path=relative_path, absolute_path=target, changed=True)

    def commit_paths(
        self,
        relative_paths: Sequence[str],
        *,
        allowed_roots: tuple[str, ...],
        message: str,
    ) -> dict[str, object]:
        paths = list(dict.fromkeys(relative_paths))
        for relative_path in paths:
            self._validate_relative_path(relative_path, allowed_roots=allowed_roots)

        if not self.auto_git_enabled:
            return {"enabled": False}
        if not paths:
            return {"enabled": True, "committed": False, "reason": "no_written_files"}

        vault = self.configured_vault()
        if vault is None or not (vault / ".git").exists():
            return {"enabled": True, "committed": False, "reason": "vault_is_not_git_repo"}

        for relative_path in paths:
            self.resolve_target(relative_path, allowed_roots=allowed_roots)

        prefix = ["git", "-C", str(vault)]
        try:
            self.command_runner(
                prefix + ["add", "--", *paths],
                check=True,
                capture_output=True,
                text=True,
            )
            diff = self.command_runner(
                prefix + ["diff", "--cached", "--quiet", "--", *paths],
                check=False,
                capture_output=True,
                text=True,
            )
            if diff.returncode == 0:
                return {"enabled": True, "committed": False, "reason": "no_changes"}
            if diff.returncode != 1:
                raise subprocess.CalledProcessError(
                    diff.returncode,
                    prefix + ["diff", "--cached", "--quiet", "--", *paths],
                    output=getattr(diff, "stdout", ""),
                    stderr=getattr(diff, "stderr", ""),
                )
            self.command_runner(
                prefix + ["commit", "-m", message, "--", *paths],
                check=True,
                capture_output=True,
                text=True,
            )
            return {"enabled": True, "committed": True}
        except Exception as exc:
            return {"enabled": True, "committed": False, "error": self._error_text(exc)}

    @classmethod
    def _validate_relative_path(cls, relative_path: str, *, allowed_roots: tuple[str, ...]) -> str:
        if not isinstance(relative_path, str) or not relative_path:
            raise ValueError("Vault path must be a non-empty string")
        if relative_path.startswith("/") or _WINDOWS_DRIVE_PATH.match(relative_path):
            raise ValueError(f"Vault path must be relative: {relative_path}")

        parts = relative_path.split("/")
        for part in parts:
            cls._validate_path_part(part, relative_path=relative_path)

        normalized_roots = []
        for root in allowed_roots:
            if not isinstance(root, str) or not root:
                raise ValueError("Allowed Vault roots must be non-empty POSIX paths")
            root_parts = root.split("/")
            for part in root_parts:
                cls._validate_path_part(part, relative_path=root)
            normalized_roots.append("/".join(root_parts))

        normalized_path = "/".join(parts)
        if not any(normalized_path == root or normalized_path.startswith(f"{root}/") for root in normalized_roots):
            raise ValueError(f"Vault path is outside the caller allowlist: {relative_path}")
        return normalized_path

    @staticmethod
    def _validate_path_part(part: str, *, relative_path: str) -> None:
        if not part or part in {".", ".."}:
            raise ValueError(f"Vault path contains an empty or relative segment: {relative_path}")
        if part.endswith((".", " ")):
            raise ValueError(f"Vault path segment has a trailing dot or space: {relative_path}")
        if any(character in _WINDOWS_ILLEGAL_CHARACTERS or ord(character) < 32 for character in part):
            raise ValueError(f"Vault path contains a Windows-illegal character: {relative_path}")
        device_basename = part.split(".", 1)[0].upper()
        if device_basename in _WINDOWS_RESERVED_BASENAMES:
            raise ValueError(f"Vault path contains a reserved Windows device name: {relative_path}")

    @staticmethod
    def _is_relative_to(path: Path, root: Path) -> bool:
        try:
            path.relative_to(root)
        except ValueError:
            return False
        return True

    @staticmethod
    def _error_text(exc: Exception) -> str:
        details = str(exc)
        stderr = getattr(exc, "stderr", None)
        if stderr:
            details = f"{details}: {stderr}"
        return details
