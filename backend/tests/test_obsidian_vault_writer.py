import json
import subprocess
import tempfile
import unittest
from dataclasses import FrozenInstanceError
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app.services.obsidian_vault_writer import ObsidianVaultWriter


ALLOWED_ROOTS = ("00_Inbox/Auto", "10_Industry", "40_UltraShort", "50_Daily", "60_Signals", "Dashboards")


class ObsidianVaultWriterTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.vault = Path(self.temp_dir.name) / "vault"

    def tearDown(self):
        self.temp_dir.cleanup()

    def _writer(self, *, enabled=True, auto_git_enabled=False, command_runner=subprocess.run):
        return ObsidianVaultWriter(
            enabled=enabled,
            vault_path=str(self.vault),
            auto_git_enabled=auto_git_enabled,
            command_runner=command_runner,
        )

    def _create_directory_link(self, link: Path, target: Path) -> None:
        try:
            link.symlink_to(target, target_is_directory=True)
        except OSError as exc:
            junction = subprocess.run(
                ["cmd", "/c", "mklink", "/J", str(link), str(target)],
                capture_output=True,
                text=True,
            )
            if junction.returncode != 0:
                self.skipTest(f"directory symlinks and junctions are unavailable: {exc}")

    def _initialize_git_repository(self) -> None:
        self.vault.mkdir()
        subprocess.run(["git", "init", str(self.vault)], check=True, capture_output=True)
        subprocess.run(["git", "-C", str(self.vault), "config", "user.email", "test@example.com"], check=True)
        subprocess.run(["git", "-C", str(self.vault), "config", "user.name", "Test User"], check=True)
        subprocess.run(["git", "-C", str(self.vault), "config", "commit.gpgSign", "false"], check=True)
        subprocess.run(["git", "-C", str(self.vault), "config", "tag.gpgSign", "false"], check=True)
        disabled_hooks = self.vault / ".git" / "disabled-hooks"
        disabled_hooks.mkdir()
        subprocess.run(
            ["git", "-C", str(self.vault), "config", "core.hooksPath", str(disabled_hooks)],
            check=True,
        )
        readme = self.vault / "README.md"
        readme.write_text("baseline\n", encoding="utf-8")
        subprocess.run(["git", "-C", str(self.vault), "add", "README.md"], check=True)
        subprocess.run(["git", "-C", str(self.vault), "commit", "-m", "baseline"], check=True, capture_output=True)

    def test_configured_vault_resolves_path_without_creating_it(self):
        writer = self._writer()

        configured = writer.configured_vault()

        self.assertEqual(configured, self.vault.resolve())
        self.assertFalse(self.vault.exists())

    def test_ensure_vault_only_creates_an_enabled_configured_vault(self):
        disabled = self._writer(enabled=False)
        unconfigured = ObsidianVaultWriter(enabled=True, vault_path="", auto_git_enabled=False)

        self.assertIsNone(disabled.ensure_vault())
        self.assertIsNone(unconfigured.ensure_vault())
        self.assertFalse(self.vault.exists())
        self.assertEqual(self._writer().ensure_vault(), self.vault.resolve())
        self.assertTrue(self.vault.is_dir())

    def test_resolve_target_accepts_a_vault_relative_posix_path_in_the_allowlist(self):
        writer = self._writer()

        target = writer.resolve_target("50_Daily/2026/2026-07-15.md", allowed_roots=ALLOWED_ROOTS)

        self.assertEqual(target, self.vault.resolve() / "50_Daily" / "2026" / "2026-07-15.md")

    def test_resolve_target_rejects_unsafe_path_shapes(self):
        writer = self._writer()
        unsafe_paths = (
            "",
            "/50_Daily/note.md",
            "C:/50_Daily/note.md",
            "C:\\50_Daily\\note.md",
            "50_Daily//note.md",
            "50_Daily/./note.md",
            "50_Daily/../note.md",
            "50_Daily/note?.md",
            "50_Daily/note<draft>.md",
            "50_Daily/trailing. /note.md",
            "50_Daily/trailing./note.md",
            "50_Daily/CON.md",
            "50_Daily/prn",
            "50_Daily/AUX.txt",
            "50_Daily/NUL",
            "50_Daily/COM1.md",
            "50_Daily/com9",
            "50_Daily/LPT1.txt",
            "50_Daily/lpt9",
        )

        for relative_path in unsafe_paths:
            with self.subTest(relative_path=relative_path):
                with self.assertRaises(ValueError):
                    writer.resolve_target(relative_path, allowed_roots=ALLOWED_ROOTS)

    def test_resolve_target_rejects_paths_outside_the_caller_allowlist(self):
        writer = self._writer()

        with self.assertRaises(ValueError):
            writer.resolve_target("Notes/trading-playbook.md", allowed_roots=ALLOWED_ROOTS)

    def test_resolve_target_rejects_an_existing_symlink_escape(self):
        writer = self._writer()
        writer.ensure_vault()
        daily_root = self.vault / "50_Daily"
        daily_root.mkdir()
        outside = Path(self.temp_dir.name) / "outside"
        outside.mkdir()
        link = daily_root / "escape"
        self._create_directory_link(link, outside)

        with self.assertRaises(ValueError):
            writer.resolve_target("50_Daily/escape/note.md", allowed_roots=ALLOWED_ROOTS)

    def test_write_text_rejects_an_allowed_root_linked_to_unallowed_notes(self):
        writer = self._writer()
        writer.ensure_vault()
        notes = self.vault / "Notes"
        notes.mkdir()
        allowed_link = self.vault / "50_Daily"
        self._create_directory_link(allowed_link, notes)

        with self.assertRaises(ValueError):
            writer.write_text("50_Daily/private.md", "must not escape\n", allowed_roots=ALLOWED_ROOTS)
        self.assertFalse((notes / "private.md").exists())

    def test_write_text_normalizes_newlines_and_returns_a_frozen_result(self):
        writer = self._writer()

        result = writer.write_text("50_Daily/note.md", "one\r\ntwo\rthree\n", allowed_roots=ALLOWED_ROOTS)

        self.assertEqual((self.vault / "50_Daily" / "note.md").read_bytes(), b"one\ntwo\nthree\n")
        self.assertEqual(result.relative_path, "50_Daily/note.md")
        self.assertEqual(result.absolute_path, self.vault.resolve() / "50_Daily" / "note.md")
        self.assertTrue(result.changed)
        with self.assertRaises(FrozenInstanceError):
            result.changed = False

    def test_write_text_does_not_replace_identical_content(self):
        writer = self._writer()
        writer.write_text("50_Daily/note.md", "same\n", allowed_roots=ALLOWED_ROOTS)

        with patch("app.services.obsidian_vault_writer.os.replace") as replace:
            result = writer.write_text("50_Daily/note.md", "same\r\n", allowed_roots=ALLOWED_ROOTS)

        self.assertFalse(result.changed)
        replace.assert_not_called()

    def test_write_text_replace_failure_preserves_old_file_and_removes_temp_file(self):
        writer = self._writer()
        target = self.vault / "50_Daily" / "note.md"
        writer.write_text("50_Daily/note.md", "old\n", allowed_roots=ALLOWED_ROOTS)
        files_before = set(target.parent.iterdir())

        with patch("app.services.obsidian_vault_writer.os.replace", side_effect=OSError("replace failed")):
            with self.assertRaises(OSError):
                writer.write_text("50_Daily/note.md", "new\n", allowed_roots=ALLOWED_ROOTS)

        self.assertEqual(target.read_text(encoding="utf-8"), "old\n")
        self.assertEqual(set(target.parent.iterdir()), files_before)

    def test_write_text_cleanup_failure_does_not_mask_replace_failure(self):
        writer = self._writer()
        writer.write_text("50_Daily/note.md", "old\n", allowed_roots=ALLOWED_ROOTS)
        replace_failure = OSError("replace failed")

        with patch("app.services.obsidian_vault_writer.os.replace", side_effect=replace_failure):
            with patch("app.services.obsidian_vault_writer.Path.unlink", side_effect=OSError("cleanup failed")):
                with self.assertRaises(OSError) as raised:
                    writer.write_text("50_Daily/note.md", "new\n", allowed_roots=ALLOWED_ROOTS)

        self.assertIs(raised.exception, replace_failure)

    def test_commit_paths_is_a_no_op_when_git_is_disabled(self):
        def unexpected_runner(*args, **kwargs):
            self.fail("Git runner must not be called while auto Git is disabled")

        result = self._writer(command_runner=unexpected_runner).commit_paths(
            ["50_Daily/note.md"],
            allowed_roots=ALLOWED_ROOTS,
            message="test commit",
        )

        self.assertEqual(result, {"enabled": False})
        json.dumps(result)

    def test_commit_paths_validates_junction_escape_when_git_is_disabled(self):
        writer = self._writer()
        writer.ensure_vault()
        notes = self.vault / "Notes"
        notes.mkdir()
        self._create_directory_link(self.vault / "50_Daily", notes)

        with self.assertRaises(ValueError):
            writer.commit_paths(
                ["50_Daily/private.md"],
                allowed_roots=ALLOWED_ROOTS,
                message="test commit",
            )

    def test_commit_paths_is_a_no_op_when_no_paths_changed(self):
        def unexpected_runner(*args, **kwargs):
            self.fail("Git runner must not be called without paths")

        result = self._writer(auto_git_enabled=True, command_runner=unexpected_runner).commit_paths(
            [],
            allowed_roots=ALLOWED_ROOTS,
            message="test commit",
        )

        self.assertEqual(result, {"enabled": True, "committed": False, "reason": "no_written_files"})

    def test_commit_paths_rejects_notes_and_other_unallowed_paths(self):
        writer = self._writer(auto_git_enabled=True)

        for relative_path in ("Notes/playbook.md", "README.md"):
            with self.subTest(relative_path=relative_path):
                with self.assertRaises(ValueError):
                    writer.commit_paths([relative_path], allowed_roots=ALLOWED_ROOTS, message="test commit")

    def test_commit_paths_uses_only_path_limited_git_commands(self):
        self.vault.mkdir()
        (self.vault / ".git").mkdir()
        commands = []

        def runner(command, **kwargs):
            commands.append((command, kwargs))
            if "status" in command:
                return SimpleNamespace(returncode=0, stdout="?? 50_Daily/note.md\n", stderr="")
            returncode = 1 if "--quiet" in command else 0
            return SimpleNamespace(returncode=returncode, stdout="", stderr="")

        result = self._writer(auto_git_enabled=True, command_runner=runner).commit_paths(
            ["50_Daily/note.md"],
            allowed_roots=ALLOWED_ROOTS,
            message="test commit",
        )

        prefix = ["git", "--literal-pathspecs", "-C", str(self.vault.resolve())]
        self.assertEqual(
            [command for command, _ in commands],
            [
                prefix + ["status", "--porcelain", "--", "50_Daily/note.md"],
                prefix + ["add", "--", "50_Daily/note.md"],
                prefix + ["diff", "--cached", "--quiet", "--", "50_Daily/note.md"],
                prefix + ["commit", "-m", "test commit", "--", "50_Daily/note.md"],
            ],
        )
        for _, kwargs in commands:
            self.assertEqual(kwargs.get("encoding"), "utf-8")
            self.assertEqual(kwargs.get("errors"), "replace")
            self.assertEqual(kwargs.get("stdin"), subprocess.DEVNULL)
            self.assertIsInstance(kwargs.get("timeout"), (int, float))
            self.assertGreater(kwargs["timeout"], 0)
            self.assertEqual(kwargs.get("env", {}).get("GIT_TERMINAL_PROMPT"), "0")
            self.assertEqual(kwargs.get("env", {}).get("GIT_ASKPASS"), "")
        self.assertEqual(result, {"enabled": True, "committed": True})

    def test_commit_paths_reports_missing_git_repository(self):
        self.vault.mkdir()

        result = self._writer(auto_git_enabled=True).commit_paths(
            ["50_Daily/note.md"],
            allowed_roots=ALLOWED_ROOTS,
            message="test commit",
        )

        self.assertEqual(result, {"enabled": True, "committed": False, "reason": "vault_is_not_git_repo"})

    def test_commit_paths_validates_junction_escape_when_git_repository_is_missing(self):
        writer = self._writer(auto_git_enabled=True)
        writer.ensure_vault()
        notes = self.vault / "Notes"
        notes.mkdir()
        self._create_directory_link(self.vault / "50_Daily", notes)

        with self.assertRaises(ValueError):
            writer.commit_paths(
                ["50_Daily/private.md"],
                allowed_roots=ALLOWED_ROOTS,
                message="test commit",
            )

    def test_commit_paths_reports_git_failure_without_raising(self):
        self.vault.mkdir()
        (self.vault / ".git").mkdir()

        def failing_runner(command, **kwargs):
            raise subprocess.CalledProcessError(128, command, stderr="fatal: test failure")

        result = self._writer(auto_git_enabled=True, command_runner=failing_runner).commit_paths(
            ["50_Daily/note.md"],
            allowed_roots=ALLOWED_ROOTS,
            message="test commit",
        )

        self.assertTrue(result["enabled"])
        self.assertFalse(result["committed"])
        self.assertIn("fatal: test failure", result["error"])
        json.dumps(result)

    def test_commit_paths_reports_timeout_without_raising(self):
        self.vault.mkdir()
        (self.vault / ".git").mkdir()

        def timing_out_runner(command, **kwargs):
            if "status" in command:
                return SimpleNamespace(returncode=0, stdout="?? 50_Daily/note.md\n", stderr="")
            raise subprocess.TimeoutExpired(command, timeout=kwargs["timeout"])

        result = self._writer(auto_git_enabled=True, command_runner=timing_out_runner).commit_paths(
            ["50_Daily/note.md"],
            allowed_roots=ALLOWED_ROOTS,
            message="test commit",
        )

        self.assertTrue(result["enabled"])
        self.assertFalse(result["committed"])
        self.assertIn("timed out", result["error"])
        json.dumps(result)

    def test_commit_paths_leaves_unrelated_staged_content_out_of_the_commit(self):
        self._initialize_git_repository()
        readme = self.vault / "README.md"
        readme.write_text("unrelated staged change\n", encoding="utf-8")
        subprocess.run(["git", "-C", str(self.vault), "add", "README.md"], check=True)
        target = self.vault / "50_Daily" / "note.md"
        target.parent.mkdir()
        target.write_text("system change\n", encoding="utf-8")

        result = self._writer(auto_git_enabled=True).commit_paths(
            ["50_Daily/note.md"],
            allowed_roots=ALLOWED_ROOTS,
            message="system commit",
        )

        committed = subprocess.run(
            ["git", "-C", str(self.vault), "show", "--pretty=", "--name-only", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.splitlines()
        staged = subprocess.run(
            ["git", "-C", str(self.vault), "diff", "--cached", "--name-only"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.splitlines()
        self.assertEqual(result, {"enabled": True, "committed": True})
        self.assertEqual(committed, ["50_Daily/note.md"])
        self.assertEqual(staged, ["README.md"])

    def test_commit_paths_treats_git_metacharacters_as_a_literal_filename(self):
        self._initialize_git_repository()
        daily_root = self.vault / "50_Daily"
        daily_root.mkdir()
        for filename in ("a.md", "b.md"):
            (daily_root / filename).write_text(f"{filename}\n", encoding="utf-8")

        writer = self._writer(auto_git_enabled=True)
        absent_result = writer.commit_paths(
            ["50_Daily/[ab].md"],
            allowed_roots=ALLOWED_ROOTS,
            message="literal path commit",
        )

        commit_count = subprocess.run(
            ["git", "-C", str(self.vault), "rev-list", "--count", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        untracked_before = subprocess.run(
            ["git", "-C", str(self.vault), "ls-files", "--others", "--exclude-standard"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.splitlines()
        self.assertEqual(absent_result, {"enabled": True, "committed": False, "reason": "no_changes"})
        self.assertEqual(commit_count, "1")
        self.assertEqual(untracked_before, ["50_Daily/a.md", "50_Daily/b.md"])

        (daily_root / "[ab].md").write_text("literal file\n", encoding="utf-8")
        present_result = writer.commit_paths(
            ["50_Daily/[ab].md"],
            allowed_roots=ALLOWED_ROOTS,
            message="literal path commit",
        )

        committed = subprocess.run(
            ["git", "-C", str(self.vault), "show", "--pretty=", "--name-only", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.splitlines()
        untracked = subprocess.run(
            ["git", "-C", str(self.vault), "ls-files", "--others", "--exclude-standard"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.splitlines()
        self.assertEqual(present_result, {"enabled": True, "committed": True})
        self.assertEqual(committed, ["50_Daily/[ab].md"])
        self.assertEqual(untracked, ["50_Daily/a.md", "50_Daily/b.md"])

    def test_commit_paths_decodes_chinese_git_output_as_utf8(self):
        self._initialize_git_repository()
        subprocess.run(["git", "-C", str(self.vault), "config", "core.quotePath", "false"], check=True)
        dashboard = self.vault / "Dashboards"
        dashboard.mkdir()
        (dashboard / "交易预案.md").write_text("# 交易预案\n", encoding="utf-8")

        result = self._writer(auto_git_enabled=True).commit_paths(
            ["Dashboards/交易预案.md"],
            allowed_roots=ALLOWED_ROOTS,
            message="Chinese path commit",
        )

        committed = subprocess.run(
            ["git", "-C", str(self.vault), "show", "--pretty=", "--name-only", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        ).stdout.splitlines()
        self.assertEqual(result, {"enabled": True, "committed": True})
        self.assertEqual(committed, ["Dashboards/交易预案.md"])


if __name__ == "__main__":
    unittest.main()
