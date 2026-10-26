"""Smoke tests the packaged tdei-copy command line interface."""

from __future__ import annotations

import subprocess

FLAGS = ("--dry-run", "--service-map", "--default-service", "--report")


def run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([*args], capture_output=True, text=True)


def test_tdei_copy_entry_point_exits_zero_when_given_help() -> None:
    proc = run_cli("tdei-copy", "--help")
    assert proc.returncode == 0, proc.stderr


def test_help_lists_copy_flags() -> None:
    proc = run_cli("tdei-copy", "--help")
    for flag in FLAGS:
        assert flag in proc.stdout, f"{flag} missing from --help output"


def test_missing_required_arguments_returns_non_zero_exit_code() -> None:
    proc = run_cli("tdei-copy", "--source-env", "dev")
    assert proc.returncode != 0
