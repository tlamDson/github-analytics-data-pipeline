"""Static checks on the gold database setting and .github/workflows/motherduck-transient.yml.

The setting (repository variable ``MOTHERDUCK_DATABASE``) must reach the three workflows that read
or write gold. The MotherDuck steps of pages.yml and data-quality.yml run for real in bash, with a
stub ``python`` that prints its arguments.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"
MIGRATION = WORKFLOWS / "motherduck-transient.yml"
SETTING = "${{ vars.MOTHERDUCK_DATABASE }}"
READERS = [("pages.yml", "--db"), ("data-quality.yml", "--database")]


def load(name: str) -> dict:
    path = WORKFLOWS / name
    assert path.is_file(), f"missing workflow file {path}"
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def motherduck_step(name: str) -> dict:
    found = [
        step
        for job in load(name)["jobs"].values()
        for step in job["steps"]
        if "md:" in step.get("run", "")
    ]
    assert len(found) == 1, (name, [s.get("name") for s in found])
    return found[0]


def bash() -> str:
    found = shutil.which("bash")
    if found is None or (sys.platform == "win32" and "git" not in found.lower()):
        pytest.skip("needs bash (on Windows: Git Bash, not the WSL bash.exe)")
    return found


# ---------------------------------------------------------------- the setting


@pytest.mark.parametrize(("name", "flag"), READERS)
@pytest.mark.parametrize(
    ("setting", "expected"),
    [("", "md:repopulse"), ("repopulse_transient", "md:repopulse_transient")],
)
def test_readers_open_the_database_of_the_setting(name, flag, setting, expected):
    script = 'python() { printf "%s\\n" "$@"; }\n' + motherduck_step(name)["run"]
    proc = subprocess.run(
        [bash(), "-eo", "pipefail", "-c", script],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "MOTHERDUCK_DATABASE": setting, "GITHUB_SHA": "abc"},
    )
    assert proc.returncode == 0, proc.stderr
    argv = proc.stdout.split()
    assert argv.count(flag) == 1, argv
    assert argv[argv.index(flag) + 1] == expected


@pytest.mark.parametrize("name", [r[0] for r in READERS])
def test_readers_get_the_setting_from_the_repository_variable(name):
    assert motherduck_step(name)["env"].get("MOTHERDUCK_DATABASE") == SETTING


def test_pipeline_gets_the_same_setting_but_never_on_a_pull_request():
    (job,) = load("pipeline.yml")["jobs"].values()
    assert job["env"].get("MOTHERDUCK_DATABASE") == (
        "${{ github.event_name != 'pull_request' && vars.MOTHERDUCK_DATABASE || '' }}"
    )


# ---------------------------------------------------------------- migration workflow


def migration() -> dict:
    return load(MIGRATION.name)


def migration_job() -> dict:
    (job,) = migration()["jobs"].values()
    return job


def test_migration_is_dispatch_only():
    data = migration()
    on = data.get("on", data.get(True))
    assert set(on) == {"workflow_dispatch"}
    inputs = on["workflow_dispatch"]["inputs"]
    assert inputs["source"]["default"] == "repopulse"
    assert inputs["target"]["default"] == "repopulse_transient"


def test_migration_never_runs_while_the_pipeline_writes():
    group = migration()["concurrency"]
    assert group["group"] == "pipeline-motherduck"
    assert group["group"] in load("pipeline.yml")["concurrency"]["group"]
    assert group["cancel-in-progress"] is False


def test_migration_runs_the_copy_with_the_token_only_in_that_step():
    steps = [s for s in migration_job()["steps"] if "repopulse.golddb" in s.get("run", "")]
    assert len(steps) == 1
    step = steps[0]
    assert "python -m repopulse.golddb migrate" in step["run"]
    assert step["env"]["MOTHERDUCK_TOKEN"] == "${{ secrets.MOTHERDUCK_TOKEN }}"
    assert "MOTHERDUCK_TOKEN" not in (migration_job().get("env") or {})
    assert "continue-on-error" not in step
    assert step["run"].strip().startswith('if [ -z "$MOTHERDUCK_TOKEN" ]; then')


def test_migration_uploads_nothing_and_never_backs_up_or_drops():
    text = "\n".join(
        line
        for line in MIGRATION.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("#")
    )
    assert "upload-" not in text
    assert "golddb backup" not in text
    assert not re.search(r"\b(drop|alter|parquet)\b", text, re.I)


def test_migration_permissions_timeout_pin_and_no_interpolation():
    assert migration()["permissions"] == {"contents": "read"}
    assert migration_job()["timeout-minutes"] <= 30
    pin = re.search(r'"duckdb==[0-9.]+"', (WORKFLOWS / "pipeline.yml").read_text(encoding="utf-8"))
    runs = [s.get("run", "") for s in migration_job()["steps"]]
    assert any(pin.group(0) in r for r in runs)
    assert not [r for r in runs if "${{" in r]


# ---------------------------------------------------------------- docs


def test_docs_have_the_steps_rollback_backup_and_the_waiting_approvals():
    text = (ROOT / "docs" / "motherduck-storage.md").read_text(encoding="utf-8")
    for needle in (
        "repopulse.golddb backup --database md:repopulse",
        "gh workflow run motherduck-transient.yml",
        "gh variable set MOTHERDUCK_DATABASE",
        "## Rollback",
        "DROP DATABASE",
        "failsafe_bytes",
    ):
        assert needle in text, needle
