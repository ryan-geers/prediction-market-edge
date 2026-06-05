import os
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"


@pytest.mark.parametrize(
    "name, expected_job",
    [
        ("run-pipeline.yml", "run"),
        ("weekly-summary-email.yml", "email-weekly-digest"),
    ],
)
def test_workflow_yaml_loads(name: str, expected_job: str) -> None:
    data = yaml.safe_load((WORKFLOWS / name).read_text(encoding="utf-8"))
    assert "jobs" in data
    assert expected_job in data["jobs"]


def test_db_audit_step_captures_critical_exit_under_errexit(tmp_path: Path) -> None:
    """GitHub runs workflow scripts with bash -e; audit exits must still be captured."""
    data = yaml.safe_load((WORKFLOWS / "run-pipeline.yml").read_text(encoding="utf-8"))
    audit_steps = data["jobs"]["audit"]["steps"]
    audit_step = next(step for step in audit_steps if step.get("name") == "Run DB audit")
    script = audit_step["run"]

    audit_command = """python scripts/audit_db.py \\
  --db data/pme.duckdb \\
  --out data/reports/audit_latest.md \\
  --json data/reports/audit_latest.json \\
  --github-summary"""
    script = script.replace(audit_command, "python3 -c 'import sys; sys.exit(2)'")
    assert "scripts/audit_db.py" not in script

    github_output = tmp_path / "github_output"
    result = subprocess.run(
        ["bash", "-e", "-c", script],
        cwd=tmp_path,
        env={**os.environ, "GITHUB_OUTPUT": str(github_output)},
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert github_output.read_text(encoding="utf-8").strip() == "audit_exit=2"
