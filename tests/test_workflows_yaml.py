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


def test_db_audit_step_captures_nonzero_exit_before_errexit() -> None:
    """WARNING/CRITICAL audit exits must still be exposed to later annotation."""
    data = yaml.safe_load((WORKFLOWS / "run-pipeline.yml").read_text(encoding="utf-8"))
    audit_steps = data["jobs"]["audit"]["steps"]
    run_audit = next(step for step in audit_steps if step.get("name") == "Run DB audit")
    script = run_audit["run"]

    assert "set +e" in script
    assert "audit_code=$?" in script
    assert 'echo "audit_exit=$audit_code" >> "$GITHUB_OUTPUT"' in script
    assert script.index("set +e") < script.index("python scripts/audit_db.py")
    assert script.index("audit_code=$?") > script.index("python scripts/audit_db.py")
