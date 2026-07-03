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


def test_run_pipeline_audit_step_captures_exit_before_reenabling_errexit() -> None:
    data = yaml.safe_load((WORKFLOWS / "run-pipeline.yml").read_text(encoding="utf-8"))
    audit_steps = data["jobs"]["audit"]["steps"]
    audit_step = next(step for step in audit_steps if step.get("id") == "audit")
    run_script = audit_step["run"]

    assert "set +e" in run_script
    assert 'audit_exit="$?"' in run_script
    assert "set -e" in run_script
    assert 'echo "audit_exit=$audit_exit" >> "$GITHUB_OUTPUT"' in run_script
    assert run_script.index("set +e") < run_script.index("python scripts/audit_db.py")
    assert run_script.index("python scripts/audit_db.py") < run_script.index('audit_exit="$?"')
    assert run_script.index('audit_exit="$?"') < run_script.index("set -e")
