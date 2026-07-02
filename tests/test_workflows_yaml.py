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


def test_run_pipeline_audit_captures_nonzero_exit_before_annotation() -> None:
    workflow = yaml.safe_load((WORKFLOWS / "run-pipeline.yml").read_text(encoding="utf-8"))
    audit_step = next(
        step
        for step in workflow["jobs"]["audit"]["steps"]
        if step.get("name") == "Run DB audit"
    )
    run = audit_step["run"]
    assert "set +e" in run
    assert "audit_exit=$?" in run
    assert 'echo "audit_exit=${audit_exit}" >> "$GITHUB_OUTPUT"' in run
