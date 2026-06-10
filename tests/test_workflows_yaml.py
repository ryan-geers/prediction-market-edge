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


def test_run_pipeline_serializes_shared_state_cache() -> None:
    data = yaml.safe_load((WORKFLOWS / "run-pipeline.yml").read_text(encoding="utf-8"))

    assert data["concurrency"] == {
        "group": "pme-state",
        "cancel-in-progress": False,
    }


def test_db_audit_step_preserves_nonzero_exit_for_annotation() -> None:
    data = yaml.safe_load((WORKFLOWS / "run-pipeline.yml").read_text(encoding="utf-8"))
    steps = data["jobs"]["audit"]["steps"]
    audit_step = next(step for step in steps if step.get("id") == "audit")
    run_script = audit_step["run"]

    assert run_script.index("set +e") < run_script.index("python scripts/audit_db.py")
    assert run_script.index("audit_exit=$?") < run_script.index("set -e")
    assert 'echo "audit_exit=$audit_exit" >> "$GITHUB_OUTPUT"' in run_script
