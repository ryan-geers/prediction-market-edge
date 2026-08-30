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


def test_pipeline_audit_preserves_nonzero_exit_for_annotation() -> None:
    workflow = (WORKFLOWS / "run-pipeline.yml").read_text(encoding="utf-8")

    assert "set +e\n          python scripts/audit_db.py" in workflow
    assert "audit_exit=$?" in workflow
    assert 'echo "audit_exit=$audit_exit" >> "$GITHUB_OUTPUT"' in workflow


def test_pipeline_audit_flags_failed_run_job_before_pass() -> None:
    workflow = (WORKFLOWS / "run-pipeline.yml").read_text(encoding="utf-8")

    assert 'RUN_RESULT="${{ needs.run.result }}"' in workflow
    assert 'if [ "$RUN_RESULT" != "success" ]; then' in workflow
    assert 'elif [ "$RUN_RESULT" = "success" ]; then' in workflow
