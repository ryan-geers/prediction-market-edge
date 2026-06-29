from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"


def _workflow(name: str) -> dict:
    return yaml.safe_load((WORKFLOWS / name).read_text(encoding="utf-8"))


def _step_by_name(job: dict, name: str) -> dict:
    return next(step for step in job["steps"] if step.get("name") == name)


@pytest.mark.parametrize(
    "name, expected_job",
    [
        ("run-pipeline.yml", "run"),
        ("weekly-summary-email.yml", "email-weekly-digest"),
    ],
)
def test_workflow_yaml_loads(name: str, expected_job: str) -> None:
    data = _workflow(name)
    assert "jobs" in data
    assert expected_job in data["jobs"]


def test_db_audit_captures_nonzero_exit_before_errexit_resumes() -> None:
    data = _workflow("run-pipeline.yml")
    audit_job = data["jobs"]["audit"]
    run_db_audit = _step_by_name(audit_job, "Run DB audit")

    run_script = run_db_audit["run"]

    assert run_db_audit["continue-on-error"] is True
    assert "set +e" in run_script
    assert "audit_exit=$?" in run_script
    assert "set -e" in run_script
    assert 'echo "audit_exit=$audit_exit" >> "$GITHUB_OUTPUT"' in run_script
    assert (
        run_script.index("set +e")
        < run_script.index("python scripts/audit_db.py")
        < run_script.index("audit_exit=$?")
        < run_script.index("set -e")
        < run_script.index('echo "audit_exit=$audit_exit" >> "$GITHUB_OUTPUT"')
    )


def test_db_audit_does_not_pass_when_pipeline_job_failed() -> None:
    data = _workflow("run-pipeline.yml")
    audit_job = data["jobs"]["audit"]
    annotate = _step_by_name(audit_job, "Annotate findings")

    run_script = annotate["run"]

    assert "needs.run.result" in run_script
    assert 'if [ "$RUN_RESULT" != "success" ]; then' in run_script
    assert "Pipeline run job did not succeed" in run_script
    assert run_script.index('if [ "$RUN_RESULT" != "success" ]; then') < run_script.index(
        "PASS::All audit checks passed."
    )
