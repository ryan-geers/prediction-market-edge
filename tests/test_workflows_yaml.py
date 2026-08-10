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


def test_run_pipeline_guards_empty_duckdb_cache_miss() -> None:
    """Cache miss must not create empty DuckDB then overwrite pme-db-latest.

    Regression lock for: restore-only cache → history/artifact guard → persist gate
    before cache/save and pme-db-latest overwrite.
    """
    text = (WORKFLOWS / "run-pipeline.yml").read_text(encoding="utf-8")
    data = yaml.safe_load(text)
    run_job = data["jobs"]["run"]
    steps = run_job["steps"]
    names = [s.get("name", "") for s in steps]

    assert "Ensure DuckDB history before mutating state" in names
    assert "Verify DuckDB still has history before persisting" in names

    restore = next(s for s in steps if s.get("name") == "Restore prior data")
    assert restore["uses"] == "actions/cache/restore@v4"

    # Combined actions/cache@v4 auto-saves on primary-key miss and would poison state.
    run_uses = [s.get("uses", "") for s in steps if "uses" in s]
    assert "actions/cache@v4" not in run_uses
    assert "actions/cache/restore@v4" in run_uses
    assert "actions/cache/save@v4" in run_uses

    guard = next(s for s in steps if s.get("name") == "Ensure DuckDB history before mutating state")
    guard_run = guard["run"]
    assert "pme-db-latest" in guard_run
    assert "PME_ALLOW_EMPTY_DB_BOOTSTRAP" in guard_run
    assert "consolidate-positions" not in guard_run  # must run only after guard

    persist = next(s for s in steps if s.get("name") == "Verify DuckDB still has history before persisting")
    assert persist["id"] == "db_persist_ok"

    save = next(s for s in steps if s.get("name") == "Save state to cache")
    assert "db_persist_ok.outputs.persist" in str(save.get("if", ""))

    upload = next(s for s in steps if s.get("name") == "Upload rolling DB snapshot")
    assert "db_persist_ok.outputs.persist" in str(upload.get("if", ""))
    assert upload["with"].get("overwrite") is True
    assert upload["with"]["name"] == "pme-db-latest"

    # Guard must appear before consolidate / run so Storage.__init__ cannot wipe first.
    assert names.index("Ensure DuckDB history before mutating state") < names.index(
        "Consolidate duplicate positions"
    )
