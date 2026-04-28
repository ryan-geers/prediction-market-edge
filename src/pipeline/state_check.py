"""Verify DuckDB pipeline state exists and is recent enough for weekly digests or CI gates."""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb


def check_state_freshness(db_path: Path, *, max_run_age_days: int) -> int:
    """
    Return 0 if ``db_path`` exists, ``run_manifest`` is non-empty, and the latest
    ``COALESCE(completed_at_utc, started_at_utc)`` is within ``max_run_age_days``.

    On failure, prints a short explanation to stderr and returns 1.
    """
    if not db_path.is_file():
        print(f"check-state: ERROR — DuckDB not found at {db_path}", file=sys.stderr)
        print(
            "  Fix: run `pme run` locally, or run the **Run Pipeline** GitHub workflow "
            "and confirm the `pme-state` artifact contains `data/pme.duckdb`.",
            file=sys.stderr,
        )
        return 1

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        try:
            n = con.execute("SELECT COUNT(*) FROM run_manifest").fetchone()[0]
        except Exception as exc:
            print(f"check-state: ERROR — cannot read run_manifest: {exc}", file=sys.stderr)
            return 1
        if int(n) == 0:
            print("check-state: ERROR — run_manifest is empty (no pipeline runs recorded).", file=sys.stderr)
            print(
                "  Fix: run **Run Pipeline** at least once so weekly digest has real history.",
                file=sys.stderr,
            )
            return 1

        row = con.execute(
            "SELECT MAX(COALESCE(completed_at_utc, started_at_utc)) FROM run_manifest"
        ).fetchone()
        last = row[0] if row else None
        if last is None:
            print("check-state: ERROR — no run timestamps in run_manifest.", file=sys.stderr)
            return 1

        if isinstance(last, str):
            last = datetime.fromisoformat(last.replace("Z", "+00:00"))
        if not isinstance(last, datetime):
            print(f"check-state: ERROR — unexpected timestamp type: {type(last)}", file=sys.stderr)
            return 1
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        else:
            last = last.astimezone(timezone.utc)

        now = datetime.now(timezone.utc)
        age = now - last
        limit = timedelta(days=max_run_age_days)
        if age > limit:
            print(
                f"check-state: ERROR — last pipeline activity was {age.days}d ago "
                f"(limit {max_run_age_days}d). Latest: {last.isoformat()}",
                file=sys.stderr,
            )
            print(
                "  Fix: run **Run Pipeline** on schedule or manually; ensure artifact upload succeeds.",
                file=sys.stderr,
            )
            return 1
    finally:
        con.close()

    print(f"check-state: OK — last activity {last.isoformat()} (age {age.total_seconds() / 3600.0:.1f}h)")
    return 0
