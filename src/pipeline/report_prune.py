"""Prune old per-run Markdown/HTML reports under data/reports (CI artifact growth)."""

from __future__ import annotations

import logging
from pathlib import Path

LOGGER = logging.getLogger(__name__)


def prune_run_reports(reports_dir: Path, *, keep: int = 48) -> tuple[int, int]:
    """
    Delete ``run_report_<uuid>.{md,html}`` beyond the ``keep`` newest by mtime.

    Preserves ``weekly_digest*.md``, ``paper_trading_latest.md``, etc.

    Returns ``(kept_md_pairs, removed_md_pairs)``.
    """
    reports_dir = reports_dir.resolve()
    if not reports_dir.is_dir():
        return 0, 0

    md_files = sorted(
        reports_dir.glob("run_report_*.md"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if len(md_files) <= keep:
        return len(md_files), 0

    removed = 0
    for p in md_files[keep:]:
        try:
            p.unlink(missing_ok=True)
            html = p.with_suffix(".html")
            html.unlink(missing_ok=True)
            removed += 1
        except OSError as exc:
            LOGGER.warning("Failed to prune report file %s: %s", p, exc)
    return keep, removed
