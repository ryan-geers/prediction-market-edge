import os
import tempfile
import time
from pathlib import Path

from src.pipeline.report_prune import prune_run_reports


def test_prune_run_reports_keeps_newest():
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        kept_n = 3
        for i in range(5):
            p = base / f"run_report_{i:032x}.md"
            p.write_text("x", encoding="utf-8")
            p.with_suffix(".html").write_text("<!html>", encoding="utf-8")
            mtime = 1_700_000_000 + i * 100
            os.utime(p, (mtime, mtime))
            os.utime(p.with_suffix(".html"), (mtime, mtime))
            time.sleep(0.001)

        k, r = prune_run_reports(base, keep=kept_n)
        assert k == kept_n
        assert r == 2
        assert len(list(base.glob("run_report_*.md"))) == kept_n
