import argparse
from pathlib import Path

from src.core.config import get_settings
from src.pipeline.email_digest import send_weekly_digest_email
from src.pipeline.reporting import (
    generate_paper_trade_report,
    generate_weekly_digest,
    generate_weekly_digest_html,
)
from src.pipeline.run import run_backfill, run_pipeline
from src.pipeline.report_prune import prune_run_reports
from src.pipeline.state_check import check_state_freshness


def main() -> None:
    parser = argparse.ArgumentParser(prog="pme")
    sub = parser.add_subparsers(dest="command", required=True)

    run_parser = sub.add_parser("run", help="Run thesis pipeline")
    run_parser.add_argument("--thesis", default="economic_indicators")

    digest_parser = sub.add_parser("weekly-digest", help="Generate weekly digest report (Markdown; optional HTML)")
    digest_parser.add_argument("--output", default="data/reports/weekly_digest.md")
    digest_parser.add_argument("--html-output", default=None, help="Also write HTML digest to this path")

    send_email = sub.add_parser(
        "send-weekly-email",
        help="Send weekly digest Markdown via SMTP (uses .env or env vars; respects EMAIL_DRY_RUN)",
    )
    send_email.add_argument("--markdown-file", default="data/reports/weekly_digest.md")
    send_email.add_argument("--subject", default="Prediction Market Edge — weekly digest")
    send_email.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview message and skip SMTP (overrides env for this invocation)",
    )

    backfill_parser = sub.add_parser("backfill", help="Run pipeline multiple times in sequence (state accrual / CI)")
    backfill_parser.add_argument("--thesis", default="economic_indicators")
    backfill_parser.add_argument("--iterations", type=int, default=1)

    paper_report_parser = sub.add_parser("paper-report", help="Generate paper trading report from DuckDB")
    paper_report_parser.add_argument("--run-id", default=None, help="Filter to a single run_id, or omit for all")
    paper_report_parser.add_argument("--output", default="data/reports/paper_trading_report.md")

    prune_reports_parser = sub.add_parser(
        "prune-reports",
        help="Delete older run_report_<uuid>.{md,html} files (keeps artifact size bounded in CI)",
    )
    prune_reports_parser.add_argument(
        "--dir",
        type=Path,
        default=Path("data/reports"),
        help="Reports directory (default: data/reports)",
    )
    prune_reports_parser.add_argument(
        "--keep",
        type=int,
        default=48,
        help="Keep the newest N per-run Markdown reports by mtime (default: 48)",
    )

    check_parser = sub.add_parser(
        "check-state",
        help="Exit 0 if DuckDB exists and run_manifest has activity within --max-run-age-days",
    )
    check_parser.add_argument(
        "--duckdb",
        type=Path,
        default=None,
        help="Path to pme.duckdb (default: DUCKDB_PATH from settings)",
    )
    check_parser.add_argument(
        "--max-run-age-days",
        type=int,
        default=7,
        help="Fail if last COALESCE(completed_at_utc, started_at_utc) is older than this",
    )

    args = parser.parse_args()
    if args.command == "run":
        run_id, report_path = run_pipeline(thesis_name=args.thesis)
        print(f"run_id={run_id}")
        print(f"report_path={report_path}")
    elif args.command == "weekly-digest":
        settings = get_settings()
        text = generate_weekly_digest(str(settings.duckdb_path))
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text)
        print(f"digest_path={output}")
        if args.html_output:
            html_path = Path(args.html_output)
            html_path.parent.mkdir(parents=True, exist_ok=True)
            html_path.write_text(generate_weekly_digest_html(str(settings.duckdb_path)))
            print(f"digest_html_path={html_path}")
    elif args.command == "send-weekly-email":
        settings = get_settings()
        if args.dry_run:
            settings = settings.model_copy(update={"email_dry_run": True})
        body = Path(args.markdown_file).read_text(encoding="utf-8")
        send_weekly_digest_email(body, settings, subject=args.subject)
        print("send-weekly-email: done")
    elif args.command == "backfill":
        run_ids = run_backfill(thesis_name=args.thesis, iterations=args.iterations)
        print("run_ids=" + ",".join(run_ids))
    elif args.command == "paper-report":
        settings = get_settings()
        text = generate_paper_trade_report(str(settings.duckdb_path), run_id=args.run_id)
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text)
        print(f"paper_report_path={output}")
    elif args.command == "prune-reports":
        kept, removed = prune_run_reports(args.dir, keep=args.keep)
        print(f"prune-reports: kept={kept} removed_pairs={removed} dir={args.dir.resolve()}")
    elif args.command == "check-state":
        settings = get_settings()
        db = args.duckdb if args.duckdb is not None else settings.duckdb_path
        raise SystemExit(check_state_freshness(db, max_run_age_days=args.max_run_age_days))


if __name__ == "__main__":
    main()
