# Pre-flight checklist (GitHub + local)

Use this before relying on **paper-trading history**, **weekly email**, or **Action artifacts**.

## Repository setup

- [ ] Push `prediction-market-edge` to GitHub (default branch matches your expected schedules).
- [ ] **Actions enabled** for the repo (Settings → Actions → General).
- [ ] **Workflow permissions**: checkout allowed; artifact **read/write** allowed for `run-pipeline.yml` uploads.

## Secrets (Actions)

**Pipeline (`run-pipeline.yml`)** — for live macro/market data where supported:

- [ ] `FRED_API_KEY`, `BLS_API_KEY`, `BEA_API_KEY` (as needed)
- [ ] `KALSHI_API_KEY` (if using Kalshi live; otherwise connectors may use fallbacks)

**Weekly email (`weekly-summary-email.yml`)**:

- [ ] `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`, `EMAIL_FROM`, `EMAIL_TO`

Optional until you trust email:

- [ ] `PME_EMAIL_DRY_RUN=true` (repo **variable**) or manual workflow **Dry run** = true

## Variables (optional)

- [ ] `PME_SKIP_WEEKLY_FRESHNESS_CHECK=true` — only if you intentionally want the weekly job to build a digest **without** a recent `run_manifest` (not recommended for production).

## First successful pipeline run

- [ ] Manually run **Run Pipeline** once; confirm job uploads **`pme-state`** with `data/pme.duckdb`.
- [ ] Locally: `pme check-state --max-run-age-days 7` exits **0** after that artifact is under `data/`.

## Schedules

- [ ] `run-pipeline.yml` cron meets your desired refresh (default: **05, 11, 17, 23** UTC daily ≈ midnight / 06 / 12 / 18 America/Chicago in **CDT**; ~1h earlier in each slot in **CST**).
- [ ] `weekly-summary-email.yml` cron meets your digest cadence (default: Saturday 13:00 UTC ≈ 08:00 America/Chicago in **CDT**; ≈ 07:00 in **CST**).

## Weekly job behavior

- The **weekly** workflow **restores `pme-state`** then runs **`pme check-state`** (unless skipped). If the DB is missing, empty, or **older than 7 days**, the job **fails** so you do not get a silent “empty” digest or email.
- After a green check, it generates the digest, uploads **`pme-weekly-digest`**, and sends email (unless dry-run).

## Local smoke (before trusting CI)

- [ ] `python -m venv .venv && source .venv/bin/activate`
- [ ] `pip install -e ".[dev]"` and `cp .env.example .env` (fill keys as needed)
- [ ] `pme run` — inspect `data/reports/run_report_*.md` and `data/pme.duckdb`
- [ ] `pytest`

## Compliance

- [ ] Confirm **Kalshi / other API terms** allow your usage (automated runs, paper vs live).
