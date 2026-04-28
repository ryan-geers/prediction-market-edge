# Operations — GitHub Actions, secrets, artifacts, email

## Workflows

| Workflow | Purpose |
|---------|---------|
| `run-pipeline.yml` | Periodic / manual pipeline; restores `pme-state`, runs `pme run`, reports, uploads DuckDB + reports |
| `weekly-summary-email.yml` | Weekly digest (Markdown + HTML), artifact `pme-weekly-digest`, optional SMTP |

## Secrets (repository)

**Pipeline / data APIs (optional but recommended for live data):** `FRED_API_KEY`, `BLS_API_KEY`, `BEA_API_KEY`, `KALSHI_API_KEY` — set as GitHub Actions secrets; exposed as env in `run-pipeline.yml`.

**Weekly email:** `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`, `EMAIL_FROM`, `EMAIL_TO`.

Rotate SMTP credentials on your provider after a leak or personnel change; update secrets and re-run the workflow manually to verify delivery.

## Repository variables

- **`PME_EMAIL_DRY_RUN`**: set to `true` to skip SMTP on **scheduled** weekly runs while still generating artifacts.
- Manual `workflow_dispatch` on the weekly workflow also exposes a **Dry run** checkbox.
- **`PME_SKIP_WEEKLY_FRESHNESS_CHECK`**: set to `true` to bypass `pme check-state` in the weekly job (not recommended for production).

## Weekly digest freshness gate

After restoring `pme-state`, the weekly workflow runs `pme check-state --max-run-age-days 7`. The job **fails** if `data/pme.duckdb` is missing, `run_manifest` is empty, or the last run timestamp is older than 7 days—so you do not get a silent empty digest or email when the main pipeline has stopped uploading state.

**Bypass:** use workflow_dispatch **Skip freshness check**, or set `PME_SKIP_WEEKLY_FRESHNESS_CHECK=true`.

## Artifacts

- **`pme-state`**: `data/pme.duckdb`, `data/reports` — continuity across ephemeral runners.
- **`pme-weekly-digest`**: `weekly_digest.md`, `weekly_digest.html` — audit trail and email source.

Retention defaults to GitHub’s policy unless you set `retention-days` on upload steps (**`pme-state`** uploads use **30 days** in `run-pipeline.yml`).

## How state persists in CI (no PR required)

Runners are ephemeral. Each **Run Pipeline** job:

1. **Downloads** the latest **`pme-state`** artifact into `data/` (DuckDB + `data/reports`).
2. Runs **`pme run`**, which **appends** new rows to `pme.duckdb` and writes new `run_report_<run_id>.md` / `.html`.
3. **Uploads** the whole `data/pme.duckdb` and `data/reports` tree again as **`pme-state`**.

So the **source of truth** for history is **`pme.duckdb`** inside the artifact; Markdown files are human-readable mirrors. You do not need commits or PRs for the pipeline to accumulate state.

**Optional:** open a PR each run (e.g. commit `data/` to a branch with a **PAT**). That duplicates what artifacts already do—useful only if you want **git diffs** or a public static site; otherwise artifacts + DuckDB are simpler.

## Run report file growth

Each run adds `run_report_<uuid>.{md,html}`. The workflow runs **`pme prune-reports --keep 48`** before upload so long CI history does not grow the artifact without bound. Older per-run files are dropped from the bundle; **closed-trade and signal history remain in DuckDB**. To change retention, edit `--keep` in `run-pipeline.yml`.

## Local recovery

1. Download latest `pme-state` from Actions.
2. Extract under your project’s `data/` preserving `pme.duckdb` and `reports/`.
3. Run `pme weekly-digest` / `streamlit run src/dashboard/streamlit_app.py` as needed.

## Scheduling

- Edit cron expressions in `.github/workflows/*.yml`. All times are **UTC**.
- After changing schedules, watch one triggered run and confirm artifact upload and (if enabled) email.
