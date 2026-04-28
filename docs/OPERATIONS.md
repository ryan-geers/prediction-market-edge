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

Retention defaults to GitHub’s policy unless you set `retention-days` on upload steps (already set on pipeline state where applicable).

## Local recovery

1. Download latest `pme-state` from Actions.
2. Extract under `prediction-market-edge/data/` preserving `pme.duckdb` and `reports/`.
3. Run `pme weekly-digest` / `streamlit run src/dashboard/streamlit_app.py` as needed.

## Scheduling

- Edit cron expressions in `.github/workflows/*.yml`. All times are **UTC**.
- After changing schedules, watch one triggered run and confirm artifact upload and (if enabled) email.
