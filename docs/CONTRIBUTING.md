# Contributing

## Environment

- Python **3.11+**
- `python -m venv .venv && source .venv/bin/activate`
- `pip install -e ".[dev]"`

## Commands

- **Tests:** `pytest` (from repo root; `pythonpath` is set in `pyproject.toml`)
- **Lint (optional):** `ruff check src tests` (Pyflakes `F` rules in `pyproject.toml`)
- **State gate (local / CI):** `pme check-state --max-run-age-days 7`

## Adding a new thesis module

1. Create a package under `src/theses/<your_thesis>/` with a `module.py` that subclasses `ThesisModule` from `src/theses/base.py`.
2. Implement the contract:
   - `ingest()` → raw dict/lists (connector outputs).
   - `build_features(raw)` → feature bag for the model.
   - `forecast(features)` → dict with whatever downstream steps need (e.g. model probability, market handle).
   - `generate_signals(run_id, forecast)` → list of `SignalRecord` and `MarketSnapshotRecord`.
   - `paper_trade(signals)` → `PaperOrderRecord` and `PaperPositionRecord` lists (can delegate to `simulate_paper_trades` from `src/pipeline/paper_trading.py`).
   - Optionally override `build_forecast_records` for `model_forecasts` table rows.
3. Register the module in `src/theses/registry.py` `build_registry` with a stable snake_case key (used by `pme run --thesis <key>`).
4. Add unit tests under `tests/` (fixtures in `tests/fixtures/` when parsing external formats).
5. If you introduce new persisted fields, extend `src/core/schemas.py`, `src/core/storage.py` DDL + migrations, and re-run `python scripts/export_contract_schemas.py`, then commit updated `contracts/json/*.schema.json`.

## Contracts

- Runtime models live in `src/core/schemas.py`.
- Published JSON Schemas for Java/other consumers: `contracts/json/` — regenerate with `scripts/export_contract_schemas.py` whenever models change.
- CI asserts committed schemas match Pydantic `model_json_schema()` output.
