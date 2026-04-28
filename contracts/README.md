# Cross-Runtime Contracts

This directory stores portable schema contracts shared by Python and Java components.

## Machine-readable schemas

- **`json/*.schema.json`** — JSON Schema derived from Pydantic models in `src/core/schemas.py`.
- Regenerate after model changes:

  ```bash
  python scripts/export_contract_schemas.py
  ```

- CI tests assert these files match `model_json_schema()` so they cannot drift silently.

## Domains (v1)

- `signal_record`, `paper_order`, `paper_position`, `market_snapshot`, `run_manifest`, `model_forecast`

## Conventions

- Python is the source of truth for runtime objects and DuckDB DDL in `src/core/storage.py`.
- Breaking persisted shapes require version suffixing in filenames (e.g., `signal_record.v2.schema.json`) and migration strategy in storage code.
