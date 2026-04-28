# Python vs Java in this monorepo

## Default: Python

- Research, data ingest, feature engineering, modeling, signals, paper trading, reporting, and CI-driven batch runs are **Python-first**.
- DuckDB, Pydantic models, and `contracts/json` schemas are the shared source of truth for persisted rows.

## When to add Java (`services-java/`)

Use the Java scaffold when you need:

- **Long-lived HTTP APIs** with standard auth, rate limiting, and operational maturity beyond one-off scripts.
- **High-throughput or strictly typed workers** consuming the same contracts (generate DTOs from `contracts/json` or OpenAPI later).
- **Integration with JVM ecosystems** (enterprise messaging, existing Spring services).

## Shared contracts

- Do **not** duplicate field names or enums informally across languages.
- Evolve `src/core/schemas.py` and regenerate `contracts/json/*.schema.json` for any breaking change; version artifacts (`model_version`, `assumption_version`) in persisted rows.

## Pragmatic rule

Keep strategy and research loops in **Python** until you have a concrete requirement for 24/7 serving or JVM integration; then promote **thin**, well-contracted boundaries to Java and keep bulk analytics in Python.
