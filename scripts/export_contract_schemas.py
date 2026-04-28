#!/usr/bin/env python3
"""Write Pydantic JSON Schemas to contracts/json for Java and other consumers. Run after changing models."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.core.schemas import (  # noqa: E402
    MarketSnapshotRecord,
    ModelForecastRecord,
    PaperOrderRecord,
    PaperPositionRecord,
    RunManifest,
    SignalRecord,
)

MODELS = {
    "signal_record": SignalRecord,
    "paper_order": PaperOrderRecord,
    "paper_position": PaperPositionRecord,
    "market_snapshot": MarketSnapshotRecord,
    "run_manifest": RunManifest,
    "model_forecast": ModelForecastRecord,
}


def main() -> None:
    out_dir = ROOT / "contracts" / "json"
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, model in MODELS.items():
        path = out_dir / f"{name}.schema.json"
        path.write_text(json.dumps(model.model_json_schema(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print("wrote", path.relative_to(ROOT))


if __name__ == "__main__":
    main()
