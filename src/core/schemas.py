from datetime import datetime, timezone
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, Field


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class SignalRecord(BaseModel):
    signal_id: str = Field(default_factory=lambda: str(uuid4()))
    run_id: str
    thesis_module: str
    venue: str
    contract_id: str
    contract_label: str
    event_time_utc: datetime = Field(default_factory=utc_now)
    ingested_at_utc: datetime = Field(default_factory=utc_now)
    model_probability: float
    market_implied_probability: float
    edge_bps: float
    bid_price: float
    ask_price: float
    spread_bps: float
    vig_adjusted_threshold_bps: float
    decision: Literal["enter_long_yes", "enter_long_no", "hold", "reject"]
    decision_reason: str
    model_version: str
    feature_set_version: str
    assumption_version: str


class PaperOrderRecord(BaseModel):
    paper_order_id: str = Field(default_factory=lambda: str(uuid4()))
    signal_id: str
    run_id: str
    venue: str
    contract_id: str
    side: Literal["yes", "no"]
    order_type: Literal["market", "limit"]
    qty: float
    limit_price: float | None = None
    fill_price: float | None = None
    fill_qty: float = 0.0
    status: Literal["submitted", "filled", "cancelled"] = "submitted"
    submitted_at_utc: datetime = Field(default_factory=utc_now)
    filled_at_utc: datetime | None = None
    cancelled_at_utc: datetime | None = None
    fill_rule: str = "midpoint"
    slippage_assumption_bps: float = 0.0
    fees_assumption_bps: float = 0.0
    assumption_version: str = "paper_exec_v1"
    slippage_model_name: str = "linear_bps_on_touch"


class PaperPositionRecord(BaseModel):
    position_id: str = Field(default_factory=lambda: str(uuid4()))
    run_id: str = ""
    signal_id: str = ""
    venue: str
    contract_id: str
    opened_at_utc: datetime = Field(default_factory=utc_now)
    closed_at_utc: datetime | None = None
    net_qty: float
    avg_entry_price: float
    avg_exit_price: float | None = None
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    mark_price: float | None = None
    last_mark_time_utc: datetime | None = None
    status: Literal["open", "closed"] = "open"
    close_reason: str | None = None
    # Added in Phase 2 (required for exit-flip logic); used for dedup in Phase 3.
    direction: Literal["yes", "no"] | None = None


class PositionClose(BaseModel):
    """Captures the data needed to close a paper position."""

    position_id: str
    avg_exit_price: float
    realized_pnl: float
    close_reason: Literal["signal_flip", "stop_loss", "contract_settled", "manual"]
    closed_at_utc: datetime = Field(default_factory=utc_now)


class PositionMark(BaseModel):
    """Lightweight mark used to re-price open positions each pipeline run."""

    contract_id: str
    venue: str
    mark_price: float
    last_mark_time_utc: datetime = Field(default_factory=utc_now)


class AddToPosition(BaseModel):
    """
    VWAP-merged update for an existing open position (Phase 3 dedup).
    Caller pre-computes new_net_qty and new_avg_entry_price so storage stays simple.
    """

    position_id: str
    new_net_qty: float
    new_avg_entry_price: float
    merged_at_utc: datetime = Field(default_factory=utc_now)


class MarketSnapshotRecord(BaseModel):
    snapshot_id: str = Field(default_factory=lambda: str(uuid4()))
    venue: str
    contract_id: str
    snapshot_time_utc: datetime = Field(default_factory=utc_now)
    best_bid: float
    best_ask: float
    last_trade: float | None = None
    mid_price: float
    spread_bps: float
    orderbook_depth_json: str | None = None
    source_latency_ms: int = 0


class RunManifest(BaseModel):
    run_id: str
    code_commit_sha: str
    config_hash: str
    active_thesis: str
    data_sources: str
    started_at_utc: datetime = Field(default_factory=utc_now)
    completed_at_utc: datetime | None = None


class ModelForecastRecord(BaseModel):
    forecast_id: str = Field(default_factory=lambda: str(uuid4()))
    run_id: str
    thesis_module: str
    release_date_utc: datetime
    model_probability: float
    target_metric: str
    model_version: str
    feature_set_version: str
    training_start_utc: datetime
    training_end_utc: datetime
    validation_rmse: float
    validation_mae: float
    created_at_utc: datetime = Field(default_factory=utc_now)
