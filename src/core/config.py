from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_env: str = "dev"
    log_level: str = "INFO"
    data_dir: Path = Path("data")
    duckdb_path: Path = Path("data/pme.duckdb")
    edge_threshold_bps: int = 300

    #: If True, never open a long NO when the model still thinks YES is more likely
    #: than not (P(YES) > 50%). Suppresses "value" trades that buy NO while the
    #: event remains the model's modal outcome — e.g. model 81% vs market 98% on YES.
    signal_block_long_no_when_model_favors_yes: bool = False

    paper_default_qty: float = 1.0  # overridden at runtime when paper_bankroll is set
    paper_bankroll: float = 500.0
    paper_position_size_pct: float = 0.05  # fraction of bankroll per position (e.g. 0.05 = $25 on $500)
    paper_slippage_bps: float = 25.0
    paper_fees_assumption_bps: float = 0.0
    paper_fill_rule: str = "aggressive_touch"
    paper_assumption_version: str = "paper_exec_v1"
    paper_slippage_model_name: str = "linear_bps_on_touch"
    paper_eod_close: bool = False

    # Phase 2 exit knobs
    paper_exit_on_flip: bool = True          # close when signal reverses direction
    paper_stop_loss_pct: float | None = None # e.g. 0.15 = close at -15% of cost basis; None = disabled
    paper_close_on_settle: bool = True       # close when connector reports contract settled

    # Phase 3 dedup knobs
    paper_allow_add_to_position: bool = False
    # Maximum number of open positions allowed per (contract_id, venue, direction) key.
    # Enforced in apply_dedup() regardless of whether individual positions have a null
    # direction (which would otherwise make them invisible to the key-based dedup dict).
    paper_max_open_per_key: int = 1
    # Hard cap on total open positions across the entire portfolio.
    # When this many positions are already open, new entries are skipped (highest-edge
    # candidates are admitted first because signals are sorted by |edge_bps| before dedup).
    # 0 = unlimited (original behaviour).
    # NOTE: set generously (200+) for paper-trading across many CPI/UNRATE strikes;
    # the per-key guard (paper_max_open_per_key=1) handles contract-level dedup.
    paper_max_total_open: int = 200

    #: Max open positions sharing the same Kalshi-style series prefix
    #: (text before the first "-", e.g. KXCPI, KXU3, CPI). Reduces one-factor CPI
    #: ladders from crowding the book. 0 = disabled.
    paper_max_open_per_contract_family: int = 15

    #: Skip signal-flip / stop-loss exits when the current quote is not executable
    #: (e.g. bid=0 so a long YES cannot realistically be sold).
    paper_skip_exit_on_unreliable_quote: bool = True

    # Quote sanity — avoid (bid=0, ask=1) → mid=0.50 phantom marks.
    market_min_bid_for_quote: float = 0.01
    market_min_ask_for_quote: float = 0.01
    market_max_spread_bps: float = 1500.0
    #: Hard ceiling above which no last_trade rescue applies — contracts with spread
    #: wider than this are completely illiquid and cannot be executed at any price.
    #: Default 10,000 bps (100% of mid). KXU3/KXECONSTATU3 contracts with empty
    #: books often show spread=20,000 bps, which the last_trade path would
    #: incorrectly treat as signal-quality.
    market_max_spread_bps_hard: float = 10_000.0
    #: When bid is missing, treat ask above this as a broken empty book (no mid).
    market_max_one_sided_ask: float = 0.85

    # Set True in CI / production to write run reports and model artifact files.
    # Leave False (default) for local runs to avoid cluttering data/ with files every invocation.
    save_run_artifacts: bool = False

    # Fallback UNRATE threshold when it cannot be parsed from the Kalshi ticker.
    unemployment_threshold_pct: float = 4.2

    # Kalshi RSA auth — key_id is the UUID from the Kalshi dashboard;
    # kalshi_api_key holds the PEM private key used to sign requests.
    kalshi_key_id: str | None = None
    kalshi_api_key: str | None = None
    polymarket_api_key: str | None = None
    fred_api_key: str | None = None
    bls_api_key: str | None = None
    bea_api_key: str | None = None

    # Weekly email (optional); also read from env in GitHub Actions
    email_dry_run: bool = False
    smtp_host: str | None = None
    smtp_port: int = 587
    smtp_user: str | None = None
    smtp_pass: str | None = None
    email_from: str | None = None
    email_to: str | None = None

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)


@lru_cache
def get_settings() -> Settings:
    settings = Settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    return settings
