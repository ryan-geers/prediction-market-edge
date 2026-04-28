from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_env: str = "dev"
    log_level: str = "INFO"
    data_dir: Path = Path("data")
    duckdb_path: Path = Path("data/pme.duckdb")
    edge_threshold_bps: int = 300

    paper_default_qty: float = 1.0  # overridden at runtime when paper_bankroll is set
    paper_bankroll: float = 500.0
    paper_position_size_pct: float = 0.05  # fraction of bankroll per position (e.g. 0.05 = $25 on $500)
    paper_slippage_bps: float = 25.0
    paper_fees_assumption_bps: float = 0.0
    paper_fill_rule: str = "aggressive_touch"
    paper_assumption_version: str = "paper_exec_v1"
    paper_slippage_model_name: str = "linear_bps_on_touch"
    paper_eod_close: bool = False

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
