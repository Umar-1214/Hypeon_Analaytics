"""Application config from environment. Load .env before importing this (e.g. in app.py)."""
from pathlib import Path
from typing import List

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _env_path(v: str | None) -> Path | None:
    if not v or not v.strip():
        return None
    return Path(v).resolve()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database
    database_url: str = "postgresql://postgres:postgres@127.0.0.1:5433/hypeon"

    # Data
    data_raw_dir: str = "data/raw"

    # API
    api_key: str | None = None  # Optional; if set, X-API-Key or Authorization: Bearer required

    # CORS (comma-separated origins; default dev)
    cors_origins: str = "http://localhost:5173,http://localhost:3000,http://127.0.0.1:5173,http://127.0.0.1:3000"

    # Logging
    log_level: str = "INFO"

    # Pipeline
    pipeline_run_interval_minutes: int = 0  # 0 = disabled

    # LLM (optional)
    gemini_api_key: str | None = None
    gemini_model: str = "gemini-1.5-flash"
    openai_api_key: str | None = None
    openai_model: str = "gpt-4o-mini"

    @property
    def data_raw_dir_path(self) -> Path:
        return Path(self.data_raw_dir).resolve()

    @property
    def cors_origins_list(self) -> List[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    @field_validator("log_level")
    @classmethod
    def log_level_upper(cls, v: str) -> str:
        return v.upper() if v else "INFO"


def get_settings() -> Settings:
    """Return validated settings (singleton per process)."""
    return Settings()
