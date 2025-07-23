"""Configuration settings using pydantic-settings."""

from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables and .env file."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Required API keys
    companies_house_key: str = Field(..., description="Companies House API key")
    
    # Optional API keys
    opencage_key: Optional[str] = Field(None, description="OpenCage geocoding API key")
    
    # Rate limiting
    ddg_max_qps: float = Field(0.3, description="DuckDuckGo max queries per second")
    max_concurrency: int = Field(10, description="Maximum concurrent requests")
    
    # Caching
    cache_dir: str = Field(".cache", description="Directory for disk cache")
    cache_ttl_days: int = Field(7, description="Cache TTL in days")
    
    # Logging
    log_level: str = Field("INFO", description="Logging level")
    
    # Timeouts
    http_timeout: float = Field(30.0, description="HTTP request timeout in seconds")
    
    # Companies House API settings
    ch_base_url: str = Field(
        "https://api.company-information.service.gov.uk",
        description="Companies House API base URL"
    )
    ch_doc_base_url: str = Field(
        "https://document-api.company-information.service.gov.uk",
        description="Companies House document API base URL"
    )
    
    @property
    def ch_auth_headers(self) -> dict[str, str]:
        """Get Companies House authentication headers."""
        return {"Authorization": f"{self.companies_house_key}:"}


# Global settings instance
settings = Settings()
