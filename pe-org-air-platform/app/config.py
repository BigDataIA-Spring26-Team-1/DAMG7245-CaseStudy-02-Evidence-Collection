from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # App
    app_name: str = "pe-org-air-platform"
    app_env: str = "dev"
    log_level: str = "INFO"
    api_prefix: str = "/api/v1"

    # Redis
    redis_url: str = "redis://localhost:6379/0"
    redis_ttl_seconds: int = 300
    redis_ttl_company_seconds: int = 300
    redis_ttl_industries_seconds: int = 3600
    redis_ttl_assessment_seconds: int = 120
    redis_ttl_dimension_weights_seconds: int = 86400

    # Snowflake
    snowflake_account: str | None = None
    snowflake_user: str | None = None
    snowflake_password: str | None = None
    snowflake_warehouse: str | None = None
    snowflake_database: str | None = None
    snowflake_schema: str = "PUBLIC"
    snowflake_role: str | None = None

    # AWS / S3
    aws_region: str = "us-east-1"
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    s3_bucket_name: str | None = None

    # SEC / EDGAR
    sec_user_agent: str = "PE-OrgAIR (Northeastern) yourname@northeastern.edu"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
