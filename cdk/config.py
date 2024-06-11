from typing import List
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
    )
from pathlib import Path
from os import environ


class Settings(BaseSettings):
    FETCH_BUCKET: str
    ENV: str = "staging"
    PROJECT: str = "openaq"
    LAMBDA_TIMEOUT: int = 900
    LAMBDA_MEMORY_SIZE: int = 1536
    RATE_MINUTES: int = 15
    LOG_LEVEL: str = 'INFO'
    TOPIC_ARN: str = None
    VPC_ID: str = None


    model_config = SettingsConfigDict(
        extra="ignore", env_file=f"../{environ.get('DOTENV', '.env')}", env_file_encoding="utf-8"
    )


settings = Settings()
