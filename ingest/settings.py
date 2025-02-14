from typing import Union

from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
    )

from pydantic import computed_field

from pathlib import Path
from os import environ

def get_env():
    parent = Path(__file__).resolve().parent.parent
    env_file = Path.joinpath(parent, environ.get("DOTENV", ".env"))
    return env_file


class Settings(BaseSettings):
    DATABASE_READ_USER: str
    DATABASE_WRITE_USER: str
    DATABASE_READ_PASSWORD: str
    DATABASE_WRITE_PASSWORD: str
    DATABASE_DB: str
    DATABASE_HOST: str
    DATABASE_PORT: int
    DRYRUN: bool = False
    FETCH_BUCKET: str
    ETL_BUCKET: str
    FETCH_ASCENDING: bool = False
    INGEST_TIMEOUT: int = 900
    PIPELINE_LIMIT: int = 10
    METADATA_LIMIT: int = 10
    REALTIME_LIMIT: int = 10
    LOG_LEVEL: str = 'INFO'
    USE_TEMP_TABLES: bool = True
    PAUSE_INGESTING: bool = False

    @computed_field
    def DATABASE_READ_URL(self) -> str:
        return f"postgresql://{self.DATABASE_READ_USER}:{self.DATABASE_READ_PASSWORD}@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_DB}"

    @computed_field
    def DATABASE_WRITE_URL(self) -> str:
        return f"postgresql://{self.DATABASE_WRITE_USER}:{self.DATABASE_WRITE_PASSWORD}@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_DB}"

    model_config = SettingsConfigDict(
        extra="ignore",
        env_file=get_env(),
        env_file_encoding="utf-8",
    )


settings = Settings()
