from typing import Union

from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
    )

from pydantic import computed_field

from pathlib import Path
from os import environ


class Settings(BaseSettings):
    DATABASE_READ_USER: str
    DATABASE_WRITE_USER: str
    DATABASE_READ_PASSWORD: str
    DATABASE_WRITE_PASSWORD: str
    DATABASE_DB: str
    DATABASE_HOST: str
    DATABASE_PORT: int
    DATABASE_READ_URL: Union[str, None]
    DATABASE_WRITE_URL: Union[str, None]
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
        return f"postgresql://{values['DATABASE_READ_USER']}:{values['DATABASE_READ_PASSWORD']}@{values['DATABASE_HOST']}:{values['DATABASE_PORT']}/{values['DATABASE_DB']}"

    @computed_field
    def DATABASE_WRITE_URL(self) -> str:
        return f"postgresql://{values['DATABASE_WRITE_USER']}:{values['DATABASE_WRITE_PASSWORD']}@{values['DATABASE_HOST']}:{values['DATABASE_PORT']}/{values['DATABASE_DB']}"

    model_config = SettingsConfigDict(
        extra="ignore", env_file=f"../{environ.get('DOTENV', '.env')}", env_file_encoding="utf-8"
    )


settings = Settings()
