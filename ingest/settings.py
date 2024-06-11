from typing import Union
from pydantic import BaseSettings, validator
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

    @validator('DATABASE_READ_URL', allow_reuse=True)
    def get_read_url(cls, v, values):
        return v or f"postgresql://{values['DATABASE_READ_USER']}:{values['DATABASE_READ_PASSWORD']}@{values['DATABASE_HOST']}:{values['DATABASE_PORT']}/{values['DATABASE_DB']}"

    @validator('DATABASE_WRITE_URL', allow_reuse=True)
    def get_write_url(cls, v, values):
        return v or f"postgresql://{values['DATABASE_WRITE_USER']}:{values['DATABASE_WRITE_PASSWORD']}@{values['DATABASE_HOST']}:{values['DATABASE_PORT']}/{values['DATABASE_DB']}"

    class Config:
        parent = Path(__file__).resolve().parent.parent
        if 'DOTENV' in environ:
            env_file = Path.joinpath(parent, environ['DOTENV'])
        else:
            env_file = Path.joinpath(parent, ".env")


settings = Settings()
