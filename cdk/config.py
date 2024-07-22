from typing import List
from pydantic import BaseSettings
from pathlib import Path
from os import environ


class Settings(BaseSettings):
    FETCH_BUCKET: str
    ENV: str = "staging"
    PROJECT: str = "openaq"
    INGEST_LAMBDA_TIMEOUT: int = 900
    INGEST_LAMBDA_MEMORY_SIZE: int = 1536
    INGEST_RATE_MINUTES: int = 15
    LOG_LEVEL: str = 'INFO'
    TOPIC_ARN: str = None
    VPC_ID: str = None

    class Config:
        parent = Path(__file__).resolve().parent.parent
        if 'DOTENV' in environ:
            env_file = Path.joinpath(parent, environ['DOTENV'])
        else:
            env_file = Path.joinpath(parent, ".env")


settings = Settings()
