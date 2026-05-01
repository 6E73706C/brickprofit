from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator
from typing import List
import json


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # Cassandra
    CASSANDRA_HOSTS: List[str] = ["cassandra1", "cassandra2", "cassandra3"]
    CASSANDRA_KEYSPACE: str = "brickprofit"
    CASSANDRA_USER: str = "brickprofit"
    CASSANDRA_PASSWORD: str = ""

    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"

    # App
    SECRET_KEY: str = "changeme"
    ENVIRONMENT: str = "production"
    CORS_ORIGINS: List[str] = ["https://yourdomain.com"]

    @field_validator("CASSANDRA_HOSTS", "CORS_ORIGINS", mode="before")
    @classmethod
    def parse_list(cls, v):
        if isinstance(v, str):
            v = v.strip()
            if v.startswith("["):
                return json.loads(v)
            return [h.strip() for h in v.split(",") if h.strip()]
        return v


settings = Settings()
