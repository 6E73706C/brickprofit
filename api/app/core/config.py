from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
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

    class Config:
        env_file = ".env"


settings = Settings()
