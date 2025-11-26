from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Runtime configuration for the feedback & interactions pipeline."""

    supabase_dsn: str = Field(..., env="SUPABASE_CONN_STRING")
    neo4j_uri: str = Field(..., env="NEO4J_URI")
    neo4j_user: str = Field(..., env="NEO4J_USER")
    neo4j_password: str = Field(..., env="NEO4J_PASSWORD")
    queue_url: str = Field(..., env="QUEUE_URL")
    pipeline_name: str = Field("feedback-interactions", env="PIPELINE_NAME")

    poll_interval_seconds: int = Field(5, env="POLL_INTERVAL_SECONDS")
    batch_size: int = Field(100, env="BATCH_SIZE")
    max_attempts: int = Field(5, env="MAX_ATTEMPTS")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
