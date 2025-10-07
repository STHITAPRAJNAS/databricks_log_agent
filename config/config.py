import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Config:
    # Google AI Studio
    google_api_key: str = os.getenv("GOOGLE_API_KEY", "")

    # Databricks
    databricks_host: str = os.getenv("DATABRICKS_HOST", "")
    databricks_token: str = os.getenv("DATABRICKS_TOKEN", "")

    # AWS S3
    aws_access_key_id: str = os.getenv("AWS_ACCESS_KEY_ID", "")
    aws_secret_access_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    aws_region: str = os.getenv("AWS_REGION", "us-west-2")

    # S3 Bucket Configuration
    databricks_logs_bucket: str = os.getenv("DATABRICKS_LOGS_BUCKET", "")
    databricks_logs_prefix: str = os.getenv("DATABRICKS_LOGS_PREFIX", "databrickslogs")

    # LLM Configuration
    max_token_limit: int = int(os.getenv("MAX_TOKEN_LIMIT", "100000"))
    log_search_chunk_size: int = int(os.getenv("LOG_SEARCH_CHUNK_SIZE", "10000"))

    def validate(self) -> bool:
        required_fields = [
            self.google_api_key,
            self.databricks_host,
            self.databricks_token,
            self.aws_access_key_id,
            self.aws_secret_access_key,
            self.databricks_logs_bucket
        ]
        return all(field for field in required_fields)

config = Config()