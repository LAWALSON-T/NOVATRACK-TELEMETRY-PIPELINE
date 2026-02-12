"""Configuration management for NovaTrack pipeline."""

import os
from dataclasses import dataclass

@dataclass
class Config:
    """Configuration for NovaTrack data pipeline."""
    
    # Database settings
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    
    # API settings
    api_url: str
    api_key: str
    api_timeout: int
    
    def __init__(self):
        """Load configuration from environment variables."""
        # Database
        self.db_host = os.getenv("POSTGRES_HOST", "localhost")
        self.db_port = int(os.getenv("POSTGRES_PORT", "5432"))
        self.db_name = os.getenv("POSTGRES_DB", "novatrack")
        self.db_user = os.getenv("POSTGRES_USER", "novatrack_user")
        self.db_password = os.getenv("POSTGRES_PASSWORD", "")
        
        # API
        self.api_url = os.getenv("API_URL", "https://api.example.com")
        self.api_key = os.getenv("API_KEY", "")
        self.api_timeout = int(os.getenv("API_TIMEOUT", "30"))
        
        # Validate
        if not self.db_password:
            raise ValueError("POSTGRES_PASSWORD must be set")
        if not self.api_url:
            raise ValueError("API_URL must be set")
    
    @property
    def db_connection_string(self) -> str:
        """Get database connection string."""
        return (
            f"postgresql://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )