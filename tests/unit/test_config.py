"""Unit tests for configuration module."""

import pytest
import os
from src.ingestion.config import Config


def test_config_loads_from_env():
    """Test configuration loads from environment variables."""
    os.environ["POSTGRES_PASSWORD"] = "test_pass"
    os.environ["API_URL"] = "https://test.api"
    
    config = Config()
    
    assert config.db_password == "test_pass"
    assert config.api_url == "https://test.api"


def test_config_validates_password():
    """Test configuration validates required password."""
    # Remove password
    if "POSTGRES_PASSWORD" in os.environ:
        del os.environ["POSTGRES_PASSWORD"]
    
    with pytest.raises(ValueError, match="POSTGRES_PASSWORD"):
        Config()


def test_config_connection_string():
    """Test database connection string generation."""
    os.environ["POSTGRES_PASSWORD"] = "pass123"
    os.environ["API_URL"] = "https://test.api"
    
    config = Config()
    conn_str = config.db_connection_string
    
    assert "postgresql://" in conn_str
    assert "pass123" in conn_str