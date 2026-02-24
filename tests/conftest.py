"""Pytest configuration and fixtures."""

import os
from typing import Any, Dict

import pytest

# Set test environment variables
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("POSTGRES_DB", "test_db")
os.environ.setdefault("POSTGRES_USER", "test_user")
os.environ.setdefault("POSTGRES_PASSWORD", "test_password")
os.environ.setdefault("API_URL", "https://api.example.com")


@pytest.fixture
def sample_event() -> Dict[str, Any]:
    """Sample telemetry event for testing."""
    return {
        "event_id": "evt_123",
        "device_id": "device_001",
        "event_type": "page_view",
        "timestamp": "2024-02-01T12:00:00Z",
        "device_model": "iPhone 13",
        "user_id": "user_789",
    }


@pytest.fixture
def test_config():
    """Test configuration."""
    from src.ingestion.config import Config

    return Config()
