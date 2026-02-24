"""API Client for fetching telemetry data."""

import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import Config

logger = logging.getLogger(__name__)


class APIClient:
    """Client for fetching telemetry data from REST API."""

    def __init__(self, config: Config):
        """Initialize API client."""
        self.config = config
        self.session = self._create_session()
        self.base_url = config.api_url
        self.api_key = config.api_key
        logger.info(f"Initialized API client for {self.base_url}")

    def _create_session(self) -> requests.Session:
        """Create requests session with retry strategy."""
        session = requests.Session()

        # Retry strategy: retry 3 times on certain errors
        retry_strategy = Retry(
            total=3,  # Total retries
            backoff_factor=1,  # Wait 1s, 2s, 4s between retries
            status_forcelist=[429, 500, 502, 503, 504],  # Retry on these errors
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def fetch_telemetry_data(
        self,
        start_date: datetime,
        end_date: datetime,
        page_size: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Fetch telemetry data from API.

        Args:
            start_date: Start date for data
            end_date: End date for data
            page_size: Records per page

        Returns:
            List of telemetry events
        """
        logger.info(f"Fetching data from {start_date} to {end_date}")

        all_data = []
        page = 1
        has_more = True

        while has_more:
            # Fetch one page
            data, has_more = self._fetch_page(start_date, end_date, page, page_size)
            all_data.extend(data)
            logger.info(f"Fetched page {page}: {len(data)} records")

            page += 1
            time.sleep(0.5)  # Be nice to the API

        logger.info(f"Total records fetched: {len(all_data)}")
        return all_data

    def _fetch_page(self, start_date, end_date, page, page_size) -> tuple:
        """Fetch a single page from API."""
        endpoint = f"{self.base_url}/telemetry/events"

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "page": page,
            "page_size": page_size,
        }

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        response = self.session.get(
            endpoint,
            params=params,
            headers=headers,
            timeout=self.config.api_timeout,
        )
        response.raise_for_status()  # Raise error if request failed

        result = response.json()
        data = result.get("data", [])
        has_more = result.get("pagination", {}).get("has_more", False)

        return data, has_more
