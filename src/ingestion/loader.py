"""Database loader for telemetry data."""

import logging
from typing import Dict, List, Any
import psycopg2
from psycopg2.extras import execute_batch, Json

from .config import Config

logger = logging.getLogger(__name__)

class DatabaseLoader:
    """Loader for inserting data into PostgreSQL."""
    
    def __init__(self, config: Config):
        """Initialize database loader."""
        self.config = config
        self.conn = None
        logger.info("Initialized database loader")
    
    def connect(self):
        """Establish database connection."""
        self.conn = psycopg2.connect(
            host=self.config.db_host,
            port=self.config.db_port,
            database=self.config.db_name,
            user=self.config.db_user,
            password=self.config.db_password,
        )
        self.conn.autocommit = False  # Use transactions
        logger.info("Database connection established")
    
    def load_telemetry_data(
        self,
        data: List[Dict[str, Any]],
        table_name: str = "telemetry_events",
    ) -> int:
        """
        Load telemetry data into database.
        
        Args:
            data: List of telemetry events
            table_name: Target table
            
        Returns:
            Number of records loaded
        """
        if not data:
            logger.warning("No data to load")
            return 0
        
        if not self.conn:
            self.connect()
        
        logger.info(f"Loading {len(data)} records into {table_name}")
        
        try:
            cursor = self.conn.cursor()
            
            # Prepare records for insertion
            records = self._prepare_records(data)
            
            # Upsert query (insert or update if exists)
            upsert_query = f"""
                INSERT INTO {table_name} (
                    event_id,
                    device_id,
                    event_type,
                    timestamp,
                    payload,
                    source_api,
                    ingestion_timestamp
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) 
                DO UPDATE SET
                    device_id = EXCLUDED.device_id,
                    event_type = EXCLUDED.event_type,
                    timestamp = EXCLUDED.timestamp,
                    payload = EXCLUDED.payload
            """
            
            # Bulk insert (fast!)
            execute_batch(cursor, upsert_query, records, page_size=1000)
            
            self.conn.commit()
            logger.info(f"Successfully loaded {len(records)} records")
            
            cursor.close()
            return len(records)
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to load data: {e}")
            raise
    
    def _prepare_records(self, data: List[Dict]) -> List[tuple]:
        """Prepare records for database insertion."""
        records = []
        
        for record in data:
            # Extract main fields
            event_id = record.get("event_id")
            device_id = record.get("device_id")
            event_type = record.get("event_type")
            timestamp = record.get("timestamp")
            source_api = record.get("source_api", self.config.api_url)
            ingestion_timestamp = record.get("ingestion_timestamp")
            
            # Everything else goes in JSON payload
            payload = {
                k: v for k, v in record.items()
                if k not in ["event_id", "device_id", "event_type", 
                            "timestamp", "source_api", "ingestion_timestamp"]
            }
            
            records.append((
                event_id,
                device_id,
                event_type,
                timestamp,
                Json(payload),  # Store as JSON
                source_api,
                ingestion_timestamp,
            ))
        
        return records
    
    def disconnect(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")