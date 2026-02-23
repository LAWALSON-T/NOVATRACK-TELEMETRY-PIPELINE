-- NovaTrack Analytics Database Initialization
-- Creates structure and minimal seed data for testing

-- ============================================
-- Create Schemas
-- ============================================
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- ============================================
-- Create Tables
-- ============================================
CREATE TABLE IF NOT EXISTS telemetry_events (
    event_id VARCHAR(255) PRIMARY KEY,
    device_id VARCHAR(100) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    payload JSONB,
    source_api VARCHAR(100),
    ingestion_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- Create Indexes
-- ============================================
CREATE INDEX IF NOT EXISTS idx_telemetry_device_id ON telemetry_events(device_id);
CREATE INDEX IF NOT EXISTS idx_telemetry_timestamp ON telemetry_events(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_event_type ON telemetry_events(event_type);
CREATE INDEX IF NOT EXISTS idx_telemetry_payload ON telemetry_events USING GIN(payload);
CREATE INDEX IF NOT EXISTS idx_telemetry_device_timestamp ON telemetry_events(device_id, timestamp DESC);

-- ============================================
-- Insert Minimal Seed Data (for initial testing)
-- ============================================
-- Note: Airflow mock generator will add bulk data
-- These records are just for verifying dbt works immediately

INSERT INTO telemetry_events (event_id, device_id, event_type, timestamp, payload, source_api)
VALUES
    ('evt_seed_001', 'device_001', 'page_view', '2024-02-01 10:00:00+00', 
     '{"device_model": "iPhone 13", "os_version": "iOS 17", "user_id": "user_001", "session_id": "session_001", "country": "Netherlands", "city": "Amsterdam", "metric_value": 100, "duration_ms": 1500}'::jsonb, 
     'seed_data'),
    
    ('evt_seed_002', 'device_002', 'button_click', '2024-02-01 11:00:00+00', 
     '{"device_model": "Samsung S23", "os_version": "Android 14", "user_id": "user_002", "session_id": "session_002", "country": "Germany", "city": "Berlin", "metric_value": 75, "duration_ms": 800}'::jsonb, 
     'seed_data'),
    
    ('evt_seed_003', 'device_003', 'page_view', '2024-02-02 09:00:00+00', 
     '{"device_model": "iPhone 14", "os_version": "iOS 17", "user_id": "user_003", "session_id": "session_003", "country": "France", "city": "Paris", "metric_value": 120, "duration_ms": 2000}'::jsonb, 
     'seed_data')
ON CONFLICT (event_id) DO NOTHING;

-- ============================================
-- Verification
-- ============================================
SELECT 
    'Database initialized!' as status,
    COUNT(*) as seed_records 
FROM telemetry_events
WHERE source_api = 'seed_data';

-- Note: Run Airflow DAG 'telemetry_pipeline_with_mock_data' to generate bulk data
