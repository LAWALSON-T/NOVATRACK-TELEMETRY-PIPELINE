-- Staging model: Clean and standardize raw telemetry data

{{
    config(
        materialized='view',
        tags=['staging', 'telemetry']
    )
}}

-- Start with raw data
with source_data as (
    select
        event_id,
        device_id,
        event_type,
        timestamp,
        payload,
        source_api,
        ingestion_timestamp,
        created_at
    from {{ source('raw', 'telemetry_events') }}
),

-- Clean and extract fields
cleaned_data as (
    select
        -- Primary identifiers
        event_id,
        device_id,
        event_type,
        
        -- Convert timestamps
        timestamp::timestamptz as event_timestamp,
        ingestion_timestamp::timestamptz as ingestion_timestamp,
        
        -- Extract fields from JSON payload
        (payload->>'device_model')::varchar as device_model,
        (payload->>'os_version')::varchar as os_version,
        (payload->>'app_version')::varchar as app_version,
        (payload->>'user_id')::varchar as user_id,
        (payload->>'session_id')::varchar as session_id,
        (payload->>'country')::varchar as country,
        (payload->>'city')::varchar as city,
        
        -- Extract metrics
        (payload->>'metric_value')::numeric as metric_value,
        (payload->>'duration_ms')::integer as duration_ms,
        
        -- Keep full payload for flexibility
        payload as raw_payload,
        source_api
        
    from source_data
    
    -- Filter out invalid records
    where event_id is not null
      and device_id is not null
      and event_type is not null
      and timestamp is not null
      and timestamp::timestamptz >= '2024-01-01'::timestamptz
      and timestamp::timestamptz <= current_timestamp
)

select * from cleaned_data