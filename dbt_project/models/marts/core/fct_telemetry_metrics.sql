-- Fact table: Daily aggregated telemetry metrics
-- Grain: One row per device per day per event type

{{
    config(
        materialized='table',
        tags=['marts', 'core', 'metrics']
    )
}}

with telemetry_data as (
    select * from {{ ref('stg_telemetry__raw') }}
),

daily_metrics as (
    select
        -- Dimensions (these define the grain - one row per unique combination)
        device_id,
        date(event_timestamp) as metric_date,
        event_type,
        
        -- Get most frequent device attributes (handles device changes during day)
        mode() within group (order by device_model) as device_model,
        mode() within group (order by os_version) as os_version,
        mode() within group (order by country) as country,
        
        -- Metrics (aggregations across all events for this device/date/type)
        count(*) as event_count,
        count(distinct session_id) as unique_sessions,
        count(distinct user_id) as unique_users,
        
        -- Time metrics
        min(event_timestamp) as first_event_time,
        max(event_timestamp) as last_event_time,
        
        -- Duration metrics (averages and totals)
        avg(duration_ms) as avg_duration_ms,
        sum(duration_ms) as total_duration_ms,
        
        -- Value metrics
        avg(metric_value) as avg_metric_value,
        sum(metric_value) as total_metric_value,
        
        -- Metadata
        current_timestamp as dbt_updated_at
        
    from telemetry_data
    
    -- CRITICAL: Group by only the grain columns
    group by device_id, metric_date, event_type
),

final as (
    select
        -- Generate surrogate key from grain columns
        MD5(device_id || '-' || metric_date::text || '-' || event_type) as metric_id,
        
        -- All other columns
        device_id,
        metric_date,
        event_type,
        device_model,
        os_version,
        country,
        event_count,
        unique_sessions,
        unique_users,
        first_event_time,
        last_event_time,
        avg_duration_ms,
        total_duration_ms,
        avg_metric_value,
        total_metric_value,
        dbt_updated_at
        
    from daily_metrics
)

select * from final