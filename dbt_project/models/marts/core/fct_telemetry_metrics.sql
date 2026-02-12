-- Fact table: Daily aggregated telemetry metrics

{{
    config(
        materialized='table',
        tags=['marts', 'core', 'metrics']
    )
}}

-- Get clean data from staging
with telemetry_data as (
    select * from {{ ref('stg_telemetry__raw') }}
),

-- Aggregate by day, device, and event type
daily_metrics as (
    select
        -- Dimensions (what we're grouping by)
        device_id,
        date(event_timestamp) as metric_date,
        event_type,
        device_model,
        os_version,
        country,
        
        -- Metrics (what we're measuring)
        count(*) as event_count,
        count(distinct session_id) as unique_sessions,
        count(distinct user_id) as unique_users,
        
        -- Time metrics
        min(event_timestamp) as first_event_time,
        max(event_timestamp) as last_event_time,
        
        -- Duration metrics
        avg(duration_ms) as avg_duration_ms,
        sum(duration_ms) as total_duration_ms,
        
        -- Value metrics
        avg(metric_value) as avg_metric_value,
        sum(metric_value) as total_metric_value,
        
        -- Metadata
        current_timestamp as dbt_updated_at
        
    from telemetry_data
    
    group by 1, 2, 3, 4, 5, 6
),

-- Add surrogate key
final as (
    select
        -- Generate unique ID for each row
        {{ dbt_utils.generate_surrogate_key([
            'device_id',
            'metric_date',
            'event_type'
        ]) }} as metric_id,
        
        *
        
    from daily_metrics
)

select * from final