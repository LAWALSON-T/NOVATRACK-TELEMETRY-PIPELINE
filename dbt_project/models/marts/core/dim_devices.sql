-- Dimension table: Device attributes

{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

with telemetry_data as (
    select * from {{ ref('stg_telemetry__raw') }}
),

-- Get latest attributes for each device
device_attributes as (
    select
        device_id,
        
        -- Get most recent attributes using window function
        first_value(device_model) over (
            partition by device_id 
            order by event_timestamp desc
            rows between unbounded preceding and unbounded following
        ) as device_model,
        
        first_value(os_version) over (
            partition by device_id 
            order by event_timestamp desc
            rows between unbounded preceding and unbounded following
        ) as os_version,
        
        first_value(country) over (
            partition by device_id 
            order by event_timestamp desc
            rows between unbounded preceding and unbounded following
        ) as country,
        
        -- Activity metrics
        min(event_timestamp) as first_seen_at,
        max(event_timestamp) as last_seen_at,
        count(*) as total_events,
        count(distinct date(event_timestamp)) as active_days
        
    from telemetry_data
    
    group by device_id
),

final as (
    select distinct
        device_id,
        device_model,
        os_version,
        country,
        first_seen_at,
        last_seen_at,
        total_events,
        active_days,
        
        -- Calculate days between first and last seen
        extract(day from (last_seen_at - first_seen_at)) as days_active_span,
        
        current_timestamp as dbt_updated_at
        
    from device_attributes
)

select * from final