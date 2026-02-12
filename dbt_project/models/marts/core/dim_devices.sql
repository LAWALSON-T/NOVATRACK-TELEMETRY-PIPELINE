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

-- Get latest attributes for each device using ROW_NUMBER
latest_device_info as (
    select
        device_id,
        device_model,
        os_version,
        country,
        event_timestamp,
        row_number() over (
            partition by device_id 
            order by event_timestamp desc
        ) as rn
    from telemetry_data
),

-- Get only the most recent record per device
latest_attributes as (
    select
        device_id,
        device_model,
        os_version,
        country
    from latest_device_info
    where rn = 1
),

-- Calculate activity metrics
device_metrics as (
    select
        device_id,
        min(event_timestamp) as first_seen_at,
        max(event_timestamp) as last_seen_at,
        count(*) as total_events,
        count(distinct date(event_timestamp)) as active_days
    from telemetry_data
    group by device_id
),

final as (
    select
        la.device_id,
        la.device_model,
        la.os_version,
        la.country,
        dm.first_seen_at,
        dm.last_seen_at,
        dm.total_events,
        dm.active_days,
        
        -- Calculate days between first and last seen
        extract(day from (dm.last_seen_at - dm.first_seen_at)) as days_active_span,
        
        current_timestamp as dbt_updated_at
        
    from latest_attributes la
    inner join device_metrics dm
        on la.device_id = dm.device_id
)

select * from final
