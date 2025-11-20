{{ config(
    materialized = 'incremental'
) }}

with src_events as (

    select *
    from {{ source('proyecto_civica_dev_bronze', 'raw_events') }}

    {% if is_incremental() %}
      where _fivetran_synced > (select max(date_load) from {{ this }})
    {% endif %}

),

events_change as (

    select
        cast(event_id   as varchar)        as event_id,
        cast(user_id    as varchar)        as user_id,
        cast(product_id as varchar)        as product_id,
        cast(order_id   as varchar)        as order_id,
        cast(session_id as varchar)        as session_id,
        -- timestamps
        convert_timezone('UTC', cast(created_at as timestamp_tz))        as event_created_at,
        cast(convert_timezone('UTC', cast(created_at as timestamp_tz)) 
             as date)                                                    as event_date,
        -- otros campos
        nullif(trim(page_url), '')                                       as page_url,
        event_type                                                       as event_type,
        _fivetran_deleted                                                as date_deleted,
        convert_timezone('UTC', _fivetran_synced)                        as date_load
    from src_events

)

select *
from events_change
