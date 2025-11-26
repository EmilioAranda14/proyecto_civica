{{ config(
    materialized      = 'incremental',
    unique_key        = 'event_id',
    on_schema_change  = 'sync_all_columns'
) }}

-- 1) Eventos desde el staging, incremental por date_load
with events_base as (

    select
        e.event_id,
        e.user_id,
        e.product_id,
        e.order_id,
        e.session_id,
        e.page_url,
        e.event_type,
        e.event_created_at,
        e.date_load
    from {{ ref('stg_proyecto_civica__events') }} e

    {% if is_incremental() %}
      where e.date_load > (
        select max(date_load)
        from {{ this }}
      )
    {% endif %}

),

-- 2) Enriquecemos con dimensiones (users, products, event_type, date)
events_enriched as (

    select
        eb.event_id,
        eb.user_id,
        eb.product_id,
        eb.order_id,
        eb.session_id,
        eb.page_url,
        eb.event_type,
        eb.event_created_at,
        eb.date_load,

        du.user_sk,
        dp.product_sk,
        det.event_type_sk,
        dd.date_day as event_date
    from events_base eb

    left join {{ ref('dim_users') }} du
        on eb.user_id = du.user_id

    left join {{ ref('dim_products') }} dp
        on eb.product_id = dp.product_id

    left join {{ ref('dim_event_type') }} det
        on eb.event_type = det.event_type_code

    left join {{ ref('dim_date') }} dd
        on eb.event_created_at::date = dd.date_day
),

final as (

    select
        event_id,

        -- claves surrogate a dimensiones
        user_sk,
        product_sk,
        event_type_sk,

        -- claves naturales
        user_id,
        product_id,
        order_id,
        session_id,

        page_url,
        event_type,
        event_date,

        event_created_at,
        date_load
    from events_enriched

)

select *
from final






