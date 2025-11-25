{{
  config(
    materialized='incremental'
  )
}}

WITH src_orders AS (
    SELECT * 
    FROM {{ source('proyecto_civica_dev_bronze', 'raw_orders') }} 


{% if is_incremental() %}

	  WHERE _fivetran_synced > (SELECT MAX(date_load) FROM {{ this }} )

{% endif %}
    ),

orders_change AS (
    SELECT
        order_id::varchar                                             AS order_id,

        -- SHIPPING SERVICE (código limpio y alineado con la dim)
        coalesce(
            nullif(lower(trim(shipping_service)), ''), 
            'unknown'
        )                                                                AS shipping_service_code,
        {{ dbt_utils.generate_surrogate_key(['shipping_service_code']) }} AS shipping_service_id,

        shipping_cost::number(12,2)                                   AS shipping_cost,
        address_id::varchar                                           AS address_id,

        convert_timezone('UTC', created_at)                           AS order_created_at,
        coalesce(cast(promo_id as varchar), 'NO_PROMO')               AS promo_id,
        convert_timezone('UTC', estimated_delivery_at)                AS estimated_delivery_at,

        order_cost::number(12,2)                                      AS order_cost,
        user_id::varchar                                              AS user_id,
        order_total::number(12,2)                                     AS order_total,
        convert_timezone('UTC', delivered_at)                         AS delivered_at,
        tracking_id::varchar                                          AS tracking_id,

        status::varchar                                               AS status,

 
        {{ dbt_utils.generate_surrogate_key(['status']) }}            AS order_status_id,

        convert_timezone('UTC', _fivetran_synced)                     AS date_load
    FROM src_orders
)
SELECT * FROM orders_change