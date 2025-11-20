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
    order_id::varchar                                                   as order_id,
    coalesce(lower(trim(shipping_service)), 'unknown')                  as shipping_service_code,
    {{ dbt_utils.generate_surrogate_key(['shipping_service_code']) }}   as shipping_service_id,
    shipping_cost::number(12,2)                                         as shipping_cost,
    address_id::varchar                                                 as address_id,
    convert_timezone ('UTC', created_at)                                as order_created_at,
    coalesce(cast(promo_id as varchar), 'NO_PROMO')                     as promo_id,
    convert_timezone ('UTC', estimated_delivery_at)                     as estimated_delivery_at,
    order_cost::number(12,2)                                            as order_cost,
    user_id::varchar                                                    as user_id,
    order_total::number(12,2)                                           as order_total, 
    convert_timezone ('UTC', delivered_at)                              as delivered_at,
    tracking_id::varchar                                                as tracking_id,
    status::varchar                                                     as status,
    convert_timezone ('UTC', _fivetran_synced)                          as date_load
    FROM src_orders  
    )

SELECT * FROM orders_change