{{
  config(
    materialized='incremental'
  )
}}

WITH src_orders_items AS (
    SELECT * 
    FROM {{ source('proyecto_civica_dev_bronze', 'raw_order_items') }} 
    
    
{% if is_incremental() %}

	  WHERE _fivetran_synced > (SELECT MAX(date_load) FROM {{ this }} )

{% endif %}
    ),

order_items_change AS (
    SELECT
    {{ dbt_utils.generate_surrogate_key(['order_id', 'product_id']) }}    as order_items_id,
    order_id::varchar                                                     as order_id,
    product_id::varchar                                                   as product_id,
    cast(quantity as int)                                                 as quantity, 
    convert_timezone ('UTC', _fivetran_synced)                            as date_load
    FROM src_orders_items
    )

SELECT * FROM order_items_change