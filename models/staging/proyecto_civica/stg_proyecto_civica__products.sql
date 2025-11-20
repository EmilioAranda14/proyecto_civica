{{
  config(
    materialized='view'
  )
}}

WITH src_products AS (
    SELECT * 
    FROM {{ source('proyecto_civica_dev_bronze', 'raw_products') }} 
    ),

products_change AS (
    SELECT
        product_id::varchar                                             as product_id,                            
        CAST(price AS DECIMAL(12,2))                                    as product_price,
        name                                                            as product_name,
        case when cast(inventory as int) > 0 then 1 else 0 end          as is_in_stock,       
        _fivetran_deleted                                               as date_deleted,
        convert_timezone ('UTC', _fivetran_synced)                      as date_load

    FROM src_products   
    )

SELECT * FROM products_change