{{ config(materialized = 'view') }}

with src_products as (

    select
        product_id::varchar        as product_id,
        name::varchar              as product_name,
        price::number(12,2)        as product_price,
        inventory::number(12,0)    as inventory,
        convert_timezone('UTC', _fivetran_synced) as date_load
    from {{ source('proyecto_civica_dev_bronze', 'raw_products') }}
    where _fivetran_deleted is null

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_sk,
        product_id,
        product_name,
        product_price,
        inventory,
        case
            when inventory > 0 then 'Yes'
            else 'No'
        end as is_in_stock,
        date_load
    from src_products

)

select *
from final

