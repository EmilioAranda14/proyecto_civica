{{ config(
    materialized = 'view'
) }}

with src_promos as (

    select
        promo_id::varchar          as promo_id,
        promo_id::varchar          as promo_desc,
        discount::number(12,2)     as promo_value,
        status::varchar            as promo_status,

        -- SK principal de la promo
        {{ dbt_utils.generate_surrogate_key(['promo_id']) }}   as promo_sk,

        -- SK del estado de la promo
        {{ dbt_utils.generate_surrogate_key(['promo_status']) }} as promo_status_sk,

        convert_timezone('UTC', _fivetran_synced) as date_load
    from {{ source('proyecto_civica_dev_bronze', 'raw_promos') }}
    where _fivetran_deleted is null

),
no_promo_row as (

    select
        'NO_PROMO'                             as promo_id,
        'no promo'                             as promo_desc,
        0::number(12,2)                        as promo_value,
        'inactive'                             as promo_status,

        -- OJO: aquí pasamos un LITERAL al macro (con comillas dentro)
        {{ dbt_utils.generate_surrogate_key(["'NO_PROMO'"]) }}  as promo_sk,
        {{ dbt_utils.generate_surrogate_key(["'inactive'"]) }}  as promo_status_sk,

        current_timestamp()::timestamp_ntz     as date_load

),

promos_change as (

    select * from src_promos
    union all
    select * from no_promo_row

)
select *
from promos_change


  