{{ config(
    materialized = 'view'
) }}

with src_promos as (

    select
        cast(promo_id   as varchar)                 as promo_id,
        cast(promo_id   as varchar)                 as desc_promo,
        cast(discount   as number(12,2))            as discount,
        cast(status     as varchar)                 as promo_status,
        convert_timezone ('UTC', _fivetran_synced)  as date_load
    from {{ source('proyecto_civica_dev_bronze', 'raw_promos') }}
    where _fivetran_deleted is null

),

no_promo_row as (
    select
        'NO_PROMO'                         as promo_id,
        'no promo'                         as desc_promo,
        0::number(12,2)                    as discount,
        'inactive'                         as promo_status,
        current_timestamp()::timestamp_ntz as date_load

),

promos_change as (

    select * from src_promos
    union all
    select * from no_promo_row

)

select *
from promos_change
  