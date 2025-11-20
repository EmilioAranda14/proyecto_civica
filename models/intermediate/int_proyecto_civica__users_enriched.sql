{{ config(materialized='view') }}

with base as (

    select *
    from {{ ref('stg_proyecto_civica__users') }}

),

joined as (

    select
        u.user_id,
        u.first_name,
        u.last_name,
        u.email,
        u.phone_number,
        u.total_orders,
        u.user_created_at,
        u.user_updated_at,
        u.address_id,
        ae.zipcode_sk,
        ae.state_sk,
        ae.country_sk
    from base u
    left join {{ ref('int_proyecto_civica__addresses_enriched') }} ae
        on u.address_id = ae.address_id

)

select *
from joined
