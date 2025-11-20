{{ config(materialized='view') }}

with base as (

    select *
    from {{ ref('stg_proyecto_civica__addresses') }}

),

joined as (

    select
        a.address_id,
        a.address_line,
        a.zipcode,
        a.state,
        a.country,
        z.zipcode_sk,
        z.state_sk,
        z.country_sk
    from base a
    left join {{ ref('stg_proyecto_civica__zipcode') }} z
        on a.zipcode = z.desc_zipcode 

)

select *
from joined

