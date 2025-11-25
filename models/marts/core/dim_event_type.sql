{{ config(
    materialized = 'view'
) }}

select
    event_type_sk,
    event_type_code
from {{ ref('stg_proyecto_civica__event_type') }}


