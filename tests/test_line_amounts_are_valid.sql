with base as (

    select
        order_items_id,
        order_id,
        shipping_cost_order,
        order_total_quantity,
        shipping_cost_unit,
        shipping_cost_line,
        promo_amount_order,
        promo_amount_unit,
        promo_amount_line
    from {{ ref('int_proyecto_civica__order_items_promos') }}

)

select *
from base
where
    -- Importes de envío no pueden ser negativos
    shipping_cost_order < 0
    or shipping_cost_unit < 0
    or shipping_cost_line < 0

    -- Importes de promo tampoco pueden ser negativos
    or promo_amount_order < 0
    or promo_amount_unit < 0
    or promo_amount_line < 0
