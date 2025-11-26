with orders as (

    select
        order_id,
        order_created_at,       -- convert_timezone('UTC', created_at)
        estimated_delivery_at,  -- convert_timezone('UTC', estimated_delivery_at)
        delivered_at,           -- convert_timezone('UTC', delivered_at)
        status
    from {{ ref('stg_proyecto_civica__orders') }}

)

select *
from orders
where
    -- La fecha estimada de entrega no puede ser anterior a la creación
    (estimated_delivery_at is not null
     and estimated_delivery_at < order_created_at)

    or

    -- La fecha real de entrega no puede ser anterior a la creación
    (delivered_at is not null
     and delivered_at < order_created_at)

    or

    -- Si el pedido está entregado, debe tener delivered_at
    (lower(status) = 'delivered'
     and delivered_at is null)

    or

    -- Si el pedido está preparando/enviado, no puede estar ya entregado
    (lower(status) in ('preparing', 'shipped')
     and delivered_at is not null)
