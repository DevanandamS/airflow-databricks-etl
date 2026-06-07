select
    Country,
    total_orders,
    total_revenue,
    total_quantity,

    round(
        total_revenue / total_orders,
        2
    ) as avg_order_value

from {{ ref('stg_country_sales') }}