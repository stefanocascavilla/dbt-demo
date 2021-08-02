with orders_info as (
    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status
    from dbt-tutorial.jaffle_shop.orders
)

select *
from orders_info