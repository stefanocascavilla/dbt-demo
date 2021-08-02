with payments_info as (
    select
        id as payment_id,
        orderid as order_id,
        status,
        amount
    from dbt-tutorial.stripe.payment
)

select *
from payments_info