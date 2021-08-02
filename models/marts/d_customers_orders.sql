with f_customers_orders as (
    select
        cus_i.customer_id as customer_id,
        cus_i.first_name as first_name,
        cus_i.last_name as last_name,
        min(ord_i.order_date) as first_order_date,
        max(ord_i.order_date) as last_order_date,
        count(ord_i.order_id) as total_orders,
        sum(pay_i.amount) as total_spent
    from
        ({{ ref('stg_customers_info') }} as cus_i LEFT JOIN {{ ref('stg_orders_info') }} as ord_i ON cus_i.customer_id = ord_i.customer_id)
        JOIN {{ ref('stg_payments_info') }} as pay_i ON ord_i.order_id = pay_i.order_id
    where pay_i.status = 'success'
    group by cus_i.customer_id, cus_i.first_name, cus_i.last_name
)

select *
from f_customers_orders