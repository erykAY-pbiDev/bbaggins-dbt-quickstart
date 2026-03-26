with 

customers as (
  select * from {{ref("stg_customers")}}
),

orders as (
  select * from {{ref("stg_orders")}}
),

customer_order as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_order
    from
        orders
    group by 
        customer_id
),

final_query as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        co.first_order_date,
        co.most_recent_order_date,
        coalesce(co.number_of_order, 0) as number_of_orders
    from 
        customers as c
    left join 
        customer_order as co
    on
        c.customer_id = co.customer_id
)

select * from final_query