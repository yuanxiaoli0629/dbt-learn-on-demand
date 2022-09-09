--import CTEs
with
customers as (
    select * from {{ ref('stg_dbt_training__customers') }}
), 

paid_orders as (
    select * from {{ ref('int_orders') }}
), 
--customer_orders as (
--    select  customers.customer_id,
--            min(orders.order_placed_at) as first_order_date,
--            max(orders.order_placed_at) as most_recent_order_date,
--            count(orders.order_id) as number_of_orders
--    from customers  
--    left join orders on orders.customer_id = customers.customer_id 
--   group by 1
--), 
--final CTEs
final as (
    select
        paid_orders.order_id,
        paid_orders.customer_id,
        paid_orders.order_placed_at,
        paid_orders.order_status,
        paid_orders.total_amount_paid,
        paid_orders.payment_finalized_date,
        customers.customer_first_name,
        customers.customer_last_name,

        row_number() over (order by paid_orders.order_placed_at, paid_orders.order_id) as transaction_seq,

        row_number() over (
        partition by paid_orders.customer_id
        order by paid_orders.order_placed_at, paid_orders.order_id
        ) as customer_sales_seq,

        case  
        when (
            rank() over (
            partition by paid_orders.customer_id
            order by paid_orders.order_placed_at, paid_orders.order_id
            ) = 1
        ) then 'new'
        else 'return' end as nvsr,

        --x.clv_bad as customer_lifetime_value,
        sum(paid_orders.total_amount_paid) over (
        partition by paid_orders.customer_id
        order by paid_orders.order_placed_at, paid_orders.order_id
        ) as customer_lifetime_value,

        --first day of sale
        first_value(paid_orders.order_placed_at) over (
        partition by paid_orders.customer_id
        order by paid_orders.order_placed_at, paid_orders.order_id
        ) as fdos
    from paid_orders 
    left join customers on paid_orders.customer_id=customers.customer_id
    --left outer join 
    --(
    --    select
    --        p.order_id,
    --        sum(t2.total_amount_paid) as clv_bad
    --   from paid_orders p
    --   left join paid_orders t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
    --   group by 1
    --   order by p.order_id
    --)  x on x.order_id = p.order_id
    order by paid_orders.order_id
)

select * from final