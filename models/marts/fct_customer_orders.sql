--import CTEs
with
customers as (
    select * from {{ ref('stg_dbt_training__customers') }}
), 

orders as (
    select * from {{ ref('stg_dbt_training__orders') }}
), 

payments as (
    select * from {{ ref('stg_dbt_training__payments') }}
), 
--logic CTEs
completed_payments as (
    select  order_id,
            max(payment_created_at) as payment_finalized_date,
            sum(payment_amount) / 100.0 as total_amount_paid
    from payments
    where payment_status <> 'fail'
    group by 1
),

paid_orders as (
    select  orders.order_id,
            orders.customer_id,
            orders.order_placed_at,
            orders.order_status,
            completed_payments.total_amount_paid,
            completed_payments.payment_finalized_date,
            customers.customer_first_name,
            customers.customer_last_name
    FROM orders
    left join completed_payments on orders.order_id = completed_payments.order_id
    left join customers on orders.customer_id = customers.customer_id 
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
        order_id,
        customer_id,
        order_placed_at,
        order_status,
        total_amount_paid,
        payment_finalized_date,
        customer_first_name,
        customer_last_name,

        row_number() over (order BY order_id) as transaction_seq,

        row_number() over (partition by customer_id order BY order_id) as customer_sales_seq,

        case  
        when (
            rank() over (
            partition by customer_id
            order by order_placed_at, order_id
            ) = 1
        ) then 'new'
        else 'return' end as nvsr,

        --x.clv_bad as customer_lifetime_value,
        sum(total_amount_paid) over (
            partition by customer_id
            order by order_placed_at
        ) as customer_lifetime_value, 

        --first day of sale
        first_value(order_placed_at) over (
        partition by customer_id
        order by order_placed_at
        ) as fdos
    from paid_orders 
    --left join customer_orders on paid_orders.customer_id=customer_orders.customer_id
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
    order by order_id
)

select * from final