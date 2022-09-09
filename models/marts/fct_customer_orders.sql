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
    select  orderid as order_id,
            max(created) as payment_finalized_date,
            sum(amount) / 100.0 as total_amount_paid
    from payments
    where status <> 'fail'
    group by 1
),

paid_orders as (
    select  orders.id as order_id,
            orders.user_id	as customer_id,
            orders.order_date AS order_placed_at,
            orders.status AS order_status,
            completed_payments.total_amount_paid,
            completed_payments.payment_finalized_date,
            customers.first_name    as customer_first_name,
            customers.last_name as customer_last_name
    FROM orders
    left join completed_payments on orders.id = completed_payments.order_id
    left join customers on orders.user_id = customers.id 
),
customer_orders as (
    select  customers.id as customer_id,
            min(orders.order_date) as first_order_date,
            max(orders.order_date) as most_recent_order_date,
            count(orders.id) as number_of_orders
    from customers  
    left join orders on orders.user_id = customers.id 
    group by 1
), 
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

        row_number() over (order BY paid_orders.order_id) as transaction_seq,

        row_number() over (partition by paid_orders.customer_id order BY paid_orders.order_id) as customer_sales_seq,

        case when customer_orders.first_order_date = paid_orders.order_placed_at
        then 'new'
        else 'return' end as nvsr,

        --x.clv_bad as customer_lifetime_value,
        sum(paid_orders.total_amount_paid) over (
            partition by paid_orders.customer_id
            order by paid_orders.order_placed_at
        ) as customer_lifetime_value, 
        customer_orders.first_order_date as fdos
    from paid_orders 
    left join customer_orders on paid_orders.customer_id=customer_orders.customer_id
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