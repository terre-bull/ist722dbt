with fudgeflix_sales as (
    select
        ab.ab_id                                                    as order_id,
        dc.customer_key                                             as customer_key,
        replace(to_date(ab.ab_date)::varchar, '-', '')::int         as order_date_key,
        dp.product_key                                              as product_key,
        dpm.payment_method_key                                      as payment_method_key,
        2                                                           as channel_key,
        1                                                           as order_quantity,
        ab.ab_billed_amount                                         as unit_selling_price,
        ab.ab_billed_amount / 2                                     as unit_cost_price,
        ab.ab_billed_amount                                         as order_sold_amount,
        ab.ab_billed_amount / 2                                     as order_cost_amount,
        ab.ab_billed_amount - (ab.ab_billed_amount / 2)             as order_profit,
        CASE WHEN ab.ab_billed_amount = 0 OR ab.ab_billed_amount IS NULL
             THEN NULL
             ELSE ROUND((ab.ab_billed_amount - (ab.ab_billed_amount / 2)) / ab.ab_billed_amount, 4)
        END                                                         as order_profit_margin,
        'FudgeFlix'                                                 as division
    from {{ source('fudgeflix_v3', 'ff_account_billing') }} ab
    left join {{ ref('dim_customer') }} dc
        on ab.ab_account_id::varchar = dc.customer_id
        and dc.division = 'FudgeFlix'
    left join {{ ref('dim_product') }} dp
        on ab.ab_plan_id::varchar = dp.product_id
        and dp.division = 'FudgeFlix'
    left join {{ ref('dim_payment_method') }} dpm
        on dpm.payment_id = 0
        and dpm.division = 'FudgeFlix'
),

fudgemart_sales as (
    select
        o.order_id                                                  as order_id,
        dc.customer_key                                             as customer_key,
        replace(to_date(o.order_date)::varchar, '-', '')::int       as order_date_key,
        dp.product_key                                              as product_key,
        dpm.payment_method_key                                      as payment_method_key,
        1                                                           as channel_key,
        od.order_qty                                                as order_quantity,
        p.product_retail_price                                      as unit_selling_price,
        p.product_wholesale_price                                   as unit_cost_price,
        od.order_qty * p.product_retail_price                       as order_sold_amount,
        od.order_qty * p.product_wholesale_price                    as order_cost_amount,
        (od.order_qty * p.product_retail_price)
            - (od.order_qty * p.product_wholesale_price)            as order_profit,
        CASE WHEN (od.order_qty * p.product_retail_price) = 0 OR (od.order_qty * p.product_retail_price) IS NULL
             THEN NULL
             ELSE ROUND(
                ((od.order_qty * p.product_retail_price) - (od.order_qty * p.product_wholesale_price))
                / (od.order_qty * p.product_retail_price), 4)
        END                                                         as order_profit_margin,
        'FudgeMart'                                                 as division
    from {{ source('fudgemart_v3', 'fm_orders') }} o
    join {{ source('fudgemart_v3', 'fm_order_details') }} od
        on o.order_id = od.order_id
    join {{ source('fudgemart_v3', 'fm_products') }} p
        on od.product_id = p.product_id
    left join {{ ref('dim_customer') }} dc
        on o.customer_id::varchar = dc.customer_id
        and dc.division = 'FudgeMart'
    left join {{ ref('dim_product') }} dp
        on od.product_id::varchar = dp.product_id
        and dp.division = 'FudgeMart'
    left join {{ ref('dim_payment_method') }} dpm
        on o.creditcard_id = dpm.payment_id
        and dpm.division = 'FudgeMart'
),

combined as (
    select * from fudgeflix_sales
    union all
    select * from fudgemart_sales
)

select * from combined