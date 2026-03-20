with fudgeflix_products as (
    select
        plan_id::varchar                        as product_id,
        plan_name                               as product_name,
        plan_current::varchar                   as product_is_active,
        null::varchar                           as product_start_date,
        'Fudgeflix Subscriptions'               as product_department,
        null::varchar                           as product_vendor_name,
        'FudgeFlix'                             as division
    from {{ source('fudgeflix_v3', 'ff_plans') }}
),

fudgemart_products as (
    select
        p.product_id::varchar                   as product_id,
        p.product_name                          as product_name,
        p.product_is_active::varchar            as product_is_active,
        p.product_add_date                      as product_start_date,
        p.product_department                    as product_department,
        v.vendor_name                           as product_vendor_name,
        'FudgeMart'                             as division
    from {{ source('fudgemart_v3', 'fm_products') }} p
    left join {{ source('fudgemart_v3', 'fm_vendors') }} v
        on p.product_vendor_id = v.vendor_id
),

combined as (
    select * from fudgeflix_products
    union all
    select * from fudgemart_products
)

select
    {{ dbt_utils.generate_surrogate_key(['product_id', 'division']) }} as product_key,
    combined.*
from combined