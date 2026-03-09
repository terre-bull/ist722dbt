with fudgeflix_payment as (
    -- FudgeFlix has no credit card data; single placeholder row
    select
        0                                       as payment_id,
        'Not Applicable'                        as payment_type,
        'FudgeFlix'                             as division
),

fudgemart_payment as (
    select
        creditcard_id                           as payment_id,
        'Credit Card'                           as payment_type,
        'FudgeMart'                             as division
    from {{ source('fudgemart_v3', 'fm_creditcards') }}
),

combined as (
    select * from fudgeflix_payment
    union all
    select * from fudgemart_payment
)

select
    {{ dbt_utils.generate_surrogate_key(['payment_id', 'division']) }} as payment_method_key,
    combined.*
from combined