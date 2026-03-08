with fudgeflix_payment as (
    -- FudgeFlix has no credit card data; single placeholder row
    select
        0                                       as payment_id,
        'Not Applicable'                        as payment_type,
        null::varchar                           as payment_exp_date,
        null::varchar                           as payment_credit_card,
        'FudgeFlix'                             as division
),

fudgemart_payment as (
    select
        creditcard_id                           as payment_id,
        'Credit Card'                           as payment_type,
        creditcard_exp_date                     as payment_exp_date,
        creditcard_number                       as payment_credit_card,
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