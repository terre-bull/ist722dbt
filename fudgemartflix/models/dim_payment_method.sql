with fudgeflix_payment as (
    select
        0                                       as payment_id,
        'NA'                        as payment_type,
        'NA'                        as card_network,
        'FudgeFlix'                             as division
),

fudgemart_payment as (
    select
        creditcard_id                           as payment_id,
        'Credit Card'                           as payment_type,
        case
            when left(creditcard_number, 1) = '4' then 'Visa'
            when left(creditcard_number, 2) in ('51','52','53','54','55') then 'Mastercard'
            when left(creditcard_number, 2) in ('34','37') then 'Amex'
            when left(creditcard_number, 4) = '6011'
                or left(creditcard_number, 2) = '65'
                or left(creditcard_number, 3) between '644' and '649' then 'Discover'
            else 'Unknown'
        end                                     as card_network,
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