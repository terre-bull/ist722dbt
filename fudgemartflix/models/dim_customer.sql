with fudgeflix_customers as (
    select
        a.account_id::varchar                   as customer_id,
        a.account_email                         as customer_email,
        a.account_firstname                     as customer_firstname,
        a.account_lastname                      as customer_lastname,
        a.account_address                       as customer_address,
        z.zip_city                              as customer_city,
        z.zip_state                             as customer_state,
        a.account_zipcode                       as customer_zip,
        null::varchar                           as customer_phone,
        null::varchar                           as customer_fax
    from {{ source('fudgeflix_v3', 'ff_accounts') }} a
    left join {{ source('fudgeflix_v3', 'ff_zipcodes') }} z
        on a.account_zipcode = z.zip_code
),

fudgemart_customers as (
    select
        customer_id::varchar                    as customer_id,
        customer_email                          as customer_email,
        customer_firstname                      as customer_firstname,
        customer_lastname                       as customer_lastname,
        customer_address                        as customer_address,
        customer_city                           as customer_city,
        customer_state                          as customer_state,
        customer_zip                            as customer_zip,
        customer_phone                          as customer_phone,
        customer_fax                            as customer_fax
    from {{ source('fudgemart_v3', 'fm_customers') }}
),

email_matches as (
    select
        ff.customer_id as ff_customer_id,
        fm.customer_id as fm_customer_id
    from fudgeflix_customers ff
    inner join fudgemart_customers fm
        on lower(trim(ff.customer_email)) = lower(trim(fm.customer_email))
),

fudgemart_final as (
    select
        fm.*,
        case
            when em.fm_customer_id is not null then 'BOTH'
            else 'FudgeMart'
        end as division
    from fudgemart_customers fm
    left join email_matches em
        on fm.customer_id = em.fm_customer_id
),

fudgeflix_final as (
    select
        ff.*,
        'FudgeFlix' as division
    from fudgeflix_customers ff
    where ff.customer_id not in (select ff_customer_id from email_matches)
),

combined as (
    select * from fudgemart_final
    union all
    select * from fudgeflix_final
)

select
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'division']) }} as customer_key,
    combined.*
from combined