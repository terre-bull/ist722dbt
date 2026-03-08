with channels as (
    select 1 as channel_key, 'FudgeMart' as channel_name, 'Retail'       as channel_model
    union all
    select 2 as channel_key, 'FudgeFlix' as channel_name, 'Subscription' as channel_model
)

select * from channels