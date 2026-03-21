with date_spine as (
    select
        dateadd(day, seq4(), '2000-01-01'::date) as full_date
    from table(generator(rowcount => 20000))
),

dates as (
    select
        replace(full_date::varchar, '-', '')::int       as date_key,
        full_date,
        dayname(full_date)                              as day_name,
        monthname(full_date)                             as month_name,
        'Q' || quarter(full_date)                       as quarter,
        year(full_date)::varchar                         as year,
        case when dayofweek(full_date) in (0, 6)
             then 'Yes' else 'No'
        end                                              as is_weekend,
        'No'::varchar                                    as is_holiday
    from date_spine
    where full_date <= '2030-12-31'
)

select * from dates