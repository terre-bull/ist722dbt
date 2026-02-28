with stg_sales as (
    select 
        o.employeeid, 
        o.customerid,
        o.orderdate, 
        od.productid,
        od.orderid, 
        od.quantity,
        -- Snowflake handles these calculations natively
        od.quantity * od.unitprice as extendedpriceamount,
        od.quantity * od.unitprice * od.discount as discountamount,
        od.quantity * od.unitprice * (1 - od.discount) as soldamount
    from {{ source('northwind', 'Orders') }} o
    join {{ source('northwind', 'Order_Details') }} od 
        on o.orderid = od.orderid
)

select * from stg_sales