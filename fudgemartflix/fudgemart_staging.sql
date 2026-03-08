-- ============================================================
-- RAW.FUDGEMART_V3 — Snowflake Staging Tables
-- Modeled after Northwind pattern
-- Source: fudgemart_v3 (SQL Server via Azure)
-- ============================================================

-- Lookup tables first (no FK dependencies)

CREATE OR REPLACE TABLE RAW.FUDGEMART_V3.fm_departments_lookup
(
    department_id varchar(20)
);
COPY INTO RAW.FUDGEMART_V3.fm_departments_lookup
    FROM '@RAW.PUBLIC.externalworld_database/fudgemart_v3.fm_departments_lookup.parquet'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


CREATE OR REPLACE TABLE RAW.FUDGEMART_V3.fm_jobtitles_lookup
(
    jobtitle_id varchar(20)
);
COPY INTO RAW.FUDGEMART_V3.fm_jobtitles_lookup
    FROM '@RAW.PUBLIC.externalworld_database/fudgemart_v3.fm_jobtitles_lookup.parquet'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


CREATE OR REPLACE TABLE RAW.FUDGEMART_V3.fm_shipvia_lookup
(
    ship_via varchar(20)
);
COPY INTO RAW.FUDGEMART_V3.fm_shipvia_lookup
    FROM '@RAW.PUBLIC.externalworld_database/fudgemart_v3.fm_shipvia_lookup.parquet'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


-- Independent entity tables

CREATE OR REPLACE TABLE RAW.FUDGEMART_V3.fm_creditcards
(
    creditcard_id       int,
    creditcard_number   varchar(50),
    creditcard_exp_date varchar        -- datetime stored as varchar (matches Northwind date pattern)
);
COPY INTO RAW.FUDGEMART_V3.fm_creditcards
    FROM '@RAW.PUBLIC.externalworld_database/fudgemart_v3.fm_creditcards.parquet'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


CREATE OR REPLACE TABLE RAW.FUDGEMART_V3.fm_customers
(
    customer_id         int,
    customer_email      varchar(100),
    customer_firstname  varchar(50),
    customer_lastname   varchar(50),
    customer_address    varchar(255),
    customer_city       varchar(50),
    customer_state      char(2),
    customer_zip        varchar(20),
    customer_phone      varchar(30),
    customer_fax        varchar(30)
);
COPY INTO RAW.FUDGEMART_V3.fm_customers
    FROM '@RAW.PUBLIC.externalworld_database/fudgemart_v3.fm_customers.parquet'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


CREATE OR REPLACE TABLE RAW.FUDGEMART_V3.fm_vendors
(
    vendor_id       int,
    vendor_name     varchar(50),
    vendor_phone    varchar(20),
    vendor_website  varchar(1000)
);
COPY INTO RAW.FUDGEMART_V3.fm_vendors
    FROM '@RAW.PUBLIC.externalworld_database/fudgemart_v3.fm_vendors.parquet'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


-- Tables with FK dependencies on lookups/independent tables

CREATE OR REPLACE TABLE RAW.FUDGEMART_V3.fm_employees
(
    employee_id             int,
    employee_ssn            char(9),
    employee_lastname       varchar(50),
    employee_firstname      varchar(50),
    employee_jobtitle       varchar(20),    -- FK -> fm_jobtitles_lookup.jobtitle_id
    employee_department     varchar(20),    -- FK -> fm_departments_lookup.department_id
    employee_birthdate      varchar,        -- datetime stored as varchar
    employee_hiredate       varchar,        -- datetime stored as varchar
    employee_termdate       varchar,        -- datetime stored as varchar (nullable)
    employee_hourlywage     decimal(19,4),  -- money type mapped to decimal
    employee_fulltime       boolean,        -- bit mapped to boolean
    employee_supervisor_id  int             -- self-referencing (nullable)
);
COPY INTO RAW.FUDGEMART_V3.fm_employees
    FROM '@RAW.PUBLIC.externalworld_database/fudgemart_v3.fm_employees.parquet'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


CREATE OR REPLACE TABLE RAW.FUDGEMART_V3.fm_products
(
    product_id              int,
    product_department      varchar(20),    -- FK -> fm_departments_lookup.department_id
    product_name            varchar(50),
    product_retail_price    decimal(19,4),  -- money mapped to decimal
    product_wholesale_price decimal(19,4),  -- money mapped to decimal
    product_is_active       boolean,        -- bit mapped to boolean
    product_add_date        varchar,        -- datetime stored as varchar
    product_vendor_id       int,            -- FK -> fm_vendors.vendor_id
    product_description     varchar(1000)
);
COPY INTO RAW.FUDGEMART_V3.fm_products
    FROM '@RAW.PUBLIC.externalworld_database/fudgemart_v3.fm_products.parquet'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


CREATE OR REPLACE TABLE RAW.FUDGEMART_V3.fm_orders
(
    order_id    int,
    customer_id int,            -- FK -> fm_customers.customer_id
    order_date  varchar,        -- datetime stored as varchar
    shipped_date varchar,       -- datetime stored as varchar (nullable)
    ship_via    varchar(20),    -- FK -> fm_shipvia_lookup.ship_via
    creditcard_id int           -- FK -> fm_creditcards.creditcard_id
);
COPY INTO RAW.FUDGEMART_V3.fm_orders
    FROM '@RAW.PUBLIC.externalworld_database/fudgemart_v3.fm_orders.parquet'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


-- Junction / child tables (depend on multiple parent tables)

CREATE OR REPLACE TABLE RAW.FUDGEMART_V3.fm_customer_creditcards
(
    customer_id     int,    -- FK -> fm_customers.customer_id
    creditcard_id   int     -- FK -> fm_creditcards.creditcard_id
);
COPY INTO RAW.FUDGEMART_V3.fm_customer_creditcards
    FROM '@RAW.PUBLIC.externalworld_database/fudgemart_v3.fm_customer_creditcards.parquet'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


CREATE OR REPLACE TABLE RAW.FUDGEMART_V3.fm_customer_product_reviews
(
    customer_id     int,    -- FK -> fm_customers.customer_id
    product_id      int,    -- FK -> fm_products.product_id
    review_date     varchar, -- datetime stored as varchar
    review_stars    int
);
COPY INTO RAW.FUDGEMART_V3.fm_customer_product_reviews
    FROM '@RAW.PUBLIC.externalworld_database/fudgemart_v3.fm_customer_product_reviews.parquet'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


CREATE OR REPLACE TABLE RAW.FUDGEMART_V3.fm_order_details
(
    order_id    int,    -- FK -> fm_orders.order_id
    product_id  int,    -- FK -> fm_products.product_id
    order_qty   int
);
COPY INTO RAW.FUDGEMART_V3.fm_order_details
    FROM '@RAW.PUBLIC.externalworld_database/fudgemart_v3.fm_order_details.parquet'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


CREATE OR REPLACE TABLE RAW.FUDGEMART_V3.fm_employee_timesheets
(
    timesheet_id            int,
    timesheet_payrolldate   varchar,        -- datetime stored as varchar
    timesheet_hourlyrate    decimal(19,4),  -- money mapped to decimal
    timesheet_employee_id   int,            -- FK -> fm_employees.employee_id
    timesheet_hours         decimal(3,1)
);
COPY INTO RAW.FUDGEMART_V3.fm_employee_timesheets
    FROM '@RAW.PUBLIC.externalworld_database/fudgemart_v3.fm_employee_timesheets.parquet'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


-- ============================================================
-- Verify row counts after load
-- ============================================================
SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, ROW_COUNT
    FROM RAW.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'FUDGEMART_V3'
    ORDER BY TABLE_NAME;
