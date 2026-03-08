-- ============================================================
-- RAW.FUDGEFLIX_V3 — Snowflake Staging Tables
-- Modeled after Northwind pattern
-- Source: fudgeflix_v3 (SQL Server via Azure)
-- ============================================================

-- Independent tables (no FK dependencies)

CREATE OR REPLACE TABLE RAW.FUDGEFLIX_V3.ff_genres
(
    genre_name  varchar(200)
);
COPY INTO RAW.FUDGEFLIX_V3.ff_genres
    FROM '@RAW.PUBLIC.externalworld_database/fudgeflix_v3.ff_genres.parquet'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


CREATE OR REPLACE TABLE RAW.FUDGEFLIX_V3.ff_people
(
    people_id   int,
    people_name varchar(200)
);
COPY INTO RAW.FUDGEFLIX_V3.ff_people
    FROM '@RAW.PUBLIC.externalworld_database/fudgeflix_v3.ff_people.parquet'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


CREATE OR REPLACE TABLE RAW.FUDGEFLIX_V3.ff_plans
(
    plan_id     int,
    plan_name   varchar(50),
    plan_price  decimal(19,4),  -- money mapped to decimal
    plan_current boolean        -- bit mapped to boolean
);
COPY INTO RAW.FUDGEFLIX_V3.ff_plans
    FROM '@RAW.PUBLIC.externalworld_database/fudgeflix_v3.ff_plans.parquet'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


CREATE OR REPLACE TABLE RAW.FUDGEFLIX_V3.ff_zipcodes
(
    zip_code    char(5),
    zip_city    varchar(50),
    zip_state   char(2)
);
COPY INTO RAW.FUDGEFLIX_V3.ff_zipcodes
    FROM '@RAW.PUBLIC.externalworld_database/fudgeflix_v3.ff_zipcodes.parquet'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


CREATE OR REPLACE TABLE RAW.FUDGEFLIX_V3.ff_titles
(
    title_id                varchar(20),
    title_name              varchar(200),
    title_type              varchar(20),
    title_synopsis          varchar,        -- varchar(MAX) in source; no limit in Snowflake
    title_avg_rating        decimal(18,2),
    title_release_year      int,
    title_runtime           int,
    title_rating            varchar(20),
    title_bluray_available  boolean,        -- bit mapped to boolean
    title_dvd_available     boolean,        -- bit mapped to boolean
    title_instant_available boolean,        -- bit mapped to boolean
    title_date_modified     varchar         -- datetime stored as varchar
);
COPY INTO RAW.FUDGEFLIX_V3.ff_titles
    FROM '@RAW.PUBLIC.externalworld_database/fudgeflix_v3.ff_titles.parquet'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


-- Tables with FK dependencies

CREATE OR REPLACE TABLE RAW.FUDGEFLIX_V3.ff_accounts
(
    account_id          int,
    account_email       varchar(200),
    account_firstname   varchar(50),
    account_lastname    varchar(50),
    account_address     varchar(1000),
    account_zipcode     char(5),        -- FK -> ff_zipcodes.zip_code
    account_plan_id     int,            -- FK -> ff_plans.plan_id (nullable)
    account_opened_on   varchar         -- datetime stored as varchar (nullable)
);
COPY INTO RAW.FUDGEFLIX_V3.ff_accounts
    FROM '@RAW.PUBLIC.externalworld_database/fudgeflix_v3.ff_accounts.parquet'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


-- Junction / child tables (depend on multiple parent tables)

CREATE OR REPLACE TABLE RAW.FUDGEFLIX_V3.ff_account_billing
(
    ab_id           int,
    ab_date         varchar,        -- datetime stored as varchar
    ab_account_id   int,            -- FK -> ff_accounts.account_id
    ab_plan_id      int,            -- FK -> ff_plans.plan_id
    ab_billed_amount decimal(19,4)  -- money mapped to decimal
);
COPY INTO RAW.FUDGEFLIX_V3.ff_account_billing
    FROM '@RAW.PUBLIC.externalworld_database/fudgeflix_v3.ff_account_billing.parquet'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


CREATE OR REPLACE TABLE RAW.FUDGEFLIX_V3.ff_account_titles
(
    at_id           int,
    at_account_id   int,        -- FK -> ff_accounts.account_id
    at_title_id     varchar(20), -- FK -> ff_titles.title_id
    at_queue_date   varchar,    -- datetime stored as varchar
    at_shipped_date varchar,    -- datetime stored as varchar (nullable)
    at_returned_date varchar,   -- datetime stored as varchar (nullable)
    at_rating       int         -- nullable
);
COPY INTO RAW.FUDGEFLIX_V3.ff_account_titles
    FROM '@RAW.PUBLIC.externalworld_database/fudgeflix_v3.ff_account_titles.parquet'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


CREATE OR REPLACE TABLE RAW.FUDGEFLIX_V3.ff_cast
(
    cast_people_id  int,            -- FK -> ff_people.people_id
    cast_title_id   varchar(20)     -- FK -> ff_titles.title_id
);
COPY INTO RAW.FUDGEFLIX_V3.ff_cast
    FROM '@RAW.PUBLIC.externalworld_database/fudgeflix_v3.ff_cast.parquet'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


CREATE OR REPLACE TABLE RAW.FUDGEFLIX_V3.ff_directors
(
    director_people_id  int,        -- FK -> ff_people.people_id
    director_title_id   varchar(20) -- FK -> ff_titles.title_id
);
COPY INTO RAW.FUDGEFLIX_V3.ff_directors
    FROM '@RAW.PUBLIC.externalworld_database/fudgeflix_v3.ff_directors.parquet'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


CREATE OR REPLACE TABLE RAW.FUDGEFLIX_V3.ff_title_genres
(
    tg_genre_name   varchar(200),   -- FK -> ff_genres.genre_name
    tg_title_id     varchar(20)     -- FK -> ff_titles.title_id
);
COPY INTO RAW.FUDGEFLIX_V3.ff_title_genres
    FROM '@RAW.PUBLIC.externalworld_database/fudgeflix_v3.ff_title_genres.parquet'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


-- ============================================================
-- Verify row counts after load
-- ============================================================
SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, ROW_COUNT
    FROM RAW.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'FUDGEFLIX_V3'
    ORDER BY TABLE_NAME;
