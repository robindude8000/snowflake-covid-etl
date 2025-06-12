-- use appropriate database
USE WAREHOUSE etl_wh;
USE DATABASE covid_etl_db;
USE SCHEMA covid_etl_db.bronze;

-- create staging area
CREATE OR REPLACE STAGE bronze.covid_stage;

-- create table to hold covid_data
CREATE OR REPLACE TABLE bronze.covid_data (
    iso_code STRING,
    continent STRING,
    location STRING,
    date DATE,
    total_cases FLOAT,
    new_cases  FLOAT,
    total_deaths FLOAT,
    new_deaths FLOAT
);

-- load data from staging area
TRUNCATE TABLE bronze.covid_data;

COPY INTO bronze.covid_data (iso_code, continent, location, date, total_cases, new_cases, total_deaths, new_deaths)
FROM (
    SELECT -- use '$' when referencing csv coz snowflake doesn't read field names 
        $1,
        $2,
        $3,
        $4,
        TRY_TO_NUMBER($5),
        TRY_TO_NUMBER($6),
        TRY_TO_NUMBER($8),
        TRY_TO_NUMBER($9)
    FROM @bronze.covid_stage
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- quick select
SELECT * FROM bronze.covid_data LIMIT 10;

