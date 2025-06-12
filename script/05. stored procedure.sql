-- use appropriate database
USE WAREHOUSE etl_wh;
USE DATABASE covid_etl_db;

CREATE OR REPLACE PROCEDURE sp_run_etl()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    -- ==================================
    -- Step 1: Refresh Bronze Layer
    -- ==================================

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


    
    -- ==================================
    -- Step 2: Refresh Silver Layer
    -- ==================================
    
    TRUNCATE TABLE silver.covid_data;
    
    INSERT INTO silver.covid_data (
        iso_code,
        continent,
        location,
        date,
        total_cases,
        new_cases,
        total_deaths,
        new_deaths
    )
    SELECT
        CASE WHEN iso_code LIKE 'OWID%' THEN SUBSTR(iso_code, 6, 3) ELSE iso_code END iso_code,
        CASE WHEN continent IS NULL THEN 'Unknown' ELSE continent END continent,
        location,
        date,
        CASE WHEN total_cases IS NULL THEN 0 ELSE total_cases END total_cases,
        CASE WHEN new_cases IS NULL THEN 0 ELSE new_cases END new_cases,
        CASE WHEN total_deaths IS NULL THEN 0 ELSE total_deaths END total_deaths,
        CASE WHEN new_deaths IS NULL THEN 0 ELSE new_deaths END new_deaths
    FROM bronze.covid_data;


    
    -- ==================================
    -- Step 2: Refresh Gold Layer
    -- ==================================

    TRUNCATE TABLE gold.covid_summary;

    INSERT INTO gold.covid_summary
    SELECT
        location,
        DATE_TRUNC('month', date) AS report_month,
        SUM(new_cases) AS total_new_cases,
        SUM(new_deaths) AS total_new_deaths
    FROM silver.covid_data
    GROUP BY location, DATE_TRUNC('month', date);

    RETURN 'ETL Complete.';

END;
$$;