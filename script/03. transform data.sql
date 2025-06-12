-- use appropriate database
USE WAREHOUSE etl_wh;
USE DATABASE covid_etl_db;
USE SCHEMA covid_etl_db.silver;

-- create table for silver
CREATE OR REPLACE TABLE silver.covid_data (
    iso_code STRING,
    continent STRING,
    location STRING,
    date DATE,
    total_cases FLOAT,
    new_cases  FLOAT,
    total_deaths FLOAT,
    new_deaths FLOAT
);

-- Load cleaned/transformed data from bronze.covid_data > to > silver.covid_data
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
    CASE WHEN new_deaths IS NULL THEN 0 ELSE new_deaths END new_deaths,
FROM bronze.covid_data;