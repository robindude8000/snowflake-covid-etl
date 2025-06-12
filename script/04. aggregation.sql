-- use appropriate database
USE WAREHOUSE etl_wh;
USE DATABASE covid_etl_db;
USE SCHEMA covid_etl_db.gold;


-- create tables for gold
TRUNCATE TABLE gold.covid_summary;

CREATE OR REPLACE TABLE gold.covid_summary AS
SELECT
    location,
    DATE_TRUNC('month', date) AS report_month,
    SUM(new_cases) AS total_new_cases,
    SUM(new_deaths) AS total_new_deaths,
FROM silver.covid_data
GROUP BY location, DATE_TRUNC('month', date)
ORDER BY location, report_month;


-- create a view for monthly summary
CREATE OR REPLACE VIEW gold.vw_covid_monthly_summary AS 
SELECT
    location,
    TO_CHAR(report_month, 'YYYY-MM') AS year_month,
    total_new_cases,
    total_new_deaths
FROM gold.covid_summary
ORDER BY location, year_month;


-- call view
SELECT * FROM gold.vw_covid_monthly_summary;