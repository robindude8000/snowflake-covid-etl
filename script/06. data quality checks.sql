-- use appropriate database
USE WAREHOUSE etl_wh;
USE DATABASE covid_etl_db;


-- ===============================
-- data quality check
-- ===============================

-- check for null records
SELECT COUNT(*) iso_code FROM silver.covid_data WHERE iso_code IS null;
SELECT COUNT(*) continent FROM silver.covid_data WHERE continent IS null;
SELECT COUNT(*) location FROM silver.covid_data WHERE location IS null;

-- check for invalid naming
SELECT LEN(iso_code) iso_code FROM silver.covid_data GROUP BY LEN(iso_code) HAVING LEN(iso_code) != 3; -- should be 0
SELECT DISTINCT(continent) continent FROM silver.covid_data; -- shouldn't have nulls
SELECT count(*) loc_ount FROM silver.covid_data WHERE location IS NULL; -- shouldn't have result value

-- check for date validity
SELECT * FROM silver.covid_data WHERE date IS NULL;


