-- create small virtual whse
CREATE OR REPLACE WAREHOUSE etl_wh
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60 -- automatic pause if idle for 60 secs to save credits
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE; -- whse starts in paused state by default and starts when running a query

-- create database
CREATE OR REPLACE DATABASE covid_etl_db;

-- create schemas
CREATE OR REPLACE SCHEMA covid_etl_db.bronze;
CREATE OR REPLACE SCHEMA covid_etl_db.silver;
CREATE OR REPLACE SCHEMA covid_etl_db.gold;

-- use them
USE WAREHOUSE etl_wh;
USE DATABASE covid_etl_db;
