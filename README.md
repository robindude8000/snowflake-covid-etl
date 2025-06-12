# 🧊 Snowflake COVID-19 ETL Project

A complete end-to-end ETL pipeline project using **Snowflake** to load, transform, and summarize global COVID-19 data. This project demonstrates the Medallion Architecture (Bronze → Silver → Gold), data quality checks, scheduling via Snowflake Tasks, and Git-based version control for real-world analytics and reporting workflows.

---

## 📊 Project Architecture

Local CSV Files (2020–2023)
⬇
Stage (@covid_stage)
⬇
Bronze Layer (Raw load)
⬇
Silver Layer (Cleaned + Transformed)
⬇
Gold Layer (Aggregated Monthly Summary)

---

## 🏗️ Features

- Medallion architecture (Bronze, Silver, Gold layers)
- COPY INTO with pattern matching for multi-file load
- Data quality checks using SQL blocks
- Scheduled ETL using Snowflake Tasks
- Modular & parameterized stored procedures
- Dashboard Creation

---

## 🔧 Technologies Used

- Snowflake (SQL, Tasks, Stored Procedures)
- CSV Files (2020–2024 OWID COVID data)

---

## ⚙️ How It Works

### 1. **Data Ingestion**
- Load multiple yearly CSV files from stage (`@covid_stage`) using `COPY INTO` with pattern matching.

### 2. **Bronze Layer**
- Raw data is loaded as-is with selected fields:  
  `iso_code`, `continent`, `location`, `date`, `total_cases`, `new_cases`, `total_deaths`, `new_deaths`

### 3. **Silver Layer**
- Transformed and Cleaned data

### 4. **Gold Layer**
- Monthly aggregated summaries (new cases, new deaths)

---

## 🕒 Scheduling ETL

ETL is run automatically using:

```sql
CREATE TASK run_etl_daily
WAREHOUSE = etl_wh
SCHEDULE = 'USING CRON 0 0 * * * UTC'
AS
    CALL sp_run_etl();
