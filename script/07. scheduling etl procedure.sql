-- ======================
-- SCHEDULING ETL PROC
-- ======================

CREATE OR REPLACE TASK run_etl_daily
    WAREHOUSE = etl_wh
    SCHEDULE = 'USING CRON 0 0 * * * UTC' -- runs daily at 00:00 UTC
AS
    CALL sp_run_etl();

ALTER TASK run_etl_daily SUSPEND;

EXECUTE TASK run_etl_daily;
