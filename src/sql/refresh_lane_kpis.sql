-- Daily KPI snapshot, run by the Lakeflow Job after the pipeline update.
--
-- gold_lane_performance is a materialized view and always shows "now". This
-- task keeps one row per lane, carrier and day in a history table so trends
-- survive pipeline full refreshes.
--
-- :catalog and :schema are named parameter markers filled from the SQL task's
-- parameters (see resources/shipments.job.yml). IDENTIFIER() turns them into
-- a three-level Unity Catalog name.
-- REPLACE WHERE makes a re-run on the same day idempotent.

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.' || :schema || '.lane_kpi_history') (
  snapshot_date      DATE    COMMENT 'Day the snapshot was taken (UTC).',
  lane               STRING  COMMENT 'ORIGIN-DESTINATION hub codes.',
  carrier_code       STRING,
  shipments          BIGINT,
  delivered          BIGINT,
  delivered_on_time  BIGINT,
  exceptions         BIGINT,
  on_time_rate       DOUBLE,
  avg_transit_hours  DOUBLE
)
COMMENT 'Daily snapshots of gold_lane_performance.';

INSERT INTO IDENTIFIER(:catalog || '.' || :schema || '.lane_kpi_history')
REPLACE WHERE snapshot_date = current_date()
SELECT
  current_date() AS snapshot_date,
  lane,
  carrier_code,
  shipments,
  delivered,
  delivered_on_time,
  exceptions,
  on_time_rate,
  avg_transit_hours
FROM IDENTIFIER(:catalog || '.' || :schema || '.gold_lane_performance');
