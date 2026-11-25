-- Run once in the BigQuery console (replace my-gcp-project and the region).
-- arf_results    : every check from every run (audit trail)
-- arf_exceptions : only WARN / FAIL / ERROR rows (what needs attention)

CREATE SCHEMA IF NOT EXISTS `my-gcp-project.arf` OPTIONS (location = 'US');

CREATE TABLE IF NOT EXISTS `my-gcp-project.arf.arf_results` (
  run_id        STRING,   -- schema must stay NULLABLE: ARF appends with a load job
  run_ts        TIMESTAMP,
  table_id      STRING,
  source_table  STRING,
  target_table  STRING,
  check_type    STRING,   -- schema | row_count | numeric | setup
  check_name    STRING,   -- missing_column, data_type, nullability, row_count, sum, min, max, null_count ...
  column_name   STRING,
  source_value  STRING,
  target_value  STRING,
  difference    STRING,
  tolerance     STRING,
  severity      STRING,   -- critical | warning
  status        STRING,   -- PASS | WARN | FAIL | ERROR | SKIPPED
  message       STRING,
  error_detail  STRING    -- traceback for ERROR rows
)
PARTITION BY DATE(run_ts)
CLUSTER BY table_id, status;

CREATE TABLE IF NOT EXISTS `my-gcp-project.arf.arf_exceptions`
LIKE `my-gcp-project.arf.arf_results`;

-- Latest run, problems only:
-- SELECT table_id, check_name, column_name, source_value, target_value, difference, status, message
-- FROM `my-gcp-project.arf.arf_exceptions`
-- WHERE run_id = (SELECT MAX(run_id) FROM `my-gcp-project.arf.arf_results`)
-- ORDER BY status, table_id;
