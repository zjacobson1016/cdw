CREATE OR REFRESH STREAMING TABLE bronze_orders
CLUSTER BY (order_date)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/mfg_mc_se_sa/cdw_sales_forecast/raw_data/orders/',
  format => 'parquet'
);
