-- Orders Pipeline for Production
-- This pipeline processes orders data for production use

CREATE OR REFRESH STREAMING TABLE ldp_demo.ldp_schema.orders_bronze_demo5
  COMMENT "Ingest raw JSON order files from cloud storage"
  TBLPROPERTIES (
    "quality" = "bronze",
    "pipelines.reset.allowed" = false
  )
AS 
SELECT 
  *,
  current_timestamp() AS processing_time, 
  _metadata.file_name AS source_file
FROM STREAM read_files(
  "${source}/orders", 
  format => "json");

CREATE OR REFRESH STREAMING TABLE ldp_demo.ldp_schema.orders_silver_demo5
  COMMENT "Cleaned and transformed orders data"
  TBLPROPERTIES ("quality" = "silver")
AS 
SELECT
  order_id,
  timestamp(order_timestamp) AS order_timestamp, 
  customer_id,
  notifications
FROM STREAM ldp_demo.ldp_schema.orders_bronze_demo5;

CREATE OR REFRESH MATERIALIZED VIEW ldp_demo.ldp_schema.orders_by_date_gold_demo5
  COMMENT "Daily order aggregations"
  TBLPROPERTIES ("quality" = "gold")
AS 
SELECT 
  date(order_timestamp) AS order_date, 
  count(*) AS total_daily_orders
FROM ldp_demo.ldp_schema.orders_silver_demo5                               
GROUP BY date(order_timestamp);

