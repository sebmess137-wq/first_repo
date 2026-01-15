-- Status Pipeline for Production
-- This pipeline processes order status data and joins with orders data

-- A. JSON -> Bronze
CREATE OR REFRESH STREAMING TABLE ldp_demo.ldp_schema.status_bronze_demo5
  COMMENT "Ingest raw JSON order status files from cloud storage"
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
  "${source}/status", 
  format => "json");

-- B. Bronze -> Silver
CREATE OR REFRESH STREAMING TABLE ldp_demo.ldp_schema.status_silver_demo5
  (
    -- Drop rows if order_status_timestamp is not valid
    CONSTRAINT valid_timestamp EXPECT (order_status_timestamp > "2021-12-25") ON VIOLATION DROP ROW,
    -- Warn if order_status is not in the following
    CONSTRAINT valid_order_status EXPECT (order_status IN ('on the way','canceled','return canceled','delivered','return processed','placed','preparing'))
  )
  COMMENT "Order with each status and timestamp"
  TBLPROPERTIES ("quality" = "silver")
AS 
SELECT
  order_id,
  order_status,
  timestamp(status_timestamp) AS order_status_timestamp
FROM STREAM ldp_demo.ldp_schema.status_bronze_demo5;

-- C. Use a Materialized View to Join Two Streaming Tables
CREATE OR REFRESH MATERIALIZED VIEW ldp_demo.ldp_schema.full_order_info_gold_demo5
  COMMENT "Joining the orders and order status silver tables to view all orders with each individual status per order"
  TBLPROPERTIES ("quality" = "gold")
AS 
SELECT
  orders.order_id,
  orders.order_timestamp,
  status.order_status,
  status.order_status_timestamp
FROM ldp_demo.ldp_schema.status_silver_demo5 status    
  INNER JOIN ldp_demo.ldp_schema.orders_silver_demo5 orders 
  ON orders.order_id = status.order_id;

-- D. Create Materialized Views for Cancelled and Delivered Orders
-- CANCELLED ORDERS MV
CREATE OR REFRESH MATERIALIZED VIEW ldp_demo.ldp_schema.cancelled_orders_gold_demo5
  COMMENT "All cancelled orders"
  TBLPROPERTIES ("quality" = "gold")
AS 
SELECT
  order_id,
  order_timestamp,
  order_status,
  order_status_timestamp,
  datediff(DAY,order_timestamp, order_status_timestamp) AS days_to_cancel
FROM ldp_demo.ldp_schema.full_order_info_gold_demo5
WHERE order_status = 'canceled';

-- DELIVERED ORDERS MV
CREATE OR REFRESH MATERIALIZED VIEW ldp_demo.ldp_schema.delivered_orders_gold_demo5
  COMMENT "All delivered orders"
  TBLPROPERTIES ("quality" = "gold")
AS 
SELECT
  order_id,
  order_timestamp,
  order_status,
  order_status_timestamp,
  datediff(DAY,order_timestamp, order_status_timestamp) AS days_to_delivery
FROM ldp_demo.ldp_schema.full_order_info_gold_demo5
WHERE order_status = 'delivered';

