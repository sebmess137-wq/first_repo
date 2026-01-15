-- Customers Pipeline with AUTO CDC
-- This pipeline demonstrates change data capture (CDC) with SCD Type 1 using AUTO CDC INTO

-- STEP 1: Create Bronze Streaming Table
CREATE OR REFRESH STREAMING TABLE ldp_demo.ldp_schema.customers_bronze_demo6
  COMMENT "Ingest raw JSON customer files from cloud storage"
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
  "${source}/customers", 
  format => "json");

-- STEP 2: Create Bronze Clean Streaming Table with Data Quality Enforcement
CREATE OR REFRESH STREAMING TABLE ldp_demo.ldp_schema.customers_bronze_clean_demo6
  (
    CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
    CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_name EXPECT (name IS NOT NULL),
    CONSTRAINT valid_address EXPECT (
      operation = 'DELETE' OR 
      (address IS NOT NULL AND city IS NOT NULL AND state IS NOT NULL)
    ),
    CONSTRAINT valid_email EXPECT (
      operation = 'DELETE' OR 
      rlike(email, '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$')
    ) ON VIOLATION DROP ROW
  )
  COMMENT "Cleaned bronze customer data with quality checks"
  TBLPROPERTIES ("quality" = "bronze")
AS 
SELECT
  customer_id,
  name,
  email,
  address,
  city,
  state,
  operation,
  timestamp
FROM STREAM ldp_demo.ldp_schema.customers_bronze_demo6;

-- STEP 3: Processing CDC Data with AUTO CDC INTO
-- Create SCD Type 1 customers silver table using AUTO CDC
CREATE OR REFRESH STREAMING TABLE ldp_demo.ldp_schema.scd_type_1_customers_silver_demo6
  COMMENT "SCD Type 1 customers table with CDC processing"
  TBLPROPERTIES ("quality" = "silver")
AS 
AUTO CDC INTO ldp_demo.ldp_schema.scd_type_1_customers_silver_demo6
FROM STREAM ldp_demo.ldp_schema.customers_bronze_clean_demo6
KEYS (customer_id)
APPLY AS DELETE WHEN operation = "DELETE"
SEQUENCE BY timestamp
STORED AS SCD TYPE 1
EXCEPT (operation, source_file, processing_time);

-- STEP 4: Create Gold Materialized View
CREATE OR REFRESH MATERIALIZED VIEW ldp_demo.ldp_schema.current_customers_gold_demo6
  COMMENT "Current active customers"
  TBLPROPERTIES ("quality" = "gold")
AS 
SELECT
  customer_id,
  name,
  email,
  address,
  city,
  state
FROM ldp_demo.ldp_schema.scd_type_1_customers_silver_demo6;

