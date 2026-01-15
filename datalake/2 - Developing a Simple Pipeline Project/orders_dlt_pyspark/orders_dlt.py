import dlt
from pyspark.sql.functions import col, current_timestamp, to_date

# Step 2: Create Bronze Streaming Table
@dlt.table(
    name="orders_bronze_demo2",
    comment="Ingest raw JSON order files from cloud storage",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false"
    }
)
def orders_bronze_demo2():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/Volumes/cetpa_external_catalog/ldp_schema/raw/orders/")
        .select(
            "*",
            current_timestamp().alias("processing_time"),
            col("_metadata.file_name").alias("source_file")
        )
    )

# Step 3: Create Silver Streaming Table
@dlt.table(
    name="orders_silver_demo2",
    comment="Cleaned and transformed orders data",
    table_properties={"quality": "silver"}
)
def orders_silver_demo2():
    return (
        dlt.read_stream("orders_bronze_demo2")
        .select(
            "order_id",
            col("order_timestamp").cast("timestamp"),
            "customer_id",
            "notifications"
        )
    )

# Step 4: Create Gold Materialized View
@dlt.table(
    name="orders_by_date_gold_demo2",
    comment="Daily order aggregations",
    table_properties={"quality": "gold"}
)
def orders_by_date_gold_demo2():
    return (
        dlt.read("orders_silver_demo2")
        .groupBy(to_date("order_timestamp").alias("order_date"))
        .agg({"*": "count"})
        .withColumnRenamed("count(1)", "total_daily_orders")
    )