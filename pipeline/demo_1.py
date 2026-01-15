
import dlt
from pyspark.sql.functions import (
    col, trim, initcap, lower, to_timestamp, row_number, count, sum as _sum
)
from pyspark.sql.window import Window

BASE_PATH = "/Volumes/cetpa_external_catalog/ldp_schema/raw"



@dlt.table(
    comment="Raw orders from JSON files"
)
def bronze_orders():
    return spark.read.json(f"{BASE_PATH}/orders/00.json")


@dlt.table(
    comment="Raw customers from JSON files"
)
def bronze_customers():
    return spark.read.json(f"{BASE_PATH}/customers/00.json")


@dlt.table(
    comment="Raw order status from JSON files"
)
def bronze_status():
    return spark.read.json(f"{BASE_PATH}/status/00.json")




@dlt.table(
    comment="Silver cleaned orders"
)
def silver_orders():
    return (
        dlt.read("bronze_orders")
        .dropDuplicates(["order_id"])
        .withColumn("order_timestamp", to_timestamp(col("order_timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    )


@dlt.table(
    comment="Silver cleaned customers"
)
def silver_customers():
    return (
        dlt.read("bronze_customers")
        .dropDuplicates(["customer_id"])
        .withColumn("name", initcap(trim(col("name"))))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    )


@dlt.table(
    comment="Silver cleaned order status"
)
def silver_status():
    return (
        dlt.read("bronze_status")
        .withColumn("status_timestamp", to_timestamp(col("status_timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("order_status", trim(col("order_status")))
    )




@dlt.table(
    comment="Latest status per order"
)
def gold_latest_order_status():
    window_spec = Window.partitionBy("order_id").orderBy(col("status_timestamp").desc())
    return (
        dlt.read("silver_status")
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )


@dlt.table(
    comment="Fact table joining orders, customers and latest status"
)
def gold_fact_orders():
    return (
        dlt.read("silver_orders").alias("o")
        .join(dlt.read("silver_customers").alias("c"), "customer_id", "left")
        .join(dlt.read("gold_latest_order_status").alias("s"), "order_id", "left")
        .select(
            col("o.order_id"),
            col("o.order_timestamp"),
            col("customer_id"),
            col("c.name").alias("customer_name"),
            col("c.city"),
            col("c.state"),
            col("o.notifications"),
            col("s.order_status"),
            col("s.status_timestamp").alias("latest_status_timestamp")
        )
    )


@dlt.table(
    comment="Daily order metrics for analytics"
)
def gold_daily_orders():
    return (
        dlt.read("gold_fact_orders")
        .withColumn("order_date", col("order_timestamp").cast("date"))
        .groupBy("order_date", "order_status")
        .agg(
            count("order_id").alias("total_orders"),
            _sum(col("notifications").cast("int")).alias("total_notifications")
        )
    )
