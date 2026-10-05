# Databricks notebook source
# MAGIC %md
# MAGIC # Build Silver promotion sales
# MAGIC
# MAGIC Incrementally enriches Bronze sales with product and promotion attributes,
# MAGIC then merges one record per `sale_id` and `sale_date`.

# COMMAND ----------

from typing import Any

from pyspark.sql import functions as F
from pyspark.sql.window import Window


spark_session: Any = globals()["spark"]
dbutils_runtime: Any = globals()["dbutils"]

source_sales_table = "retail_demo.bronze.sales"
source_product_table = "retail_demo.bronze.product"
source_promotions_table = "retail_demo.bronze.promotions"
target_table = "retail_demo.silver.promotion_sales"
checkpoint_path = (
    "/Volumes/retail_demo/bronze/raw_data/_checkpoints/"
    "silver/promotion_sales/"
)

# COMMAND ----------

dbutils_runtime.widgets.dropdown(
    "is_reprocess",
    "false",
    ["false", "true"],
    "Reprocess all Silver promotion sales data",
)
is_reprocess_value = (
    dbutils_runtime.widgets.get("is_reprocess").strip().lower()
)

if is_reprocess_value not in {"false", "true"}:
    raise ValueError("is_reprocess must be either 'false' or 'true'")

if is_reprocess_value == "true":
    spark_session.sql(f"DROP TABLE IF EXISTS {target_table}")
    dbutils_runtime.fs.rm(checkpoint_path, recurse=True)

# COMMAND ----------

spark_session.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {target_table}
    USING DELTA
    CLUSTER BY (sale_id, sale_date)
    AS
    SELECT
        s.sale_id,
        s.sale_date,
        s.store_id,
        s.product_id,
        product.product_name,
        product.category,
        product.regular_price,
        product.gross_margin_pct,
        promotion.promotion_id,
        promotion.discount_pct,
        promotion.promotion_name,
        s.units_sold,
        s.revenue,
        (product.regular_price * s.units_sold) - s.revenue
            AS discount_amount,
        CAST(NULL AS STRING) AS promotion,
        current_timestamp() AS silver_ingested_at
    FROM {source_sales_table} AS s
    LEFT JOIN {source_product_table} AS product
        ON s.product_id = product.product_id
    LEFT JOIN {source_promotions_table} AS promotion
        ON s.product_id = promotion.product_id
        AND s.sale_date BETWEEN promotion.start_date AND promotion.end_date
    WHERE 1 = 0
    """
)

# COMMAND ----------

def latest_by_key(
    data_frame: Any,
    key_columns: list[str],
) -> Any:
    order_columns = []
    if "_ingested_at" in data_frame.columns:
        order_columns.append(F.col("_ingested_at").desc_nulls_last())
    if "_source_file_modified_at" in data_frame.columns:
        order_columns.append(
            F.col("_source_file_modified_at").desc_nulls_last()
        )

    if not order_columns:
        return data_frame.dropDuplicates(key_columns)

    latest_window = Window.partitionBy(*key_columns).orderBy(
        *order_columns
    )
    return (
        data_frame.withColumn(
            "_latest_row_number",
            F.row_number().over(latest_window),
        )
        .filter(F.col("_latest_row_number") == 1)
        .drop("_latest_row_number")
    )


def merge_promotion_sales_batch(
    micro_batch_df: Any,
    batch_id: int,
) -> None:
    del batch_id

    sales = latest_by_key(
        micro_batch_df,
        ["sale_id", "sale_date"],
    ).alias("sales")
    products = latest_by_key(
        spark_session.table(source_product_table),
        ["product_id"],
    ).alias("product")
    promotions = latest_by_key(
        spark_session.table(source_promotions_table),
        ["promotion_id"],
    ).alias("promotion")

    promotion_match = (
        (F.col("sales.product_id") == F.col("promotion.product_id"))
        & F.col("sales.sale_date").between(
            F.col("promotion.start_date"),
            F.col("promotion.end_date"),
        )
    )

    final_result = (
        sales.join(
            products,
            F.col("sales.product_id") == F.col("product.product_id"),
            "left",
        )
        .join(promotions, promotion_match, "left")
        .select(
            F.col("sales.sale_id").alias("sale_id"),
            F.col("sales.sale_date").alias("sale_date"),
            F.col("sales.store_id").alias("store_id"),
            F.col("sales.product_id").alias("product_id"),
            F.col("product.product_name").alias("product_name"),
            F.col("product.category").alias("category"),
            F.col("product.regular_price").alias("regular_price"),
            F.col("product.gross_margin_pct").alias(
                "gross_margin_pct"
            ),
            F.col("promotion.promotion_id").alias("promotion_id"),
            F.col("promotion.discount_pct").alias("discount_pct"),
            F.col("sales.units_sold").alias("units_sold"),
            F.col("sales.revenue").alias("revenue"),
            (
                F.col("product.regular_price")
                * F.col("sales.units_sold")
                - F.col("sales.revenue")
            ).alias("discount_amount"),
            F.when(
                F.col("promotion.promotion_id").isNotNull(),
                F.lit("Promotion"),
            )
            .otherwise(F.lit("Baseline"))
            .alias("promotion"),
            F.current_timestamp().alias("silver_ingested_at"),
        )
    )

    final_result.createOrReplaceTempView(
        "promotion_sales_micro_batch"
    )

    spark_session.sql(
        f"""
        MERGE INTO {target_table} AS target
        USING promotion_sales_micro_batch AS source
            ON target.sale_id <=> source.sale_id
            AND target.sale_date <=> source.sale_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )

# COMMAND ----------

sales_stream = spark_session.readStream.table(source_sales_table)

query = (
    sales_stream.writeStream.foreachBatch(
        merge_promotion_sales_batch
    )
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
