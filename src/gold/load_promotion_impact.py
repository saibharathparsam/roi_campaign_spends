# Databricks notebook source
# MAGIC %md
# MAGIC # Build Gold promotion impact
# MAGIC
# MAGIC Compares each product promotion with the product's aggregated baseline
# MAGIC performance and calculates incremental sales and uplift metrics.

# COMMAND ----------

from typing import Any

from pyspark.sql import functions as F


spark_session: Any = globals()["spark"]

source_table = "retail_demo.silver.promotion_sales"
target_table = "retail_demo.gold.promotion_impact"

# COMMAND ----------

promotion_sales = spark_session.table(source_table)

required_columns = {
    "product_id",
    "product_name",
    "category",
    "promotion_id",
    "promotion_name",
    "promotion",
    "regular_price",
    "discount_pct",
    "gross_margin_pct",
    "units_sold",
    "revenue",
    "discount_amount",
}
missing_columns = required_columns.difference(promotion_sales.columns)
if missing_columns:
    raise ValueError(
        "Silver promotion sales is missing required columns: "
        + ", ".join(sorted(missing_columns))
    )

invalid_percentage = promotion_sales.filter(
    (
        F.col("discount_pct").isNotNull()
        & ~F.col("discount_pct").between(0, 100)
    )
    | (
        F.col("gross_margin_pct").isNotNull()
        & ~F.col("gross_margin_pct").between(0, 100)
    )
).limit(1).count()
if invalid_percentage:
    raise ValueError(
        "discount_pct and gross_margin_pct must be between 0 and 100"
    )

baseline_by_product = (
    promotion_sales.filter(F.col("promotion") == "Baseline")
    .groupBy("product_id")
    .agg(
        F.sum("units_sold").alias("baseline_units"),
        F.sum("revenue").alias("baseline_revenue"),
    )
)

promotion_by_product = (
    promotion_sales.filter(F.col("promotion") == "Promotion")
    .filter(F.col("promotion_id").isNotNull())
    .filter(F.col("product_id").isNotNull())
    .groupBy(
        "product_id",
        "promotion_id",
    )
    .agg(
        F.max("product_name").alias("product_name"),
        F.max("promotion_name").alias("promotion_name"),
        F.max("category").alias("category"),
        F.max("regular_price").alias("regular_price"),
        F.max("gross_margin_pct").alias("gross_margin_pct"),
        F.max("discount_pct").alias("discount_pct"),
        F.sum("units_sold").alias("promotion_units"),
        F.sum("revenue").alias("promotion_revenue"),
        F.sum("discount_amount").alias("promotion_investment"),
    )
)

# COMMAND ----------

promotion_with_baseline = (
    promotion_by_product.join(
        baseline_by_product,
        on="product_id",
        how="left",
    )
    .fillna(
        {
            "baseline_units": 0,
            "baseline_revenue": 0,
            "promotion_investment": 0,
        }
    )
    .withColumn("promotion", F.lit("Promotion"))
)

promotion_with_metrics = (
    promotion_with_baseline.withColumn(
        "incremental_units",
        F.col("promotion_units") - F.col("baseline_units"),
    )
    .withColumn(
        "net_selling_price",
        F.when(
            F.col("promotion_units") != 0,
            F.col("promotion_revenue") / F.col("promotion_units"),
        ).otherwise(F.lit(None).cast("double")),
    )
    .withColumn(
        "incremental_revenue",
        F.col("incremental_units") * F.col("net_selling_price"),
    )
    .withColumn(
        "incremental_gross_profit",
        F.col("incremental_revenue")
        * (F.col("gross_margin_pct") / F.lit(100.0)),
    )
    .withColumn(
        "revenue_uplift_pct",
        F.when(
            F.col("baseline_revenue") != 0,
            (
                (
                    F.col("promotion_revenue")
                    - F.col("baseline_revenue")
                )
                / F.col("baseline_revenue")
            )
            * F.lit(100.0),
        ).otherwise(F.lit(None).cast("double")),
    )
    .withColumn(
        "unit_uplift_pct",
        F.when(
            F.col("baseline_units") != 0,
            (
                (
                    F.col("promotion_units")
                    - F.col("baseline_units")
                )
                / F.col("baseline_units")
            )
            * F.lit(100.0),
        ).otherwise(F.lit(None).cast("double")),
    )
    .withColumn(
        "promotion_effectiveness",
        F.when(
            F.col("revenue_uplift_pct") >= F.lit(20.0),
            F.lit("HIGH"),
        )
        .when(
            F.col("revenue_uplift_pct") > F.lit(0.0),
            F.lit("MEDIUM"),
        )
        .otherwise(F.lit("LOW")),
    )
    .withColumn("discount_pct", F.round("discount_pct", 2))
    .withColumn(
        "gross_margin_pct",
        F.round("gross_margin_pct", 2),
    )
    .withColumn(
        "revenue_uplift_pct",
        F.round("revenue_uplift_pct", 2),
    )
    .withColumn(
        "unit_uplift_pct",
        F.round("unit_uplift_pct", 2),
    )
    .withColumn("gold_ingested_at", F.current_timestamp())
    .select(
        "product_id",
        "product_name",
        "category",
        "promotion_id",
        "promotion_name",
        "promotion",
        "regular_price",
        "discount_pct",
        "gross_margin_pct",
        "baseline_units",
        "baseline_revenue",
        "promotion_units",
        "promotion_revenue",
        "net_selling_price",
        "promotion_investment",
        "incremental_units",
        "incremental_revenue",
        "incremental_gross_profit",
        "revenue_uplift_pct",
        "unit_uplift_pct",
        "promotion_effectiveness",
        "gold_ingested_at",
    )
)

promotion_with_metrics.createOrReplaceTempView(
    "promotion_impact_source"
)

# COMMAND ----------

if spark_session.catalog.tableExists(target_table):
    target_columns = set(spark_session.table(target_table).columns)
    source_columns = set(promotion_with_metrics.columns)
    if target_columns != source_columns:
        spark_session.sql(f"DROP TABLE {target_table}")

spark_session.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {target_table}
    USING DELTA
    CLUSTER BY (promotion_id, product_id)
    AS
    SELECT *
    FROM promotion_impact_source
    WHERE 1 = 0
    """
)

spark_session.sql(
    f"""
    MERGE INTO {target_table} AS target
    USING promotion_impact_source AS source
        ON target.product_id <=> source.product_id
        AND target.promotion_id <=> source.promotion_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE THEN DELETE
    """
)
