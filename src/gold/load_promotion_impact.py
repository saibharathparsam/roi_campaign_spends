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

# Validate the Silver contract before starting any aggregation. Percentage
# columns use whole percentage points, for example 10 means 10%.
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

# Establish one baseline per product across all rows classified as Baseline.
# These totals are assigned to every promotion for the same product so that
# promotion performance can be compared with non-promotion performance.
baseline_by_product = (
    promotion_sales.filter(F.col("promotion") == "Baseline")
    .groupBy("product_id")
    .agg(
        F.sum("units_sold").alias("baseline_units"),
        F.sum("revenue").alias("baseline_revenue"),
    )
)

# Establish one row per product and promotion. Descriptive attributes are
# aggregated with max() so they do not alter the (product_id, promotion_id)
# grain required by the target MERGE.
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

# Attach each product's baseline to all its promotions. A product with no
# Baseline rows receives zero baseline units and revenue. Null investment is
# also normalized to zero so downstream metrics have deterministic inputs.
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
    # Additional units sold during the promotion compared with the product's
    # aggregated baseline units. Negative values indicate underperformance.
    promotion_with_baseline.withColumn(
        "incremental_units",
        F.col("promotion_units") - F.col("baseline_units"),
    )
    # Average realized selling price for promotion units. A zero-unit
    # promotion returns NULL to avoid division by zero.
    .withColumn(
        "net_selling_price",
        F.when(
            F.col("promotion_units") != 0,
            F.col("promotion_revenue") / F.col("promotion_units"),
        ).otherwise(F.lit(None).cast("double")),
    )
    # Revenue attributable to units above or below baseline, valued at the
    # average promotion selling price.
    .withColumn(
        "incremental_revenue",
        F.col("incremental_units") * F.col("net_selling_price"),
    )
    # Profit associated with incremental revenue. gross_margin_pct - discount_pct is stored
    # as percentage points, so it is divided by 100 before multiplication.
    .withColumn(
        "incremental_gross_profit",
        F.col("incremental_revenue")
        * ( (F.col("gross_margin_pct") - F.col("discount_pct")) / F.lit(100.0)),
    )
    # Percentage change in promotion revenue relative to baseline revenue.
    # A zero baseline produces NULL because percentage uplift is undefined.
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
    # Percentage change in promotion units relative to baseline units.
    # A zero baseline produces NULL because percentage uplift is undefined.
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
    # Classify effectiveness from unrounded revenue uplift: at least 20% is
    # HIGH, positive but below 20% is MEDIUM, and all other cases are LOW.
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
    # Round percentage outputs only after all calculations and classifications
    # so displayed precision does not affect business-rule thresholds.
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

# The Gold table is fully derived. Recreate it when its columns differ from the
# current output so renamed or newly added metrics do not break the MERGE.
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

# Synchronize the target at the (product_id, promotion_id) grain: update current
# promotions, insert new ones, and delete promotions no longer found in Silver.
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
