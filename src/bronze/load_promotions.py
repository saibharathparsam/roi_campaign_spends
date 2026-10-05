# Databricks notebook source
# MAGIC %md
# MAGIC # Load promotions data with Auto Loader
# MAGIC
# MAGIC Processes newly arrived promotion CSV files on demand and appends them to the Bronze table.

# COMMAND ----------

from typing import Any


spark_session: Any = globals()["spark"]
dbutils_runtime: Any = globals()["dbutils"]

source_path = "/Volumes/retail_demo/bronze/raw_data/csv/promotions/"
schema_path = "/Volumes/retail_demo/bronze/raw_data/_autoloader/promotions/schema/"
checkpoint_path = "/Volumes/retail_demo/bronze/raw_data/_autoloader/promotions/checkpoint/"
target_table = "retail_demo.bronze.promotions"

# COMMAND ----------

dbutils_runtime.widgets.dropdown(
    "is_reprocess",
    "false",
    ["false", "true"],
    "Reprocess all promotions data",
)
is_reprocess_value = (
    dbutils_runtime.widgets.get("is_reprocess").strip().lower()
)

if is_reprocess_value not in {"false", "true"}:
    raise ValueError("is_reprocess must be either 'false' or 'true'")

if is_reprocess_value == "true":
    spark_session.sql(f"DROP TABLE IF EXISTS {target_table}")
    dbutils_runtime.fs.rm(checkpoint_path, recurse=True)
    dbutils_runtime.fs.rm(schema_path, recurse=True)

# COMMAND ----------

promotions_stream = (
    spark_session.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .load(source_path)
    .selectExpr(
        "*",
        "_metadata.file_path AS _source_file_path",
        "_metadata.file_name AS _source_file_name",
        "_metadata.file_size AS _source_file_size",
        "_metadata.file_modification_time AS _source_file_modified_at",
        "current_timestamp() AS _ingested_at",
    )
)

# COMMAND ----------

query = (
    promotions_stream.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(target_table)
)

query.awaitTermination()
