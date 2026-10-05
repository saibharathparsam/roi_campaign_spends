# Databricks notebook source
# MAGIC %md
# MAGIC # Load stores from Azure SQL
# MAGIC
# MAGIC Reads the current store dataset from Azure SQL and incrementally merges
# MAGIC new and changed rows into `retail_demo.bronze.store`.

# COMMAND ----------

from typing import Any


spark_session: Any = globals()["spark"]
dbutils_runtime: Any = globals()["dbutils"]

target_table = "retail_demo.bronze.store"

# COMMAND ----------

dbutils_runtime.widgets.text(
    "sql_server",
    "cusotmer.database.windows.net",
    "Azure SQL server",
)
dbutils_runtime.widgets.text(
    "sql_database",
    "customer",
    "Azure SQL database",
)
dbutils_runtime.widgets.text("source_table", "dbo.store", "Source table")
dbutils_runtime.widgets.text(
    "store_id_column",
    "store_id",
    "Store ID column",
)
dbutils_runtime.widgets.text(
    "sql_username",
    "",
    "Azure SQL username",
)
dbutils_runtime.widgets.text(
    "sql_password",
    "",
    "Azure SQL password",
)

sql_server = dbutils_runtime.widgets.get("sql_server").strip()
sql_database = dbutils_runtime.widgets.get("sql_database").strip()
source_table = dbutils_runtime.widgets.get("source_table").strip()
store_id_column = (
    dbutils_runtime.widgets.get("store_id_column").strip()
)
sql_username = dbutils_runtime.widgets.get("sql_username").strip()
sql_password = dbutils_runtime.widgets.get("sql_password")

required_values = {
    "sql_server": sql_server,
    "sql_database": sql_database,
    "source_table": source_table,
    "store_id_column": store_id_column,
    "sql_username": sql_username,
    "sql_password": sql_password,
}
missing_values = [
    name for name, value in required_values.items() if not value
]
if missing_values:
    raise ValueError(
        "Missing required parameters: " + ", ".join(missing_values)
    )

jdbc_url = (
    f"jdbc:sqlserver://{sql_server}:1433;"
    f"database={sql_database};"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;"
    "loginTimeout=30;"
)

store_source = (
    spark_session.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", source_table)
    .option("user", sql_username)
    .option("password", sql_password)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()
)

expected_columns = {
    "store_id",
    "store_name",
    "store_location",
    "store_zip",
}
missing_columns = expected_columns.difference(store_source.columns)
if missing_columns:
    raise ValueError(
        f"Azure SQL table '{source_table}' is missing columns: "
        + ", ".join(sorted(missing_columns))
    )

if store_id_column not in store_source.columns:
    raise ValueError(
        f"Store ID column '{store_id_column}' does not exist in "
        f"Azure SQL table '{source_table}'"
    )

if store_source.filter(
    store_source[store_id_column].isNull()
).limit(1).count():
    raise ValueError(
        f"Store ID column '{store_id_column}' contains null values"
    )

if store_source.groupBy(store_id_column).count().filter(
    "count > 1"
).limit(1).count():
    raise ValueError(
        f"Store ID column '{store_id_column}' is not unique"
    )

# COMMAND ----------

store_source.createOrReplaceTempView("azure_sql_store_source")

if not spark_session.catalog.tableExists(target_table):
    spark_session.sql(
        f"""
        CREATE TABLE {target_table}
        USING DELTA
        AS
        SELECT
            source.*,
            current_timestamp() AS _created_at,
            current_timestamp() AS _updated_at
        FROM azure_sql_store_source AS source
        WHERE 1 = 0
        """
    )

# COMMAND ----------

source_columns = store_source.columns
quoted_source_columns = [
    f"`{column.replace('`', '``')}`" for column in source_columns
]
update_assignments = ",\n            ".join(
    f"target.{column} = source.{column}"
    for column in quoted_source_columns
    if column != f"`{store_id_column.replace('`', '``')}`"
)
insert_columns = ", ".join(
    quoted_source_columns + ["`_created_at`", "`_updated_at`"]
)
insert_values = ", ".join(
    [f"source.{column}" for column in quoted_source_columns]
    + ["current_timestamp()", "current_timestamp()"]
)
quoted_store_id = f"`{store_id_column.replace('`', '``')}`"

matched_clause = ""
if update_assignments:
    matched_clause = f"""
        WHEN MATCHED THEN UPDATE SET
            {update_assignments},
            target.`_updated_at` = current_timestamp()
    """

spark_session.sql(
    f"""
    MERGE INTO {target_table} AS target
    USING azure_sql_store_source AS source
        ON target.{quoted_store_id} <=> source.{quoted_store_id}
    {matched_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_columns})
    VALUES ({insert_values})
    """
)
