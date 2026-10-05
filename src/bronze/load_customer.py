# Databricks notebook source
# MAGIC %md
# MAGIC # Load customers from Azure SQL
# MAGIC
# MAGIC Reads the current customer dataset from Azure SQL and incrementally merges
# MAGIC new and changed rows into `retail_demo.bronze.customer`.

# COMMAND ----------

from typing import Any


spark_session: Any = globals()["spark"]
dbutils_runtime: Any = globals()["dbutils"]

target_table = "retail_demo.bronze.customer"

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
dbutils_runtime.widgets.text("source_table", "dbo.testtable", "Source table")
dbutils_runtime.widgets.text(
    "customer_id_column",
    "Id",
    "Customer ID column",
)
dbutils_runtime.widgets.text("secret_scope", "", "Databricks secret scope")
dbutils_runtime.widgets.text(
    "username_secret_key",
    "azure-sql-username",
    "Username secret key",
)
dbutils_runtime.widgets.text(
    "password_secret_key",
    "azure-sql-password",
    "Password secret key",
)

sql_server = dbutils_runtime.widgets.get("sql_server").strip()
sql_database = dbutils_runtime.widgets.get("sql_database").strip()
source_table = dbutils_runtime.widgets.get("source_table").strip()
customer_id_column = (
    dbutils_runtime.widgets.get("customer_id_column").strip()
)
secret_scope = dbutils_runtime.widgets.get("secret_scope").strip()
username_secret_key = (
    dbutils_runtime.widgets.get("username_secret_key").strip()
)
password_secret_key = (
    dbutils_runtime.widgets.get("password_secret_key").strip()
)

required_values = {
    "sql_server": sql_server,
    "sql_database": sql_database,
    "source_table": source_table,
    "customer_id_column": customer_id_column,
    "secret_scope": secret_scope,
    "username_secret_key": username_secret_key,
    "password_secret_key": password_secret_key,
}
missing_values = [
    name for name, value in required_values.items() if not value
]
if missing_values:
    raise ValueError(
        "Missing required parameters: " + ", ".join(missing_values)
    )

# COMMAND ----------

sql_username = dbutils_runtime.secrets.get(
    scope=secret_scope,
    key=username_secret_key,
)
sql_password = dbutils_runtime.secrets.get(
    scope=secret_scope,
    key=password_secret_key,
)

jdbc_url = (
    f"jdbc:sqlserver://{sql_server}:1433;"
    f"database={sql_database};"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;"
    "loginTimeout=30;"
)

customer_source = (
    spark_session.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", source_table)
    .option("user", sql_username)
    .option("password", sql_password)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()
)

if customer_id_column not in customer_source.columns:
    raise ValueError(
        f"Customer ID column '{customer_id_column}' does not exist in "
        f"Azure SQL table '{source_table}'"
    )

if customer_source.filter(
    customer_source[customer_id_column].isNull()
).limit(1).count():
    raise ValueError(
        f"Customer ID column '{customer_id_column}' contains null values"
    )

if customer_source.groupBy(customer_id_column).count().filter(
    "count > 1"
).limit(1).count():
    raise ValueError(
        f"Customer ID column '{customer_id_column}' is not unique"
    )

# COMMAND ----------

customer_source.createOrReplaceTempView("azure_sql_customer_source")

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
        FROM azure_sql_customer_source AS source
        WHERE 1 = 0
        """
    )

# COMMAND ----------

source_columns = customer_source.columns
quoted_source_columns = [
    f"`{column.replace('`', '``')}`" for column in source_columns
]
update_assignments = ",\n            ".join(
    f"target.{column} = source.{column}"
    for column in quoted_source_columns
    if column != f"`{customer_id_column.replace('`', '``')}`"
)
insert_columns = ", ".join(
    quoted_source_columns + ["`_created_at`", "`_updated_at`"]
)
insert_values = ", ".join(
    [f"source.{column}" for column in quoted_source_columns]
    + ["current_timestamp()", "current_timestamp()"]
)
quoted_customer_id = f"`{customer_id_column.replace('`', '``')}`"

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
    USING azure_sql_customer_source AS source
        ON target.{quoted_customer_id} <=> source.{quoted_customer_id}
    {matched_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_columns})
    VALUES ({insert_values})
    """
)
