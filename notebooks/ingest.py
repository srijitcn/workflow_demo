# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("landing_location","")
dbutils.widgets.text("file_master_id","")
dbutils.widgets.text("catalog_name","")
dbutils.widgets.text("schema_name","")
dbutils.widgets.text("table_name","")

# COMMAND ----------

landing_location = dbutils.widgets.get("landing_location")
file_master_id = dbutils.widgets.get("file_master_id")
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
table_name = dbutils.widgets.get("table_name")

if (landing_location == "" 
    or file_master_id == ""
    or catalog_name == ""
    or schema_name == ""
    or table_name == ""):
    raise Exception("Mandatory input parameters not set")

# COMMAND ----------

data_file_path = f"{landing_location}/{file_master_id}"

(spark
 .read
 .parquet(data_file_path)
 .write
 .format("delta")
 .mode("append")
 .saveAsTable(f"{catalog_name}.{schema_name}.{table_name}")
)

# COMMAND ----------


