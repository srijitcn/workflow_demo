# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("src_data_location","s3://databricks-e2demofieldengwest/external_location_srijit_nair/gilead/workflow_demo/src")
dbutils.widgets.text("dest_data_location","s3://databricks-e2demofieldengwest/external_location_srijit_nair/gilead/workflow_demo/landing")
dbutils.widgets.text("file_master_id","")

# COMMAND ----------

src_data_location = dbutils.widgets.get("src_data_location")
dest_data_location = dbutils.widgets.get("dest_data_location")
file_master_id = dbutils.widgets.get("file_master_id")

if (src_data_location == "" 
    or dest_data_location == ""
    or file_master_id == ""):
    raise Exception("Mandatory input parameters not set")

# COMMAND ----------

src_data_path = f"{src_data_location}/{file_master_id}"
dest_data_path = f"{dest_data_location}/{file_master_id}"

(spark
  .read
  .option("header","true")
  .csv(src_data_path)
  .write
  .mode("overwrite")
  .parquet(dest_data_path)
)

# COMMAND ----------


