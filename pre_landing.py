# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("src_data_location","s3://databricks-e2demofieldengwest/external_location_srijit_nair/gilead/workflow_demo/src")
dbutils.widgets.text("file_master_id","")

# COMMAND ----------

src_data_location = dbutils.widgets.get("src_data_location")
file_master_id = dbutils.widgets.get("file_master_id")

if (src_data_location == "" 
    or file_master_id == ""):
    raise Exception("Mandatory input parameters not set")

# COMMAND ----------


from dbruntime.dbutils import FileInfo

#check csv file is having data
def is_valid_file(file: FileInfo) -> bool :
  if file.path.endswith(".csv"):
    if file.size > 0:
      return True
    else:
      return False
  else:
    return True
  

# COMMAND ----------

data_file_path = f"{src_data_location}/{file_master_id}"

files = dbutils.fs.ls(data_file_path)
for file in files:
  if not is_valid_file(file):
    raise Exception(f"Empty CSV file {file.path} detected")

