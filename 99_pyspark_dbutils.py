# Databricks notebook source
# MAGIC %md #### Use %fs to run native filesystem commands 

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls

# COMMAND ----------

# MAGIC %md #### Can use dbutils from pyspark 

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

# MAGIC %md #### To refer simply to a container we can mount it to dbfs

# COMMAND ----------

dbutils.fs.mount(
 source="wasbs://datasets@staccadlprod.blob.core.windows.net"
,mount_point="/mnt/raw_zone"
,extra_configs={"fs.azure.account.key.staccadlprod.blob.core.windows.net": "account key"}
)

display(dbutils.fs.ls('/mnt/raw_zone'))

# COMMAND ----------


