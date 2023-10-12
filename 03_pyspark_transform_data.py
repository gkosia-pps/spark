# Databricks notebook source
# MAGIC %run ./01_pyspark_load_data

# COMMAND ----------

# MAGIC %md #### Create calculated columns

# COMMAND ----------

# sql like expressions
df_stores = df_stores.selectExpr("id", "name", "opened_at", "tax_rate", "date(opened_at) as opened_date")
display(df_stores.head(3))

# using pyspark
from pyspark.sql.functions import year
df_stores = df_stores.withColumn("opened_year", year("opened_date"))
display(df_stores.head(3))

# COMMAND ----------

# MAGIC %md #### Fill missing values

# COMMAND ----------

# fill with the same value
df_sales = df_sales.na.fill(0, subset=["subtotal","tax_paid","order_total"])

# fill each column with different values
df_sales = df_sales.na.fill({
     "ordered_at": "2099-01-01"
    ,"order_total": -1
})

# COMMAND ----------

# MAGIC %md #### Drop missing values

# COMMAND ----------

df_sales = df_sales.na.drop(subset=["order_total"])

# COMMAND ----------

# MAGIC %md #### Filter rows

# COMMAND ----------


