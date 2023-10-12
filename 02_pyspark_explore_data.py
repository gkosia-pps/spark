# Databricks notebook source
# MAGIC %md #### Run the 01_pyspark_load_data to load the data in df_customers and df_sales

# COMMAND ----------

# MAGIC %run ./01_pyspark_load_data

# COMMAND ----------

# MAGIC %md #### Explore df_sales dataframe schema

# COMMAND ----------

# get the schema of the dataframe
display(df_sales.printSchema())

# get the definition of the schema as Struct
display(df_sales.schema)

# get the datatype of each column
display(df_sales.dtypes)

# COMMAND ----------

# MAGIC %md #### Get an overview of the values based on statistics

# COMMAND ----------

df_stores.describe().show()

# COMMAND ----------

# MAGIC %md #### Get null values by column

# COMMAND ----------

from pyspark.sql.functions import when, col, count

display(df_sales.select([count(when(col(c).isNull(),c)).alias(c) for c in df_sales.columns]))
