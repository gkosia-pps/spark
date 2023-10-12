# Databricks notebook source
# MAGIC %md #### Check the files of raw_zone

# COMMAND ----------

display(dbutils.fs.ls('/mnt/raw_zone/jaffle-shop/'))

# COMMAND ----------

# MAGIC %md #### Load data into dataframe by infering schema

# COMMAND ----------

df_customers = spark.read.format('csv').option('header', 'true').load('dbfs:/mnt/raw_zone/jaffle-shop/raw_customers.csv')
display(df_customers.show(3))

# COMMAND ----------

# MAGIC %md #### Load data by specifiyng the schema

# COMMAND ----------

sales_schema = "_c0 int, id string, customer string, ordered_at timestamp, strore_id string, subtotal double, tax_paid double, order_total double"

df_sales = spark.read.csv(path='/mnt/raw_zone/jaffle-shop/raw_orders.csv', header='true', schema=sales_schema)
display(df_sales.show(3))

# COMMAND ----------

# MAGIC %md #### Load data by specifiyng the schema as struct

# COMMAND ----------

# dbutils.fs.head('dbfs:/mnt/raw_zone/jaffle-shop/raw_stores.csv')
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType

stores_schema = StructType([
     StructField("_c0", IntegerType(), False)
    ,StructField("id", StringType(), False)
    ,StructField("name", StringType(), False)
    ,StructField("opened_at", TimestampType(), False)
    ,StructField("tax_rate", DoubleType(), False)
])

df_stores = spark.read.format('csv').schema(stores_schema).option('header', 'true').load('dbfs:/mnt/raw_zone/jaffle-shop/raw_stores.csv')

# display(df_stores.dtypes)
display(df_stores.head(3))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------





