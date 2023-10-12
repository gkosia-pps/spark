# Databricks notebook source
# MAGIC %md #### Create small files that will be used to simulate a stream of data

# COMMAND ----------

from pyspark.sql.functions import date_trunc, date_format
df_sales = spark.read.csv(path='/mnt/raw_zone/jaffle-shop/raw_orders.csv', header='true')
df_sales = df_sales.withColumn("order_time", date_format(date_trunc('MINUTE', "ordered_at"), 'yyyymmdd_hhMM'))
display(df_sales.head(3))


num_of_wows_to_export = 30

df_sales.limit(num_of_wows_to_export).write.partitionBy("order_time").mode("overwrite").json('/mnt/raw_zone/jaffle-shop/raw_orders/')

for folder in dbutils.fs.ls('/mnt/raw_zone/jaffle-shop/raw_orders/'):
    for fl in dbutils.fs.ls(folder.path):
        if fl.name.startswith('part'):
            dbutils.fs.mv(fl.path, '/mnt/raw_zone/jaffle-shop/raw_orders/')

for folder in dbutils.fs.ls('/mnt/raw_zone/jaffle-shop/raw_orders/'):
    if folder.name.startswith('order_time'):
        for fl in dbutils.fs.ls(folder.path):
            dbutils.fs.rm(fl.path)    
        dbutils.fs.rm(folder.path)

# COMMAND ----------

# MAGIC %md ##### Copy files one by one with delay to simulate incoming realtime data

# COMMAND ----------

import time

try:
    dbutils.fs.rm('/mnt/raw_zone/jaffle-shop/live-orders-source/',true)
except:
    pass


for fl in dbutils.fs.ls('/mnt/raw_zone/jaffle-shop/raw_orders/'):
    dbutils.fs.cp(fl.path, '/mnt/raw_zone/jaffle-shop/live-orders-source/' + fl.name)
    time.sleep(2)

# COMMAND ----------


