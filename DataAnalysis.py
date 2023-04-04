# Databricks notebook source
# MAGIC %sh
# MAGIC mkdir -p /dbfs/GDELT

# COMMAND ----------

# MAGIC %pip install gdelt

# COMMAND ----------

import gdelt

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gdelt_events location '/tmp/';

# COMMAND ----------

# Extracting 5 days of events data from GDELT2.0 using gdeltpyr package. The output is in pandas dataframe.
gd = gdelt.gdelt(version=2)
events = gd.Search(date=['2022 01 Sep','2022 05 Sep'],table='events',coverage=True,output='pd',normcols=True)
print("=>Succeeded")

# COMMAND ----------

# Creating a spark dataframe for faster processing
sdf_events_bronze=spark.createDataFrame(events)

# COMMAND ----------

# creating a table for permanent use, so that data is not lost every time the cluster is turned off.
sdf_events_bronze.write.saveAsTable("events_bronze")

# COMMAND ----------

spark.sql("select * from events_bronze").display()
