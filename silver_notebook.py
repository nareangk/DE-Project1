# Databricks notebook source
# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

df = spark.read.format('parquet')\
                .option('inferSchema', 'true')\
                .load('abfss://bronze@gkncarsdatalake.dfs.core.windows.net/rawdata')

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformation

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = df.withColumn('model_category', split(col('Model_ID'), '-')[0])

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('Units_Sold',col('Units_Sold').cast('string')).display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df_rev = df.withColumn('Revperunit',col('Revenue')/col('Units_Sold'))

# COMMAND ----------

df.display()

# COMMAND ----------

df.groupBy('Year', 'BranchName') \
  .agg(sum('Units_Sold').alias('Total_Units_Sold')) \
  .sort('Year', 'Total_Units_Sold', ascending=[True, False]) \
  .display()


# COMMAND ----------

# MAGIC %md
# MAGIC # Data Writing

# COMMAND ----------

df_rev.write.format('parquet')\
    .mode('append')\
    .option('path','abfss://silver@gkncarsdatalake.dfs.core.windows.net/carsales')\
    .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@gkncarsdatalake.dfs.core.windows.net/carsales`;

# COMMAND ----------

