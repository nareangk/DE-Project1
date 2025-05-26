# Databricks notebook source
# MAGIC %md
# MAGIC # CREATE FACT TABLE
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ***Reading Silver Data***

# COMMAND ----------


df_silver = spark.sql("select * from parquet.`abfss://silver@gkncarsdatalake.dfs.core.windows.net/carsales`")

# COMMAND ----------

# MAGIC %md
# MAGIC ***Reading all the dims***

# COMMAND ----------

df_model = spark.sql("select * from cars_catalog.gold.dim_model")
df_branch = spark.sql("select * from cars_catalog.gold.dim_branch")


# COMMAND ----------

# MAGIC %md
# MAGIC ***bringing keys to fact table***

# COMMAND ----------

df_fact = df_silver.join(df_branch, df_silver.Branch_ID == df_branch.branch_id,how='left')\
                   .join(df_model, df_silver.Model_ID == df_model.model_id,how='left')\
                   .select(df_silver['Revenue'],df_silver['Units_Sold'],df_silver['revperunit'],df_branch['dim_branch_key'],df_model['dim_model_key'])
                   
display(df_fact)


# COMMAND ----------

# MAGIC %md
# MAGIC # Writing Fact Table

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("factsales"):
    deltatable = deltaTable.forname(spark,'cars_catalog.gold.factsales')
    deltable.alias('target').merge(df_fact.alias('source'),'target.dim_branch_key = source.dim_branch_key and target. dim_model_key = source.dim_model_key')\
                           .whenMatchedUpdateAll()\
                           .whenNotMatchedInsertAll()\
                           .execute()

else:
    df_fact.write.format('delta')\
        .mode('append')\
        .option('path','abfss://silver@gkncarsdatalake.dfs.core.windows.net/factsales')\
        .saveAsTable('cars_catalog.gold.factsales')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.factsales;

# COMMAND ----------

