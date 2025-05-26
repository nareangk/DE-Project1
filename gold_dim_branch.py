# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Flag parameter

# COMMAND ----------

dbutils.widgets.text('incrementatl_flag','0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incrementatl_flag')
print(incremental_flag)

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating dimension model

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@gkncarsdatalake.dfs.core.windows.net/carsales`

# COMMAND ----------

df_src = spark.sql('''
                   select distinct branch_id, BranchName
                   from parquet.`abfss://silver@gkncarsdatalake.dfs.core.windows.net/carsales`
                   ''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Dim_model sink - Initial and Incremental

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_branch'):
    df_sink = spark.sql('''
                    select dim_branch_key, branch_id, branchName 
                    from cars_catalog.gold.dim_branch
                    ''')


else:
    df_sink = spark.sql('''
                    select 1 as dim_branch_key, branch_id, branchName 
                    from parquet.`abfss://silver@gkncarsdatalake.dfs.core.windows.net/carsales`
                    where 1=0
                    ''')

# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Filtering old records and new records

# COMMAND ----------

df_filter = df_src.join(df_sink,df_src['branch_id'] == df_sink['branch_id'],'left').select(df_src['branch_id'],df_src['branchName'],df_sink['dim_branch_key'])

# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ***df_filter_old***

# COMMAND ----------

df_filter_old = df_filter.filter(col('dim_branch_key').isNotNull())
df_filter_old.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ***df_filter_new***

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_branch_key').isNull()).select(df_filter['branch_id'],df_filter['branchName'])
df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # create surrogate key

# COMMAND ----------

# MAGIC %md
# MAGIC ***fetch max surrogate key from existing table***

# COMMAND ----------

if incremental_flag == '0':
    max_value = 1
else:
    max_value_df = spark.sql("select max(dim_branch_key) from cars_catalog.gold.dim_branch")
    max_value = max_value_df.collect()[0][0]



# COMMAND ----------

# MAGIC %md
# MAGIC ***create surrogate key column and add max value***

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_branch_key',max_value+monotonically_increasing_id())
df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Creat Final df - df_filter_old + df_filter_new

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD - Type 1(UPSERT)

# COMMAND ----------

# Incremental load
if spark.catalog.tableExists('cars_catalog.gold.dim_branch'):
    delta_tbl = DeltaTable.forPath(spark, 'abfss://gold@gkncarsdatalake.dfs.core.windows.net/dim_branch')
    delta_tbl.alias('target').merge(df_final.alias('source'),'target.branch_id = source.branch_id')\
                                    .whenMatchedUpdateAll()\
                                    .whenNotMatchedInsertAll()\
                                    .execute()


else:
    df_final.write.format('delta')\
                .mode('overwrite')\
                .option('path','abfss://gold@gkncarsdatalake.dfs.core.windows.net/dim_branch')\
                .saveAsTable("cars_catalog.gold.dim_branch")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_branch;