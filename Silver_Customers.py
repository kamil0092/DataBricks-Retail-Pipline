# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Data Read**

# COMMAND ----------

df = spark.read.format("parquet")\
    .load("abfss://bronze@databricks2003sa.dfs.core.windows.net/customers")

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.drop('_rescued_data')
display(df)

# COMMAND ----------

df = df.withColumn('domain', split(col('email'), "@")[1])
display(df)

# COMMAND ----------

df_agg = df.groupBy('domain').agg(count("customer_id").alias('domain_count')).sort(desc('domain_count'))
display(df_agg)

# COMMAND ----------

df_gmail  = df_agg.filter(col('domain') == 'gmail.com')
display(df_gmail)

df_yahoo = df_agg.filter(col('domain') == 'yahoo.com')
display(df_yahoo)

df_hotmail = df_agg.filter(col('domain') == 'hotmail.com')
display(df_hotmail)

# COMMAND ----------

df = df.withColumn('full_name', concat(col('first_name'), lit(' '), col('last_name')))
df = df.drop('first_name', 'last_name')
display(df)


# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Data Write**

# COMMAND ----------

df.write.format("delta").mode('overwrite').save('abfss://silver@databricks2003sa.dfs.core.windows.net/customers')

# COMMAND ----------

# customer = spark.read.format("delta").load('abfss://silver@databricks2003sa.dfs.core.windows.net/customers')
# display(customer)

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catalog.silver.customers
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@databricks2003sa.dfs.core.windows.net/customers"

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT  * FROM databricks_catalog.silver.customers

# COMMAND ----------

