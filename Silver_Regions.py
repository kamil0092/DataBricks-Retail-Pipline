# Databricks notebook source
# MAGIC %md 
# MAGIC ### **Read the Deleta Table Using Spark**

# COMMAND ----------

df = spark.read.table('databricks_catalog.bronze.regions')

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.drop('_rescued_data')

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Write Data**

# COMMAND ----------

# df.write.format("delta").save("abfss://silver@databricks2003sa.dfs.core.windows.net/regions")
df.write.mode("overwrite").save("abfss://silver@databricks2003sa.dfs.core.windows.net/regions")

# COMMAND ----------

# df = spark.read.format("delta").load("abfss://silver@databricks2003sa.dfs.core.windows.net/orders")
# display(df)

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catalog.silver.regions
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@databricks2003sa.dfs.core.windows.net/regions"

# COMMAND ----------

