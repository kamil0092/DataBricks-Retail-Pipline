# Databricks notebook source
# MAGIC %md 
# MAGIC ### **Dynamic Capabilities**

# COMMAND ----------

dbutils.widgets.text("file_name", "")

# COMMAND ----------

p_file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Data Reading**

# COMMAND ----------

# df = spark.read.format('parquet').load(f"abfss://bronze@databricks2003sa.dfs.core.windows.net/{p_file_name}")
# df.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Spark Structure Streaming**

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Read Data**

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", f"abfss://bronze@databricks2003sa.dfs.core.windows.net/checkpoint_{p_file_name}")\
    .load(f"abfss://sourse@databricks2003sa.dfs.core.windows.net/{p_file_name}")

# COMMAND ----------

# df.display(2)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Write Data**
# MAGIC ### with Rocks DB it's only read the new Data

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Write the Data**

# COMMAND ----------

df.writeStream.format("parquet")\
    .outputMode("append")\
    .option("checkpointLocation", f"abfss://bronze@databricks2003sa.dfs.core.windows.net/checkpoint_{p_file_name}")\
    .option("path", f"abfss://bronze@databricks2003sa.dfs.core.windows.net/{p_file_name}")\
    .trigger(once=True)\
    .start()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

