# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("parquet").load('abfss://bronze@databricks2003sa.dfs.core.windows.net/products')
display(df.limit(5))

# COMMAND ----------

df = df.drop('_rescued_data')

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Functions**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION databricks_catalog.bronze.discount_func(p_price DOUBLE)
# MAGIC   RETURNS DOUBLE
# MAGIC   RETURN p_price * 0.90

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, price, databricks_catalog.bronze.discount_func(price) discount_price from products

# COMMAND ----------

#using spark use the function 
df = df.withColumn("discount_price", expr("databricks_catalog.bronze.discount_func(price)"))
display(df.limit(5))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION databricks_catalog.bronze.upper_func(p_brand STRING)
# MAGIC   RETURNS STRING
# MAGIC   LANGUAGE PYTHON
# MAGIC   AS
# MAGIC   $$
# MAGIC   return p_brand.upper()
# MAGIC   $$

# COMMAND ----------

# MAGIC %sql 
# MAGIC select product_id, brand, databricks_catalog.bronze.upper_func(brand) brand_upper 
# MAGIC from products 
# MAGIC limit 10

# COMMAND ----------

display(df.take(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Write data**

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@databricks2003sa.dfs.core.windows.net/products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Create Table**

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catalog.silver.products
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@databricks2003sa.dfs.core.windows.net/products"

# COMMAND ----------

# %sql 
# SELECT * FROM databricks_catalog.silver.products

# COMMAND ----------

