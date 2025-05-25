# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Pipline

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from dlt import expect_all_or_drop

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Streaming Table 

# COMMAND ----------

# Expectation
my_rules = {
    "rule1": "product_id IS NOT NULL",
    "rule2": "product_name IS NOT NULL"
}

# COMMAND ----------

@dlt.table
@expect_all_or_drop(my_rules)
def DimProducts_stage():
    df = spark.readStream.table('databricks_catalog.silver.products')
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming View

# COMMAND ----------

@dlt.view
def DimProducts_View():
    
    df = spark.readStream.table("Live.DimProducts_stage")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim Products

# COMMAND ----------

dlt.create_streaming_table("DimProducts")

# COMMAND ----------

dlt.apply_changes(
    target = "DimProducts",
    source = "DimProducts_View",
    keys = ["product_id"],
    sequence_by="product_id",
    stored_as_scd_type=2
)

# COMMAND ----------

