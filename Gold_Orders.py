# Databricks notebook source
# MAGIC %md
# MAGIC ### **Fact **

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Data Reading

# COMMAND ----------

df = spark.sql("Select * from databricks_catalog.silver.orders")

# COMMAND ----------

df_dimcus = spark.sql("select DimCustomerKey, customer_id as dim_customer_id  from databricks_catalog.gold.dimcustomers")

df_dimpro = spark.sql("select product_id as  DimProductKey, product_id as dim_product_id from databricks_catalog.gold.dimproducts")
display(df_dimpro.limit(5))

# COMMAND ----------

df_join = df.join(df_dimcus, df.customer_id == df_dimcus.dim_customer_id, 'left').join(df_dimpro, df.product_id == df_dimpro.dim_product_id, 'left')

df_join_new = df_join.drop('dim_customer_id', 'dim_product_id', 'customer_id', 'product_id')

# COMMAND ----------

display(df_join_new)

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

if spark.catalog.tableExists("databricks_catalog.gold.FactOrders"):
    dlt_obj = DeltaTable.forName(spark, "databricks_catalog.gold.FactOrders")
    dlt_obj.alias("trg").merge(
        df_join_new.alias("src"), "trg.order_id = src.order_id AND trg.DimCustomerKey = src.DimCustomerKey AND trg.DimProductKey = src.DimProductKey" )

else:
    df_join_new.write.format("delta").saveAsTable("databricks_catalog.gold.FactOrders")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from databricks_catalog.gold.FactOrders

# COMMAND ----------

