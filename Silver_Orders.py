# Databricks notebook source
df = spark.read.format("parquet").load("abfss://bronze@databricks2003sa.dfs.core.windows.net/orders")

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.withColumnRenamed("_rescued_data", "rescued_data")

# COMMAND ----------

df = df.drop("rescued_data")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

df = df.withColumn("order_date", to_timestamp(col('order_date')))
display(df.take(4))

# COMMAND ----------

df = df.withColumn("year", year(col('order_date')))
display(df.take(5))

# COMMAND ----------

df1 = df.withColumn("dense_rnk", dense_rank().over(Window.partitionBy("year").orderBy(desc('total_amount'))))
display(df1.take(5))

# COMMAND ----------

df1 = df1.withColumn("rank", rank().over(Window.partitionBy("year").orderBy(desc('total_amount'))))
display(df1.take(5))

# COMMAND ----------

df1 = df1.withColumn("row_num", row_number().over(Window.partitionBy("year").orderBy(desc('total_amount'))))
display(df1.take(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Classes OPP**

# COMMAND ----------



# COMMAND ----------

class Windows:

    def dense_rank(self, df):

        df_dense_rank = df.withColumn("dense_rank", dense_rank().over(Window.partitionBy("year").orderBy(desc('total_amount'))))

        return df_dense_rank
    
    def rank(self, df):

        df_rank = df.withColumn("rank", rank().over(Window.partitionBy("year").orderBy(desc('total_amount'))))

        return df_rank
    
    def row_number(self, df):

        df_row_number = df.withColumn("row_number", row_number().over(Window.partitionBy("year").orderBy(desc('total_amount'))))

        return df_row_number


# COMMAND ----------

df_new = df


# COMMAND ----------

obj = Windows()
df_new = obj.rank(df_new)

# COMMAND ----------

# display(df_new)

# COMMAND ----------

# display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Data Writing**
# MAGIC ##### ** In Silver layer write data into delta format **

# COMMAND ----------

df.write.format("delta").mode('overwrite').save('abfss://silver@databricks2003sa.dfs.core.windows.net/orders')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Table  

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catalog.silver.orders
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@databricks2003sa.dfs.core.windows.net/orders"

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * from databricks_catalog.silver.orders limit 5

# COMMAND ----------

