# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

init_load_flag = dbutils.widgets.text("init_load_flag", "")
init_load_flag = int(dbutils.widgets.get("init_load_flag"))

# COMMAND ----------

df = spark.sql("select * from databricks_catalog.silver.customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Removing Duplicates

# COMMAND ----------

df = df.dropDuplicates(subset=['customer_id'])

# COMMAND ----------

# display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #  **New vs Old Records**

# COMMAND ----------

if spark.catalog.tableExists("databricks_catalog.gold.DimCustomers"):
    df_old = spark.sql(''' select DimCustomerKey, customer_id , created_date, updated_date
                    from databricks_catalog.gold.DimCustomers''')
   
else:
    df_old = spark.sql(''' select 0 DimCustomerKey, 0 customer_id ,0  created_date, 0 updated_date
                    from databricks_catalog.silver.customers where 1 = 0 ''')

# COMMAND ----------

display(df_old.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renaming Columns of Old_df

# COMMAND ----------

df_old = df_old.withColumnRenamed('DimCustomerKey', 'old_DimCustomerKey')\
                .withColumnRenamed('customer_id', 'old_customer_id')\
                .withColumnRenamed('created_date', 'old_created_date')\
                .withColumnRenamed('updated_date', 'old_updated_date')

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Apply Join With the Old Records**

# COMMAND ----------

df_join = df.join(df_old, df['customer_id'] == df_old['old_customer_id'], 'left')

# COMMAND ----------

display(df_join.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Seperating New VS Old Records

# COMMAND ----------

df_new = df_join.filter(df_join['old_DimCustomerKey'].isNull())

# COMMAND ----------

display(df_new.limit(5))

# COMMAND ----------

df_old = df_join.filter(df_join['old_DimCustomerKey'].isNotNull())

# COMMAND ----------

display(df_old.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparing for the df_old

# COMMAND ----------

#Droping all the column which are not required
df_old  = df_old.drop('old_customer_id', 'old_updated_date')

#Renaming the "old_created_col" column to "create_date"
df_old = df_old.withColumnRenamed('old_DimCustomerKey', 'DimCustomerKey')   
df_old = df_old.withColumnRenamed('old_created_date', 'created_date')
df_old = df_old.withColumn("created_date", to_timestamp(col("created_date")))

#Recreating the "updated_date" column with current timestamp

df_old = df_old.withColumn("updated_date", current_timestamp())


# COMMAND ----------

display(df_old.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparing for df_new

# COMMAND ----------

#Droping all the column which are not required
df_new  = df_new.drop('old_DimCustomerKey','old_customer_id', 'old_updated_date', 'old_created_date')

#Recreating the "updated_date", "created_date" column with current timestamp
df_new = df_new.withColumn("updated_date", current_timestamp())
df_new = df_new.withColumn("created_date", current_timestamp())

# COMMAND ----------

display(df_new.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### #  Surrogate Key - from 1

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey", monotonically_increasing_id() + lit(1))

# COMMAND ----------

if init_load_flag == 1 :
    max_surrogate_key = 0
    
else:
    df_max_surr = spark.sql("select max(DimCustomerKey) as max_surrogate_key from databricks_catalog.gold.DimCustomers")

    #Converting df_max_surr to max_surrogate_key
    max_surrogate_key = df_max_surr.collect()[0]['max_surrogate_key']

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey", lit(max_surrogate_key) + col("DimCustomerKey"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union of df_old and df_new

# COMMAND ----------

df_final = df_new.unionByName(df_old)


# COMMAND ----------

display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **SCD Type 1**

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

if spark.catalog.tableExists("databricks_catalog.gold.DimCustomers"):
    dlt_obj = DeltaTable.forPath(spark, "abfss://gold@databricks2003sa.dfs.core.windows.net/DimCustomers")

    dlt_obj.alias("trg").merge(df_final.alias("src"), "trg.customer_id = src.customer_id")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

else:
    df_final.write.format("delta")\
        .option("path", "abfss://gold@databricks2003sa.dfs.core.windows.net/DimCustomers")\
        .saveAsTable("databricks_catalog.gold.DimCustomers")
    

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from databricks_catalog.gold.DimCustomers

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

