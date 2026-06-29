# Databricks notebook source
df_products = spark.read.parquet("abfss://retailcustomer@teststorageacc786.dfs.core.windows.net/source/dbo.products.parquet")

# COMMAND ----------

df_store=spark.read.format("parquet").option("header","true").load("abfss://retailcustomer@teststorageacc786.dfs.core.windows.net/source/dbo.stores.parquet")

# COMMAND ----------

df_transactions=spark.read.format("parquet").option("header","true").load("abfss://retailcustomer@teststorageacc786.dfs.core.windows.net/source/dbo.transactions.parquet")

# COMMAND ----------

df_customers=spark.read.format("parquet").option("header","true").load("abfss://retailcustomer@teststorageacc786.dfs.core.windows.net/source/customers.parquet")

# COMMAND ----------

from pyspark.sql.functions import initcap,col,current_timestamp,ltrim,rtrim,coalesce
df_store=df_store.select(col("store_id").alias("Store_id"),ltrim(rtrim(initcap(col("store_name")))).alias("Store_name"),
                         ltrim(rtrim(initcap(col("location")))).alias("Location")).withColumn("LastModifiedOn",current_timestamp())

# COMMAND ----------



df_products=df_products.select(ltrim(rtrim(col("product_id"))).alias("Product_id"),ltrim(rtrim(initcap(col("product_name"))))\
                               .alias("Product_name"),ltrim(rtrim(initcap(col("category")))).alias("Category"),col("price")\
                               .alias("Price")).withColumn("LastModifiedOn",current_timestamp())

# COMMAND ----------

df_transactions=df_transactions.select(col("transaction_id").alias("Transaction_id"),ltrim(rtrim(initcap(col("customer_id")))).alias("Customer_id"),ltrim(rtrim(initcap(col("product_id")))).alias("Product_id"),ltrim(rtrim(initcap(col("store_id")))).alias("Store_id"),ltrim(rtrim(initcap(col("quantity")))).alias("Quantity"),ltrim(rtrim(initcap(col("transaction_date")))).alias("Transaction_date")).withColumn("LastModifiedOn",current_timestamp())

# COMMAND ----------


df_customers=df_customers.select(col("customer_id").alias("Customer_id"),ltrim(rtrim(initcap(col("first_name")))).alias("First_name"),ltrim(rtrim(initcap(col("last_name")))).alias("Last_name"),initcap(col("email")).alias("Email"),initcap(col("phone")).alias("Phone"),ltrim(rtrim(initcap(col("city")))).alias("City"),ltrim(rtrim(initcap(col("registration_date")))).alias("Registration_date")).withColumn("LastModifiedOn",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC Null val handling :

# COMMAND ----------

df_products = df_products.withColumn(
    "Price",
    col("Price").cast("double")
)


# COMMAND ----------

from pyspark.sql.functions import initcap,col,current_timestamp,ltrim,rtrim,coalesce,lit,col
df_store=df_store.withColumn("Store_name",coalesce(col("Store_name"),lit("UNKNOWN")))\
                        .withColumn("Location",coalesce(col("Location"),lit("UNKNOWN")))



# COMMAND ----------

from pyspark.sql.functions import coalesce,lit,col


df_products=df_products.withColumn("Category",coalesce(col("Category"),lit("UNKNOWN")))\
                        .withColumn("Product_name",coalesce(col("Product_name"),lit("UNKNOWN")))                               


# COMMAND ----------

df_transactions=df_transactions.filter(col("Customer_id").isNotNull() & col("Product_id").isNotNull() & col("Store_id").isNotNull() &
                                       col("Transaction_date").isNotNull())
df_transactions=df_transactions.filter(col("Customer_id").isNotNull() & col("Product_id").isNotNull() & col("Store_id").isNotNull() &
                                       col("Transaction_date").isNotNull())


# COMMAND ----------

df_customers=df_customers.filter(col("First_name").isNotNull() & col("Last_name").isNotNull() & col("Email").isNotNull() &
                                       col("Phone").isNotNull() & col("City").isNotNull() & col("Registration_date").isNotNull())


# COMMAND ----------

# MAGIC %md
# MAGIC -ve val rows removal

# COMMAND ----------

df_products=df_products.filter(col("Price")>=0)


# COMMAND ----------

df_transactions=df_transactions.filter(col("Quantity")>0) .filter( col("Transaction_date")<lit("2026-06-18"))



# COMMAND ----------


df_transactions=df_transactions.dropDuplicates(["Customer_id","Product_id","Store_id","Quantity","Transaction_date"])


# COMMAND ----------


df_customers=df_customers.dropDuplicates(["First_name","Last_name","Email","Phone","City","Registration_date"])


# COMMAND ----------

# MAGIC %md
# MAGIC # Pushing to Silver Layer as a Delta TBL :

# COMMAND ----------

df_products.write.format("delta").mode("append").save("abfss://retailcustomer@teststorageacc786.dfs.core.windows.net/bronze/dbo.products")

# COMMAND ----------

df_store.write.format("delta").mode("append").save("abfss://retailcustomer@teststorageacc786.dfs.core.windows.net/bronze/dbo.stores")

# COMMAND ----------

df_transactions.write.format("delta").mode("append").save("abfss://retailcustomer@teststorageacc786.dfs.core.windows.net/bronze/dbo.transactions")

# COMMAND ----------

df_customers.write.format("delta").mode("append").save("abfss://retailcustomer@teststorageacc786.dfs.core.windows.net/bronze/dbo.customers")

# COMMAND ----------

# MAGIC %md
# MAGIC Silver Layer data insertion :

# COMMAND ----------

# df_products.write.format("delta").mode("append").save("abfss://retailcustomer@teststorageacc786.dfs.core.windows.net/silver/dbo.products")

# COMMAND ----------

# df_store.write.format("delta").mode("append").save("abfss://retailcustomer@teststorageacc786.dfs.core.windows.net/silver/dbo.stores")

# COMMAND ----------

# df_transactions.write.format("delta").mode("append").save("abfss://retailcustomer@teststorageacc786.dfs.core.windows.net/silver/dbo.transactions")

# COMMAND ----------

# df_customers.write.format("delta").mode("append").save("abfss://retailcustomer@teststorageacc786.dfs.core.windows.net/silver/dbo.customers")

# COMMAND ----------

# MAGIC %md
# MAGIC External TBL Creation for bronze and silver:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create table ade_project01.schema01.customers_silver
# MAGIC -- using delta
# MAGIC -- location 'abfss://retailcustomer@teststorageacc786.dfs.core.windows.net/silver/dbo.customers'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create table ade_project01.schema01.transactions_silver
# MAGIC -- using delta
# MAGIC -- location 'abfss://retailcustomer@teststorageacc786.dfs.core.windows.net/silver/dbo.transactions'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from ade_project01.schema01.project_bronze
# MAGIC -- select * from ade_project01.schema01.stores_silver