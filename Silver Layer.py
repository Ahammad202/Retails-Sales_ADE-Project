# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC Latest Data fetching for Products :

# COMMAND ----------

df_products=spark.read.format("delta").load("abfss://retailcustomer@teststorageacc786.dfs.core.windows.net/bronze/dbo.products")


# COMMAND ----------

#Filtering the latest record:
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,col

w=Window.partitionBy("Product_id").orderBy(col("LastModifiedOn").desc())

df_products=df_products.withColumn("rownum",row_number().over(w)).filter(col("rownum")==1).drop(col("rownum"))

df_products.show()

df_products.createOrReplaceTempView("products_src")#TempTblCreation

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO ade_project01.schema01.products_silver AS tgt
# MAGIC USING products_src AS src
# MAGIC ON tgt.Product_id = src.Product_id
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC        NOT tgt.Product_name <=> src.Product_name
# MAGIC     OR NOT tgt.Category <=> src.Category
# MAGIC     OR NOT tgt.Price <=> src.Price
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC     tgt.Product_name   = src.Product_name,
# MAGIC     tgt.Category       = src.Category,
# MAGIC     tgt.Price          = src.Price,
# MAGIC     tgt.LastModifiedOn = src.LastModifiedOn
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (
# MAGIC     Product_id,
# MAGIC     Product_name,
# MAGIC     Category,
# MAGIC     Price,
# MAGIC     LastModifiedOn
# MAGIC )
# MAGIC VALUES (
# MAGIC     src.Product_id,
# MAGIC     src.Product_name,
# MAGIC     src.Category,
# MAGIC     src.Price,
# MAGIC     src.LastModifiedOn
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC %md
# MAGIC Latest Data fetching for stores :

# COMMAND ----------

df_stores=spark.read.format("delta").load("abfss://retailcustomer@teststorageacc786.dfs.core.windows.net/bronze/dbo.stores")

# COMMAND ----------

#Filtering the latest record:
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,col

w=Window.partitionBy("Store_id").orderBy(col("LastModifiedOn").desc())

df_stores=df_stores.withColumn("rownum",row_number().over(w)).filter(col("rownum")==1).drop(col("rownum"))


df_stores.createOrReplaceTempView("stores_src")#TempTblCreation

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO ade_project01.schema01.stores_silver AS tgt
# MAGIC USING stores_src AS src
# MAGIC ON tgt.Store_id = src.Store_id
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC         NOT tgt.Store_name <=> src.Store_name
# MAGIC     OR NOT tgt.Location <=> src.Location
# MAGIC
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC     tgt.Store_name   = src.Store_name,
# MAGIC     tgt.Location       = src.Location,
# MAGIC     tgt.LastModifiedOn = src.LastModifiedOn
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (
# MAGIC     Store_id,
# MAGIC     Store_name,
# MAGIC     Location,
# MAGIC     LastModifiedOn
# MAGIC )
# MAGIC VALUES (
# MAGIC     src.Store_id,
# MAGIC     src.Store_name,
# MAGIC     src.Location,
# MAGIC     src.LastModifiedOn
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC Latest Data fetching for transactions :

# COMMAND ----------

df_transactions=spark.read.format("delta").load("abfss://retailcustomer@teststorageacc786.dfs.core.windows.net/bronze/dbo.transactions")


# COMMAND ----------

df_transactions.write.format("delta").mode("append").save("abfss://retailcustomer@teststorageacc786.dfs.core.windows.net/silver/dbo.transactions")

# COMMAND ----------

df_transactions.columns

# COMMAND ----------

# MAGIC %md
# MAGIC Latest Data fetching for customers :

# COMMAND ----------

df_customers=spark.read.format("delta").load("abfss://retailcustomer@teststorageacc786.dfs.core.windows.net/bronze/dbo.customers")


# COMMAND ----------

df_customers.columns

# COMMAND ----------

#Filtering the latest record:
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,col

w=Window.partitionBy("Customer_id").orderBy(col("LastModifiedOn").desc())

df_customers=df_customers.withColumn("rownum",row_number().over(w)).filter(col("rownum")==1).drop(col("rownum"))


df_customers.createOrReplaceTempView("customers_src")#TempTblCreation

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO ade_project01.schema01.customers_silver AS tgt
# MAGIC USING customers_src AS src
# MAGIC ON tgt.Customer_id = src.Customer_id
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC         NOT tgt.First_name <=> src.First_name
# MAGIC     OR NOT tgt.Last_name <=> src.Last_name
# MAGIC     OR NOT tgt.Email <=> src.Email
# MAGIC     OR NOT tgt.Phone <=> src.Phone
# MAGIC     OR NOT tgt.City <=> src.City
# MAGIC     OR NOT tgt.Registration_date <=> src.Registration_date
# MAGIC     OR NOT tgt.LastModifiedOn <=> src.LastModifiedOn
# MAGIC
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC     tgt.First_name   = src.First_name,
# MAGIC     tgt.Last_name = src.Last_name,
# MAGIC     tgt.Email = src.Email,
# MAGIC     tgt.Phone = src.Phone,
# MAGIC     tgt.City = src.City,
# MAGIC     tgt.Registration_date = src.Registration_date,
# MAGIC     tgt.LastModifiedOn = src.LastModifiedOn
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (
# MAGIC     Customer_id,
# MAGIC     First_name,
# MAGIC     Last_name,
# MAGIC     Email,
# MAGIC     Phone,
# MAGIC     City,
# MAGIC     Registration_date,
# MAGIC     LastModifiedOn
# MAGIC )
# MAGIC VALUES (
# MAGIC     src.Customer_id,
# MAGIC     src.First_name,
# MAGIC     src.Last_name,
# MAGIC     src.Email,
# MAGIC     src.Phone,
# MAGIC     src.City,
# MAGIC     src.Registration_date,
# MAGIC     src.LastModifiedOn
# MAGIC );