# Databricks notebook source
# MAGIC %sql
# MAGIC -- select * from ade_project01.schema01.products_silver
# MAGIC -- select * from ade_project01.schema01.stores_silver
# MAGIC -- select * from ade_project01.schema01.customers_silver
# MAGIC -- select * from ade_project01.schema01.transactions_silver
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ade_project01.schema01.gold_sales_summary
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://retailcustomer@teststorageacc786.dfs.core.windows.net/gold/sales_summary'
# MAGIC AS
# MAGIC
# MAGIC select sum(Quantity) as TotalQuantitySold,sum(price) as TotalRevenue,count(Transaction_id) as Transactions,avg(Price) as AvgPricePerProduct
# MAGIC from ade_project01.schema01.products_silver P 
# MAGIC inner join ade_project01.schema01.transactions_silver t on P.Product_id=t.Product_id 
# MAGIC inner join ade_project01.schema01.customers_silver C on t.Customer_id=C.Customer_id
# MAGIC inner join ade_project01.schema01.stores_silver S on t.Store_id=S.Store_id
# MAGIC group by Transaction_date,p.Product_id,Product_name,Category,S.Store_id,S.Store_name,S.Location

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ade_project01.schema01.gold_category_summary
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://retailcustomer@teststorageacc786.dfs.core.windows.net/gold/category_summary'
# MAGIC AS
# MAGIC
# MAGIC select Category,count(Product_name) as Product_Count,sum(price) as TotalRevenue
# MAGIC from ade_project01.schema01.products_silver
# MAGIC group by Category

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ade_project01.schema01.gold_product_sales
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://retailcustomer@teststorageacc786.dfs.core.windows.net/gold/product_sales'
# MAGIC AS
# MAGIC
# MAGIC select Product_name,Category,price,LastModifiedOn from ade_project01.schema01.products_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ade_project01.schema01.gold_store_performance
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://retailcustomer@teststorageacc786.dfs.core.windows.net/gold/store_performance'
# MAGIC AS
# MAGIC
# MAGIC select Store_name,Location,LastModifiedOn from ade_project01.schema01.stores_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ade_project01.schema01.gold_citywise_customers
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://retailcustomer@teststorageacc786.dfs.core.windows.net/gold/citywise_customers'
# MAGIC AS
# MAGIC
# MAGIC select City,count(Email) as Customers
# MAGIC from ade_project01.schema01.customers_silver
# MAGIC group by City
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Product_id	Product_name	Category	Price	LastModifiedOn	Store_id	Store_name	Location	LastModifiedOn	Transaction_id	Customer_id	Product_id	Store_id	Quantity	Transaction_date	LastModifiedOn	Customer_id	First_name	Last_name	Email	Phone	City	Registration_date	LastModifiedOn

# COMMAND ----------

# MAGIC %md
# MAGIC