# Databricks notebook source
# MAGIC %md ## AdventureWorks EDA Using Spark SQL aggregate functions from Python
# MAGIC
# MAGIC ![Spark Logo](http://spark-mooc.github.io/web-assets/images/ta_Spark-logo-small.png)
# MAGIC
# MAGIC More examples are available on the Spark website: http://spark.apache.org/examples.html
# MAGIC
# MAGIC PySpark API documentation: http://spark.apache.org/docs/latest/api/python/

# COMMAND ----------

# MAGIC %md
# MAGIC ## Author: Bryan Cafferky Copyright 08/20/2021

# COMMAND ----------

# MAGIC %md ### Warning!!!
# MAGIC
# MAGIC #### To run this code, you need to have uploaded the files and created the database tables - see Lesson 9 - Creating the SQL Tables on Databricks.  Link in video description to that video.

# COMMAND ----------

# MAGIC %md ## Using Spark SQL from Python

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dataframe naming prefix convention:
# MAGIC ##### 1st character is s for Spark DF
# MAGIC ##### 2nd character is p for Python
# MAGIC ##### 3rd and 4th character is df for dataframe
# MAGIC ##### 5th = _ separator
# MAGIC ##### rest is a meaningful name
# MAGIC
# MAGIC ##### spdf_salessummary = a Spark Python dataframe containing sales summary information.

# COMMAND ----------

# DBTITLE 1,Code Cell 1 - Use Spark SQL to load a PySpark dataframe from the factinternetsales table...
spark.sql('use awproject')
spdf_salesinfo = spark.sql('select * from factinternetsales').dropna()

# COMMAND ----------

# DBTITLE 1,Code Cell 2 
display(spdf_salesinfo.head(5))

# COMMAND ----------

# MAGIC %md #### Using SQL Aggregate Functions via agg()
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.agg.html

# COMMAND ----------

# MAGIC %md ### More on using SQL Aggregate Functions
# MAGIC https://sparkbyexamples.com/spark/spark-sql-aggregate-functions/

# COMMAND ----------

# MAGIC %md ### Just using SQL directly...

# COMMAND ----------

# DBTITLE 1,Code Cell 3 - SQL Code Cell
# MAGIC %sql
# MAGIC
# MAGIC SELECT avg(SalesAmount) as avg_sales, 
# MAGIC        stddev(SalesAmount) as std_sales, 
# MAGIC        min(SalesAmount) as min_sales, 
# MAGIC        max(SalesAmount) as max_sales, 
# MAGIC        approx_count_distinct(ProductKey) as count_productkey, 
# MAGIC        approx_count_distinct(CustomerKey) as count_customerkey
# MAGIC FROM factinternetsales

# COMMAND ----------

# DBTITLE 1,Code Cell 4 - Using SQL from Python
spdf_salesagg = spark.sql('''
SELECT avg(SalesAmount) as avg_sales, 
       stddev(SalesAmount) as std_sales, 
       min(SalesAmount) as min_sales, 
       max(SalesAmount) as max_sales, 
       approx_count_distinct(ProductKey) as count_productkey, 
       approx_count_distinct(CustomerKey) as count_customerkey
FROM factinternetsales''')

display(spdf_salesagg)

# COMMAND ----------

# MAGIC %md ### Look mom, just Python...

# COMMAND ----------

# DBTITLE 1,Code Cell 5
# import some more functions
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import avg
from pyspark.sql.functions import max, min
from pyspark.sql.functions import stddev

# Perform aggregations on the DataFrame
spdf_agg = spdf_salesinfo.agg(
    avg(spdf_salesinfo.SalesAmount).alias("avg_sales"), 
    stddev("SalesAmount").alias("stddev_sales"),
    min(spdf_salesinfo.SalesAmount).alias("min_sales"),
    max(spdf_salesinfo.SalesAmount).alias("max_sales"),
    countDistinct(spdf_salesinfo.ProductKey).alias("distinct_products"), 
    countDistinct(spdf_salesinfo['CustomerKey']).alias('distinct_customers')
)

# Convert the results to Pandas DataFrame
spdf_agg.toPandas()

# COMMAND ----------

# DBTITLE 1,Code Cell 6 - Show Type
type(spdf_agg)

# COMMAND ----------

# DBTITLE 1,Code Cell 7 - groupBy()
import pyspark.sql.functions

spdf_salesinfo.groupBy("ProductKey").avg("SalesAmount").toPandas()

# COMMAND ----------

# DBTITLE 1,Code Cell 8 - Using SQL Aggregate Functions without individually importing them
#  See https://sparkbyexamples.com/spark/using-groupby-on-dataframe/

import pyspark.sql.functions

# Notice, groupBy() before the agg() functions.
spdf_salesinfo.groupBy("PromotionKey").agg(
    avg("SalesAmount").alias("avg_sales"), 
    stddev("SalesAmount").alias("stddev_sales"),
    min(spdf_salesinfo.SalesAmount).alias("min_sales"),
    max(spdf_salesinfo.SalesAmount).alias("max_sales"),
    countDistinct(spdf_salesinfo.ProductKey).alias("distinct_products"), 
    countDistinct(spdf_salesinfo['CustomerKey']).alias('distinct_customers')
).toPandas()


# COMMAND ----------

# DBTITLE 1,Code Cell 9 - Using SQL Aggregate Functions with prefix returning list
# Modifed example from https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.agg.html

from pyspark.sql import functions as F

lpls_salesagg = spdf_salesinfo.agg(F.min(spdf_salesinfo.SalesAmount), F.max(spdf_salesinfo.SalesAmount)).collect()
lpls_salesagg

# COMMAND ----------

# DBTITLE 1,Code Cell 10 - Show Type
type(lpls_salesagg)

# COMMAND ----------

# DBTITLE 1,Code Cell 11 - run agg() returning pandas dataframe
from pyspark.sql import functions as F

lpdf_salesagg = spdf_salesinfo.agg(F.min(spdf_salesinfo.SalesAmount), F.max(spdf_salesinfo.SalesAmount)).toPandas()
lpdf_salesagg

# COMMAND ----------

# DBTITLE 1,Code Cell 12 - Another Example, multiple columns in aggregate function.
from pyspark.sql.functions import col

agg_df = spdf_salesinfo.groupBy("SalesTerritoryKey").agg(
    avg("SalesAmount"), 
    stddev("SalesAmount"), 
    min("SalesAmount"), 
    max("SalesAmount"), 
    countDistinct("ProductKey").alias("distinct_product_count"), 
    countDistinct("CustomerKey").alias("distinct_customer_count"),
    countDistinct("ProductKey", "CustomerKey").alias("distinct_product_customer_count")
)

filtered_df = agg_df.where(col("distinct_product_count") > 1)
display(filtered_df.toPandas())