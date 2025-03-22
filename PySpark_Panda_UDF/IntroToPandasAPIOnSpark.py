# Databricks notebook source
# MAGIC %md 
# MAGIC ### Copyright Bryan Cafferky, BPC Global Solutions LLC
# MAGIC #### No warrantees expressed or implied.  Review and Test all code meets your requirements before using. 

# COMMAND ----------

# MAGIC %md ### Link to this notebook and data in the video description.

# COMMAND ----------

# MAGIC %md
# MAGIC # Introduction to the Pandas API package
# MAGIC
# MAGIC ## Pandas API is a library you use in place of pandas that switches the functions to 
# MAGIC ## use Spark dataframes instead of pandas dataframes.  Prior to Spark 3.2, this was a separate library
# MAGIC ## called Koalas.  Now it is part of PySpark.
# MAGIC
# MAGIC ### Documentation 
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/index.html
# MAGIC
# MAGIC ### Introduction to
# MAGIC https://docs.microsoft.com/en-us/azure/databricks/languages/pandas-spark
# MAGIC
# MAGIC ### User Guide
# MAGIC https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/index.html
# MAGIC
# MAGIC ### Migrating from Koalas to the PySPark pandas API
# MAGIC https://spark.apache.org/docs/latest/api/python/migration_guide/koalas_to_pyspark.html
# MAGIC
# MAGIC ### Databricks Blog about the pnadas API on Spark
# MAGIC
# MAGIC https://databricks.com/blog/2021/10/04/pandas-api-on-upcoming-apache-spark-3-2.html

# COMMAND ----------

# MAGIC %md 
# MAGIC ## To use the PySpark pandas API you must:
# MAGIC ### - Use Apache Spark 3.2 (Oct 13, 2021) or above
# MAGIC ### - Use Databricks Runtime 10.0 or above
# MAGIC
# MAGIC See Spark 3.2 release documentation: https://spark.apache.org/releases/spark-release-3-2-0.html

# COMMAND ----------

# MAGIC %md ## What is Pandas API? 
# MAGIC https://databricks.com/blog/2021/10/04/pandas-api-on-upcoming-apache-spark-3-2.html
# MAGIC
# MAGIC ## It's Databricks answer to Dask!

# COMMAND ----------

# MAGIC %md ### Why Pandas API?  The reason given.
# MAGIC #### - Inspired by Python Dask.  See my video on Dask.
# MAGIC #### - No need to learn a new Python API, just use pandas.
# MAGIC #### - Eases migration from local Python and pandas to Spark dataframes, i.e. almost no code changes. Just swap out the module.
# MAGIC #### - Pandas API implements almost all widely used APIs and features in pandas, such as plotting, grouping, windowing, I/O, and transformation.
# MAGIC #### - In addition, Pandas API APIs such as transform_batch and apply_batch can directly leverage pandas APIs, enabling almost all pandas workloads to be converted into Pandas API workloads with minimal changes in Pandas API 1.0.0.
# MAGIC #### - Uses the new Python User Defined Functions (UDF) APIs under the covers. <br>
# MAGIC
# MAGIC ##### See this for an explanation of why Koalas was merged into Spark. 
# MAGIC https://issues.apache.org/jira/browse/SPARK-34849

# COMMAND ----------

# MAGIC %md ### What is NOT supported?
# MAGIC
# MAGIC #### - Does not support Structured Streaming officially.
# MAGIC
# MAGIC #### - Some pandas data types:  Spark data types are not the same as pandas.
# MAGIC
# MAGIC #### - The APIs interacting with other DBMSes in Pandas API are slightly different from the ones in pandas because Pandas API leverages JDBC APIs in PySpark to read and write from/to other DBMSes.
# MAGIC
# MAGIC #### - Best Practices & Recommendations
# MAGIC https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/best_practices.html#leverage-pyspark-apis

# COMMAND ----------

# MAGIC %md # Recommendation - Parsimony?
# MAGIC - ### You started with Python using pandas.
# MAGIC - ### Next easiest step is to try Dask which can scale out. 
# MAGIC - ### If Dask does not cut it, try PySpark's pandas API or just use the PySpark API.
# MAGIC - ### Summary: pandas -> Dask -> Apache Spark

# COMMAND ----------

# MAGIC %md ### Change Notebook Theme

# COMMAND ----------

# MAGIC %md # The Arrrgh Test!!!

# COMMAND ----------

# MAGIC %md ### Let's do a quick basic test of Pandas API vs. pandas.  Does it really work?

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Try pandas code and then try the same code using the new PySpark pandas API

# COMMAND ----------

# MAGIC %md
# MAGIC ## Getting things set up...

# COMMAND ----------

# DBTITLE 1,Code Example 01 - Let's get the Python libraries we need...
# MAGIC %matplotlib notebook
# MAGIC
# MAGIC import pandas as pd
# MAGIC import numpy as np
# MAGIC import matplotlib as plt
# MAGIC
# MAGIC %matplotlib notebook 

# COMMAND ----------

# DBTITLE 1,Code Example 02 - As requested in the error message, change %matplotlib notebook to %matplotlib inline 
# MAGIC %matplotlib inline

# COMMAND ----------

# DBTITLE 1,Code Example 03 - Load pyspark pandas API
import pyspark.pandas as ps

# COMMAND ----------

# MAGIC %md
# MAGIC ### List files in the ..\Data folder that start with aw and have a csv file extension.  Notice I'm not using the magic command prefix.

# COMMAND ----------

# DBTITLE 1,Code Example 04 - List files in our data folder
# MAGIC %fs
# MAGIC
# MAGIC ls dbfs:/FileStore/tables/FactInternetSales.csv

# COMMAND ----------

# MAGIC %md ## Some migration notes: <br> <br>
# MAGIC
# MAGIC - Be sure to change any pandas module prefix to the Pandas API, i.e. pd. to ps.
# MAGIC - Be careful to change all pandas dataframe names to the correct Pandas API dataframe names!
# MAGIC - The pd.function() format does not work in all cases on Pandas API but the dataframe.method() syntax might!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparing pandas to Pandas API.  Are they identical?

# COMMAND ----------

# DBTITLE 1,Code Example 05 - pandas API Code complete
ps.read_

# COMMAND ----------

help(ps)

# COMMAND ----------

help(ps.read_csv)

# COMMAND ----------

# MAGIC %md
# MAGIC ###     Dataframes...

# COMMAND ----------

# DBTITLE 1,Code Example 06 - Load a file into a pandas dataframe, note path has /dbfs/ not dbfs:
lpdf_sales = pd.read_csv('/dbfs/FileStore/tables/FactInternetSales.csv', nrows=2000)

# COMMAND ----------

# DBTITLE 1,Code Example 07 - Load a file into a pandas API dataframe, we get an error
# Note:  We get an error on the path specification.  We need to change it!
psdf_sales = ps.read_csv('/FileStore/tables/FactInternetSales.csv', nrows=2000)

# COMMAND ----------

# DBTITLE 1,Code Example 08 - Load a file into a pandas API dataframe - after fixing the path spec
psdf_sales = ps.read_csv('/FileStore/tables/FactInternetSales.csv', nrows=2000)

# COMMAND ----------

# DBTITLE 1,Code Example 09 - pandas - display some rows with the head() method
lpdf_sales.head(2)

# COMMAND ----------

# DBTITLE 1,Code Example 10 - pandas API - display some rows with the head() method
psdf_sales.head(2)

# COMMAND ----------

# DBTITLE 1,Code Example 11 - pandas - Confirm the data frame type
type(lpdf_sales)

# COMMAND ----------

# DBTITLE 1,Code Example 12 - pandas API - Confirm the data frame type
type(psdf_sales)

# COMMAND ----------

# DBTITLE 1,Code Example 13 - pandas - View column names and types
lpdf_sales.dtypes.head(4)

# COMMAND ----------

# DBTITLE 1,Code Example 14 - pandas API - View column names and types
psdf_sales.dtypes.head(4)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Statistical methods...

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using the . syntax to reference columns...

# COMMAND ----------

# DBTITLE 1,Code Example 15 - pandas - Get summary stats with describe()
lpdf_sales.SalesAmount.describe()

# COMMAND ----------

# DBTITLE 1,Code Example 16 - pandas API -  Get summary stats with describe()
psdf_sales.SalesAmount.describe()

# COMMAND ----------

# DBTITLE 1, Code Example 17 - pandas - Get the average (mean) of 
lpdf_sales.SalesAmount.mean()

# COMMAND ----------

# DBTITLE 1, Code Example 18 - pandas API - Get the average (mean) of 
psdf_sales.SalesAmount.mean()

# COMMAND ----------

# DBTITLE 1,Code Example 19 - pandas - Using the [ ] syntax to reference columns...
lpdf_sales['SalesAmount']

# COMMAND ----------

# DBTITLE 1,Code Example 20 - pandas API - Using the [ ] syntax to reference columns.  Notice all rows display.
psdf_sales['SalesAmount']

# COMMAND ----------

# DBTITLE 1,Code Example 21 - pandas - Filtering rows
lpdf_sales[lpdf_sales['SalesAmount'] > 2000].head(3)

# COMMAND ----------

# DBTITLE 1,Code Example 22 - pandas API - Filtering rows - be careful to change ALL dataframe prefixes!
psdf_sales[psdf_sales['SalesAmount'] > 2000].head(3)

# COMMAND ----------

# DBTITLE 1,Code Example 23 - pandas - Slicing by rows by index values, columns by name
lpdf_sales[0:3]['ProductKey']

# COMMAND ----------

# DBTITLE 1,Code Example 24 - pandas API - Slicing by rows by index values, columns by name
psdf_sales[0:3]['ProductKey']

# COMMAND ----------

# DBTITLE 1,Code Example 25 - pandas - Select columns by index
lpdf_sales.iloc[:,[1,2]].head(5)  # select by column number...

# COMMAND ----------

# DBTITLE 1,Code Example 26 - pandas API - Select columns by index
psdf_sales.iloc[:,[1,2]].head(5)  # select by column number...

# COMMAND ----------

# DBTITLE 1,Code Example 27 - pandas - Slicing by row index and column names
lpdf_sales[1:5][['ProductKey', 'SalesAmount']]

# COMMAND ----------

# DBTITLE 1,Code Example 28 - pandas API - Slicing by row index and column names
psdf_sales[1:5][['ProductKey', 'SalesAmount']]

# COMMAND ----------

# DBTITLE 1,Code Example 29 - pandas - Setting the index
lpdf_sales = lpdf_sales.set_index('SalesOrderNumber', drop=False)
lpdf_sales.head(3)

# COMMAND ----------

# DBTITLE 1,Code Example 30 - pandas API - Setting the index
psdf_sales = psdf_sales.set_index('SalesOrderNumber', drop=False)
psdf_sales.head(3)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Take-A-Ways
# MAGIC
# MAGIC #### - pandas syntax for our test was a 100% match!  Success!
# MAGIC #### - Code changes to switch from pandas to the PySpark pandas API was minimal.
# MAGIC #### - Name your dataframes so you can tell which is local pandas and which is a PySpark pandas API.
# MAGIC #### - Read and follow the Best Practices Guidelines. https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/best_practices.html#leverage-pyspark-apis
# MAGIC #### - Accept that that this is a moving target.  Changes will be coming. 

# COMMAND ----------

# MAGIC %md  
# MAGIC
# MAGIC
# MAGIC ## <h1 align="right">Thank You!</h1>  
# MAGIC
# MAGIC ### Please like, share, subscribe, and support me on Patreon.  Link in the description.