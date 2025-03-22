# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction to the Koalas package
# MAGIC
# MAGIC ### Koalas is a library you use in place of pandas that switches the functions to 
# MAGIC ### use Spark dataframes instead of pandas dataframes.
# MAGIC
# MAGIC ### Documentation 
# MAGIC https://pythonrepo.com/repo/databricks-koalas-python-data-containers
# MAGIC
# MAGIC ### See the Python Source Code
# MAGIC https://koalas.readthedocs.io/en/latest/_modules/databricks/koalas/frame.html

# COMMAND ----------

# MAGIC %md ### Why Koalas?
# MAGIC #### - No need to learn a new Python API, just use pandas.
# MAGIC #### - Eases migration from local Python and pandas to Spark dataframes, i.e. almost no code changes. Just swap out the module.
# MAGIC #### - Koalas implements almost all widely used APIs and features in pandas, such as plotting, grouping, windowing, I/O, and transformation.
# MAGIC #### - In addition, Koalas APIs such as transform_batch and apply_batch can directly leverage pandas APIs, enabling almost all pandas workloads to be converted into Koalas workloads with minimal changes in Koalas 1.0.0.
# MAGIC #### - Uses the new Python User Defined Functions (UDF) APIs under the covers.

# COMMAND ----------

# MAGIC %md ### What is Koalas? 
# MAGIC https://databricks.com/blog/2020/06/24/introducing-koalas-1-0.html

# COMMAND ----------

# MAGIC %md ### What is NOT supported?
# MAGIC
# MAGIC #### Some pandas data types:  Spark data types are not the same as pandas.
# MAGIC https://koalas.readthedocs.io/en/latest/user_guide/types.html
# MAGIC
# MAGIC #### The APIs interacting with other DBMSes in Koalas are slightly different from the ones in pandas because Koalas leverages JDBC APIs in PySpark to read and write from/to other DBMSes.
# MAGIC https://koalas.readthedocs.io/en/latest/user_guide/from_to_dbms.html
# MAGIC
# MAGIC #### Best Practices & Recommendations
# MAGIC https://koalas.readthedocs.io/en/latest/user_guide/best_practices.html
# MAGIC

# COMMAND ----------

# MAGIC %md ### Let's do a quick basic test of Koalas vs. pandas.  Does it really work?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Getting things set up...

# COMMAND ----------

# DBTITLE 1,Code Example 01 - Let's get the Python libraries we need...
import pandas as pd
import numpy as np
import matplotlib as plt

%matplotlib notebook 

# COMMAND ----------

# DBTITLE 1,Code Example 02 - As requested in the error message, change %matplotlib notebook to %matplotlib inline 
# MAGIC %matplotlib inline

# COMMAND ----------

# DBTITLE 1,Code Example 03 - Load koalas
import databricks.koalas as ks

# COMMAND ----------

# MAGIC %md
# MAGIC ### List files in the ..\Data folder that start with aw and have a csv file extension.  Notice I'm not using the magic command prefix.

# COMMAND ----------

# DBTITLE 1,Code Example 04 - List files in our data folder
# MAGIC %fs
# MAGIC
# MAGIC ls dbfs:/FileStore/tables/

# COMMAND ----------

# MAGIC %md ## Some migration notes: <br> <br>
# MAGIC
# MAGIC - Be sure to change any pandas module prefix to the koalas, i.e. pd. to ks.
# MAGIC - Be careful to change all pandas dataframe names to the correct koalas dataframe names!
# MAGIC - The pd.function() format does not work in all cases on koalas but the dataframe.method() syntax might!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparing pandas to koalas.  Are they identical?

# COMMAND ----------

# DBTITLE 1,Code Example 05 - koalas tab complete
ks.read_

# COMMAND ----------

help(ks)

# COMMAND ----------

help(ks.read_csv)

# COMMAND ----------

# MAGIC %md
# MAGIC ###     Dataframes...

# COMMAND ----------

# DBTITLE 1,Code Example 06 - Load a file into a pandas dataframe, note path has /dbfs/ not dbfs:
lpdf_sales = pd.read_csv('/dbfs/FileStore/tables/FactInternetSales.csv', nrows=2000)

# COMMAND ----------

# DBTITLE 1,Code Example 07 - Load a file into a koalas dataframe, we get an error
# Note:  We get an error on the path specification.  We need to change it!
skdf_sales = ks.read_csv('/dbfs/FileStore/tables/FactInternetSales.csv', nrows=2000)

# COMMAND ----------

# DBTITLE 1,Code Example 08 - Load a file into a koalas dataframe - after fixing the path spec
skdf_sales = ks.read_csv('/FileStore/tables/FactInternetSales.csv', nrows=2000)

# COMMAND ----------

# DBTITLE 1,Code Example 09 - pandas - display some rows with the head() method
lpdf_sales.head(2)

# COMMAND ----------

# DBTITLE 1,Code Example 10 - koalas - display some rows with the head() method
skdf_sales.head(2)

# COMMAND ----------

# DBTITLE 1,Code Example 11 - pandas - Confirm the data frame type
type(lpdf_sales)

# COMMAND ----------

# DBTITLE 1,Code Example 12 - koalas - Confirm the data frame type
type(skdf_sales)

# COMMAND ----------

# DBTITLE 1,Code Example 13 - pandas - View column names and types
lpdf_sales.dtypes.head(4)

# COMMAND ----------

# DBTITLE 1,Code Example 14 - koalas- View column names and types
skdf_sales.dtypes.head(4)

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

# DBTITLE 1,Code Example 16 - koalas -  Get summary stats with describe()
skdf_sales.SalesAmount.describe()

# COMMAND ----------

# DBTITLE 1, Code Example 17 - pandas - Get the average (mean) of 
lpdf_sales.SalesAmount.mean()

# COMMAND ----------

# DBTITLE 1, Code Example 18 - koalas Get the average (mean) of 
skdf_sales.SalesAmount.mean()

# COMMAND ----------

# DBTITLE 1,Code Example 19 - pandas - Using the [ ] syntax to reference columns...
lpdf_sales['SalesAmount']

# COMMAND ----------

# DBTITLE 1,Code Example 20 - koalas - Using the [ ] syntax to reference columns.  Notice all rows display.
skdf_sales['SalesAmount']

# COMMAND ----------

# DBTITLE 1,Code Example 21 - koalas - Filtering rows
lpdf_sales[lpdf_sales['SalesAmount'] > 2000].head(3)

# COMMAND ----------

# DBTITLE 1,Code Example 22 - koalas - Filtering rows - be careful to change ALL dataframe prefixes!
skdf_sales[skdf_sales['SalesAmount'] > 2000].head(3)

# COMMAND ----------

# DBTITLE 1,Code Example 23 - koalas - Slicing by rows by index values, columns by name
lpdf_sales[0:3]['ProductKey']

# COMMAND ----------

# DBTITLE 1,Code Example 24 - koalas - Slicing by rows by index values, columns by name
skdf_sales[0:3]['ProductKey']

# COMMAND ----------

# DBTITLE 1,Code Example 25 - pandas - Select columns by index
lpdf_sales.iloc[:,[1,2]].head(5)  # select by column number...

# COMMAND ----------

# DBTITLE 1,Code Example 26 - koalas - Select columns by index
skdf_sales.iloc[:,[1,2]].head(5)  # select by column number...

# COMMAND ----------

# DBTITLE 1,Code Example 27 - pandas - Slicing by row numbers and column names
lpdf_sales[1:5][['ProductKey', 'SalesAmount']]

# COMMAND ----------

# DBTITLE 1,Code Example 28 - koalas - Slicing by row numbers and column names
skdf_sales[1:5][['ProductKey', 'SalesAmount']]

# COMMAND ----------

# DBTITLE 1,Code Example 29 - pandas - Setting the index
lpdf_sales = lpdf_sales.set_index('SalesOrderNumber', drop=False)
lpdf_sales.head(3)

# COMMAND ----------

# DBTITLE 1,Code Example 30 - koalas - Setting the index
skdf_sales = skdf_sales.set_index('SalesOrderNumber', drop=False)
skdf_sales.head(3)

# COMMAND ----------

# MAGIC %md ## Take-A-Ways
# MAGIC
# MAGIC ### - pandas syntax for our test was a 100% match!  Success!
# MAGIC ### - Code changes to switch from pandas to koalas was minimal.
# MAGIC ### - Name your dataframes so you can tell which is pandas and which is koalas.

# COMMAND ----------

# MAGIC %md  
# MAGIC
# MAGIC
# MAGIC ## <h1 align="right">Thank You!</h1>  
# MAGIC
# MAGIC ### Please like, share, subscribe, and support me on Patreon.  Link in the description.

# COMMAND ----------

