# Databricks notebook source
# MAGIC %md # Coding pandas User Defined Scalar Functions (UDF)
# MAGIC
# MAGIC ![Spark Logo](http://spark-mooc.github.io/web-assets/images/ta_Spark-logo-small.png)
# MAGIC
# MAGIC More examples are available on the Spark website: http://spark.apache.org/examples.html
# MAGIC
# MAGIC Documentation on pandas UDFs at:
# MAGIC https://docs.microsoft.com/en-us/azure/databricks/spark/latest/spark-sql/udf-python-pandas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Author: Bryan Cafferky Copyright 09/27/2021

# COMMAND ----------

# MAGIC %md ### Warning!!!
# MAGIC
# MAGIC #### To run this code, you need to have uploaded the files and created the database tables - see Lesson 9 - Creating the SQL Tables on Databricks.  Link in video description to that video.

# COMMAND ----------

# DBTITLE 1,Code Cell 1 - Check the Spark version
sc.version

# COMMAND ----------

# DBTITLE 1,Code Cell 2 - Check if Arrow is Enabled
# See if Arrow is enabled.
spark.conf.get("spark.sql.execution.arrow.enabled")

# COMMAND ----------

# DBTITLE 1,Code Cell 3 - You can enable Apache Arrow as follows.
# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Enabling for Conversion to/from Pandas
# MAGIC
# MAGIC Arrow is available as an optimization when converting a Spark DataFrame to a Pandas DataFrame using the call toPandas() and when creating a Spark DataFrame from a Pandas DataFrame with createDataFrame(pandas_df). To use Arrow when executing these calls, users need to first set the Spark configuration spark.sql.execution.arrow.pyspark.enabled to true. This is disabled by default.
# MAGIC
# MAGIC See https://spark.apache.org/docs/3.0.1/sql-pyspark-pandas-with-arrow.html#enabling-for-conversion-tofrom-pandas

# COMMAND ----------

# DBTITLE 1,Code Cell 4 - Confirm PyArrow is enabled.
# Enable Arrow-based columnar data transfers
spark.conf.get("spark.sql.execution.arrow.pyspark.enabled")

# COMMAND ----------

# DBTITLE 1,Code Cell 5 
# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# COMMAND ----------

# DBTITLE 1,Code Cell 6
spark.conf.get("spark.sql.execution.arrow.pyspark.enabled")

# COMMAND ----------

# MAGIC %md
# MAGIC In addition, optimizations enabled by spark.sql.execution.arrow.pyspark.enabled could fallback automatically to non-Arrow optimization implementation if an error occurs before the actual computation within Spark. This can be controlled by spark.sql.execution.arrow.pyspark.fallback.enabled.

# COMMAND ----------

# DBTITLE 1,Code Cell 7
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

# COMMAND ----------

# DBTITLE 1,Versions: pandas and PyArrow
# MAGIC %md
# MAGIC Recommended Pandas and PyArrow Versions
# MAGIC For usage with pyspark.sql, the supported versions of Pandas is 0.24.2 and PyArrow is 0.15.1. Higher versions may be used, however, compatibility and data correctness can not be guaranteed and should be verified by the user.
# MAGIC
# MAGIC See https://spark.apache.org/docs/3.0.0/sql-pyspark-pandas-with-arrow.html#recommended-pandas-and-pyarrow-versions

# COMMAND ----------

# DBTITLE 1,Code Cell 8
import pandas as pd

pd.show_versions()

# COMMAND ----------

# DBTITLE 1,Code Cell 9 - Checking pyarrow version
import pyarrow

pyarrow.__version__

# COMMAND ----------

# MAGIC %md ## Create dataframe from a Spark SQL table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataframe naming prefix convention:
# MAGIC ##### 1st character is s for Spark DF
# MAGIC ##### 2nd character is p for Python
# MAGIC ##### 3rd and 4th character is df for dataframe
# MAGIC ##### 5th = _ separator
# MAGIC ##### rest is a meaningful name
# MAGIC
# MAGIC ##### spdf_salessummary = a Spark Python dataframe containing sales summary information.

# COMMAND ----------

# DBTITLE 1,Code Cell 10 - Use Spark SQL to load a PySpark dataframe from the t_salesinfo table...
spark.sql('use awproject')
spdf_sales = spark.sql('select CustomerKey, OrderDateKey, SalesAmount, TotalProductCost from factinternetsales limit 10').dropna()

# COMMAND ----------

# DBTITLE 1,Code Cell 11 - View the data
display(spdf_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Series to scalar UDF
# MAGIC * Series to scalar pandas UDFs are similar to Spark aggregate functions. 
# MAGIC * A Series to scalar pandas UDF defines an aggregation from one or more pandas Series to a scalar value, where each pandas Series represents a Spark column. 
# MAGIC * You use a Series to scalar pandas UDF with APIs such as select, withColumn, groupBy.agg, and pyspark.sql.Window.
# MAGIC *  This type of UDF does not support partial aggregation and all data for each group is loaded into memory.
# MAGIC
# MAGIC See https://docs.microsoft.com/en-us/azure/databricks/spark/latest/spark-sql/udf-python-pandas

# COMMAND ----------

# DBTITLE 1,Code Cell 12 - Create the Series to Scalar function
import pandas as pd
from pyspark.sql.functions import pandas_udf

# Declare the function and create the UDF
@pandas_udf("double")
def avg_margin_precent_udf(salesamount: pd.Series, productcost: pd.Series) -> float:
  profitmargin = (salesamount - productcost) / salesamount
  return profitmargin.mean()

# COMMAND ----------

# DBTITLE 1,Code Cell 13 - Call the function with select()
spdf_sales.select(avg_margin_precent_udf("SalesAmount", "TotalProductCost")).show()

# COMMAND ----------

# DBTITLE 1,Code Cell 14 - Call function with groupby()
spdf_sales.groupby("CustomerKey").agg(avg_margin_precent_udf("SalesAmount", "TotalProductCost")).show()

# COMMAND ----------

# DBTITLE 1,Code Cell 15 - Get error if you try to combine with aggregate functions
from pyspark.sql.functions import sum

spdf_sales.groupby("CustomerKey").agg(avg_margin_precent_udf("SalesAmount", "TotalProductCost"), sum("SalesAmount")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### See Lesson 16 in this series to learn more about SQL Window Functions.

# COMMAND ----------

# DBTITLE 1,Code Cell 16 - Call with windows function
from pyspark.sql import Window

w = Window \
    .partitionBy('CustomerKey') \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

spdf_sales.withColumn('avg_profit_margin', avg_margin_precent_udf("SalesAmount", "TotalProductCost").over(w)).show()