# Databricks notebook source
# MAGIC %md
# MAGIC ## Author: Bryan Cafferky Copyright 10/15/2021
# MAGIC
# MAGIC #### Caution:  This code provided for demonstration "as is" with no implied warrantees.  Always test and vet any code before using.

# COMMAND ----------

# MAGIC %md # Coding pandas Function API
# MAGIC
# MAGIC ### In this notebook we will learn how to code the following Spark pandas API function types:
# MAGIC
# MAGIC #### - Grouped Map
# MAGIC #### - Map
# MAGIC #### - Cogroup Map

# COMMAND ----------

# MAGIC %md ## Notes:
# MAGIC
# MAGIC #### - Leverages Apache Arrow.
# MAGIC #### - Does not use Python type hints.
# MAGIC #### - This API is experimental according to the Apache Spark documentation. 
# MAGIC #### - Each pandas.DataFrame size can be controlled by spark.sql.execution.arrow.maxRecordsPerBatch.

# COMMAND ----------

# MAGIC %md ![Spark Logo](http://spark-mooc.github.io/web-assets/images/ta_Spark-logo-small.png)
# MAGIC
# MAGIC More examples are available on the Spark website: http://spark.apache.org/examples.html
# MAGIC
# MAGIC Documentation on pandas unction API at:
# MAGIC https://docs.microsoft.com/en-us/azure/databricks/spark/latest/spark-sql/pandas-function-apis
# MAGIC

# COMMAND ----------

http://www.ltcconline.net/greenl/courses/201/probdist/zScore.htm%md ### Warning!!!

#### To run this code, you need to have uploaded the files and created the database tables - see Lesson 9 - Creating the SQL Tables on Databricks.  Link in video description to that video.

# COMMAND ----------

# MAGIC %md ### Skips Code Cells 1 through 9 if you already have Apache Arrow and PyArrow enabled.

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

# MAGIC %md ## Coding pandas API functions starts here!!!

# COMMAND ----------

# MAGIC %md ### Create dataframe from a Spark SQL table

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

# DBTITLE 1,Code Cell 10 - Load our Spark data frame
spark.sql('use awproject')

spdf_sales = spark.sql('select CustomerKey, SalesAmount from factinternetsales limit 15000').dropna()

# COMMAND ----------

# DBTITLE 1,Code Cell 11 - View the data
display(spdf_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Group Map
# MAGIC
# MAGIC ###### You transform your grouped data via groupBy().applyInPandas() to implement the “split-apply-combine” pattern. Split-apply-combine consists of three steps:
# MAGIC
# MAGIC - Split the data into groups by using DataFrame.groupBy.
# MAGIC - Apply a function on each group. The input and output of the function are both pandas.DataFrame. 
# MAGIC - The input data contains all the rows and columns for each group.
# MAGIC - Combine the results into a new DataFrame.

# COMMAND ----------

# DBTITLE 1,Code Cell 12 - Simple Group Map example
df = spark.createDataFrame(
    [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
    ("id", "v"))

def subtract_mean(pdf):
    # pdf is a pandas.DataFrame
    v = pdf.v
    return pdf.assign(v_minus_mean = v - v.mean())

df.groupby("id").applyInPandas(subtract_mean, schema="id long, v double, v_minus_mean double").show()

# COMMAND ----------

# MAGIC %md #### See information at blog
# MAGIC Link https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.applyInPandas.html#pyspark-sql-groupeddata-applyinpandas
# MAGIC
# MAGIC #### For information about calculating the z-score of a number see
# MAGIC https://developers.google.com/machine-learning/data-prep/transform/normalization
# MAGIC
# MAGIC http://www.ltcconline.net/greenl/courses/201/probdist/zScore.htm
# MAGIC
# MAGIC
# MAGIC ### To Get the Z-score:
# MAGIC
# MAGIC z-score  = (value - values mean) / standard deviation of the values

# COMMAND ----------

# DBTITLE 1,Code Cell 13 - Group Map - Create a function to get the z-core of each SalesAmount
import pandas as pd

def get_z_score(pdf):
    # pdf is a pandas.DataFrame
    SalesAmount = pdf.SalesAmount
    return pdf.assign(ZScoreSales = (SalesAmount - SalesAmount.mean()) / SalesAmount.std(), AvgSales =  SalesAmount.mean())

spdf_sales.groupby("CustomerKey").applyInPandas(get_z_score, schema="CustomerKey long, SalesAmount double, ZScoreSales double, AvgSales double").show()

# COMMAND ----------

# MAGIC %md ### Map 
# MAGIC
# MAGIC  #### - Note:  Seems to be primarily used to filter Spark dataframes.
# MAGIC
# MAGIC
# MAGIC - Uses DataFrame.mapInPandas() in order to transform an iterator of pandas.DataFrame to another iterator of pandas.DataFrame that represents the current PySpark DataFrame.
# MAGIC - Returns the result as a PySpark DataFrame.
# MAGIC - The underlying function takes and outputs an iterator of pandas.DataFrame. 
# MAGIC - It can return the output of arbitrary length in contrast to some pandas UDFs such as Series to Series pandas UDF.

# COMMAND ----------

# DBTITLE 1,Code Cell 14 - Map - Simple Example
df = spark.createDataFrame([(1, 21), (2, 30)], ("id", "age"))

def filter_func(iterator):
    for pdf in iterator:
        yield pdf[pdf.id == 1]

df.mapInPandas(filter_func, schema=df.schema).show()

# COMMAND ----------

# MAGIC %md  ### Some experimentation with the Map API 

# COMMAND ----------

# DBTITLE 1,Code Cell 15 - Broadcast a variable
b_multiplier = sc.broadcast(2)
b_multiplier.value

# COMMAND ----------

# MAGIC %md 
# MAGIC #### When you create a pandas UDF as I showed in a prior video, 
# MAGIC #### you can do initialization work before you start the iterator.  This seems to work here as well.

# COMMAND ----------

# DBTITLE 1,Code Cell 16 - Map - A bit more complex and experimental
df = spark.createDataFrame([(1, 21), (1, 15), (2, 30)], ("id", "age"))

def filter_func(iterator):
  
    # Do some expensive initialization with a state  - This is not mentioned in the docs
    # so beware.  It may not be a good idea but wanted to see if it worked.  :-) 
    multiplier = b_multiplier.value
    
    for pdf in iterator:
        yield pdf[pdf.id == 1].assign(age_times_x = pdf.age[pdf.id == 1] * multiplier)

df.mapInPandas(filter_func, schema="id long, age long, age_times_x double").show()

# COMMAND ----------

# DBTITLE 1,Code Cell 16 - Map - Create a function to filter the sales dataframe
def filter_func(iterator):
    for pdf in iterator:
        yield pdf[pdf.CustomerKey == 11000]

spdf_sales.mapInPandas(filter_func, schema=spdf_sales.schema).show()

# COMMAND ----------

# DBTITLE 1,Code Cell 17 - Map - Doing a calculation on a column in the dataframe. Probably not a good idea.
def filter_func(iterator):
    for pdf in iterator:
        SalesAmount = pdf.SalesAmount
        pdf = pdf.assign(SalesAmount = (SalesAmount - SalesAmount.mean()) / SalesAmount.std())
        yield pdf[pdf.CustomerKey == 11000]

spdf_sales.mapInPandas(filter_func, schema=spdf_sales.schema).show()

# COMMAND ----------

# MAGIC %md ### Cogrouped Map
# MAGIC
# MAGIC #### Purpose:  Join dataframes on specified keys.
# MAGIC
# MAGIC This function requires a full shuffle, i.e. this is an expensive operation.
# MAGIC
# MAGIC All the data of a cogroup will be loaded into memory, so the user should be aware of the potential OOM risk if data is skewed and certain groups are too large to fit in memory.
# MAGIC
# MAGIC If returning a new pandas.DataFrame constructed with a dictionary, it is recommended to explicitly index the columns by name to ensure the positions are correct, or alternatively use an OrderedDict. For example, pd.DataFrame({‘id’: ids, ‘a’: data}, columns=[‘id’, ‘a’]) or pd.DataFrame(OrderedDict([(‘id’, ids), (‘a’, data)])).

# COMMAND ----------

# MAGIC %md 
# MAGIC #### It consists of the following steps:
# MAGIC   
# MAGIC - Shuffle the data such that the groups of each DataFrame which share a key are cogrouped together.
# MAGIC - Apply a function to each cogroup. The input of the function is two pandas.DataFrame (with an optional tuple representing the key). The output of the function is a pandas.DataFrame.
# MAGIC - Combine the pandas.DataFrames from all groups into a new PySpark DataFrame.

# COMMAND ----------

# DBTITLE 1,Code Cell 18 - Cogrouped Map - Simple Example
import pandas as pd

df1 = spark.createDataFrame(
    [(20000101, 1, 1.0), (20000101, 2, 2.0), (20000102, 1, 3.0), (20000102, 2, 4.0)],
    ("time", "id", "v1"))

df2 = spark.createDataFrame(
    [(20000101, 1, "x"), (20000101, 2, "y")],
    ("time", "id", "v2"))

def asof_join(l, r):
    return pd.merge_asof(l, r, on="time", by="id")

df1.groupby("id").cogroup(df2.groupby("id")).applyInPandas(
    asof_join, schema="time int, id int, v1 double, v2 string").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### For more information on merge_asof() is a pandas function
# MAGIC See https://pandas.pydata.org/pandas-docs/version/0.25.0/reference/api/pandas.merge_asof.html

# COMMAND ----------

# DBTITLE 1,Code Cell 19 - Load customer data into a Spark dataframe
spark.sql('use awproject')
spdf_customer = spark.sql('select CustomerKey, BirthDate from DimCustomer').dropna()

# COMMAND ----------

# DBTITLE 1,Code Cell 20 - Cogroup Map - Join Customer dataframe to Sales dataframe on CustomerKey
import pandas as pd

def asof_join(l, r):
    result = pd.merge_asof(l, r, on="CustomerKey")
    result["BirthDate"] = result["BirthDate"].astype(str)
    return result

spdf_customer.groupby("CustomerKey").cogroup(spdf_sales.groupby("CustomerKey")).applyInPandas(
    asof_join, schema="CustomerKey int, SalesAmount double, BirthDate string")

display(spdf_customer)