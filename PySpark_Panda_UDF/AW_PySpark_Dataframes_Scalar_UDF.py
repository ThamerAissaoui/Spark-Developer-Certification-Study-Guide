# Databricks notebook source
# MAGIC %md # AdventureWorks EDA with Python Dataframes Scalar User Defined Function (UDF) - Part 3
# MAGIC
# MAGIC ![Spark Logo](http://spark-mooc.github.io/web-assets/images/ta_Spark-logo-small.png)
# MAGIC
# MAGIC More examples are available on the Spark website: http://spark.apache.org/examples.html
# MAGIC
# MAGIC PySpark API documentation: http://spark.apache.org/docs/latest/api/python/

# COMMAND ----------

# MAGIC %md
# MAGIC ## Author: Bryan Cafferky Copyright 08/01/2021

# COMMAND ----------

# MAGIC %md  #### With Databricks, there's no need to import PySpark or create a Spark Context...

# COMMAND ----------

# MAGIC %md ### Warning!!!
# MAGIC
# MAGIC #### To run this code, you need to have uploaded the files and created the database tables - see Lesson 9 - Creating the SQL Tables on Databricks.  Link in video description to that video.

# COMMAND ----------

# MAGIC %md ## Using Spark SQL from Python

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataframe naming prefix convention:
# MAGIC #### 1st character is s for Spark DF
# MAGIC #### 2nd character is p for Python
# MAGIC #### 3rd and 4th character is df for dataframe
# MAGIC #### 5th = _ separator
# MAGIC #### rest is a meaningful name
# MAGIC
# MAGIC #### spdf_salessummary = a Spark Python dataframe containing sales summary information.

# COMMAND ----------

# DBTITLE 1,Code Cell 1 - Use Spark SQL to load a PySpark dataframe from table factinternetsales
spark.sql('use awproject')
spdf_salesinfo = spark.sql('select * from factinternetsales').dropna()

# COMMAND ----------

# DBTITLE 1,Code Cell 2 - Display some data
display(spdf_salesinfo.take(3))

# COMMAND ----------

# DBTITLE 1,Code Cell 3 - Show Dataframe Schema
spdf_salesinfo.printSchema()

# COMMAND ----------

# MAGIC %md ## PySpark User Defined Scalar Functions

# COMMAND ----------

# MAGIC %md ### Enable Apache Arrow for optimal performance
# MAGIC
# MAGIC ##### Apache Arrow is a language-agnostic software framework for developing data analytics applications that process columnar data. It contains a standardized column-oriented memory format that is able to represent flat and hierarchical data for efficient analytic operations on modern CPU and GPU hardware.
# MAGIC
# MAGIC https://arrow.apache.org/

# COMMAND ----------

# MAGIC %md ### Spark Configurations
# MAGIC https://kb.databricks.com/data/get-and-set-spark-config.html

# COMMAND ----------

# DBTITLE 1,Code Cell 4 - Show All Spark Configuration Settings
sc.getConf().getAll()

# COMMAND ----------

# DBTITLE 1,Code Cell 5 - Get a Specific Spark Configuration Setting Value
spark.conf.get("spark.sql.execution.arrow.enabled")

# COMMAND ----------

# DBTITLE 1,Code Cell 6 - Enable Apache Arrow as follows
# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# COMMAND ----------

# DBTITLE 1,Code Cell 7 - Change Database Context
# MAGIC %sql USE awproject

# COMMAND ----------

# DBTITLE 1,Code Cell 8 - DESCRIBE factinternetsales
# MAGIC %sql describe table dimproduct

# COMMAND ----------

# MAGIC %md ## Spark SQL Scalar Functions
# MAGIC
# MAGIC ### Before you create a PySpark scalar UDF, make sure there is no Spark SQL function that does what you need! 
# MAGIC
# MAGIC #### Spark SQL Functions documentation at:
# MAGIC
# MAGIC https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/functions.html

# COMMAND ----------

# DBTITLE 1,Code Cell 9 - Using Spark SQL Scalar Functions
# MAGIC %sql
# MAGIC
# MAGIC SELECT ModelName , instr(ModelName, 'Mount'), substring(ModelName, 1,2), translate(ModelName, 'o', 'X') 
# MAGIC FROM dimproduct
# MAGIC WHERE instr(ModelName, 'Mount') > 0;

# COMMAND ----------

# DBTITLE 1,Code Cell 10 - Load table dimproduct into a PySpark dataframe
spdf_dimproduct = spark.sql('''select * from awproject.dimproduct''')

# COMMAND ----------

# DBTITLE 1,Code Cell 11 - Using SQL Scalar Functions on a PySpark dataframe
from pyspark.sql.functions import instr, translate, substring

# Note:  where() is an alias for filter()

display(
spdf_dimproduct.select(instr("ModelName", "Mount"), substring("ModelName",1,2), translate('ModelName', 'o', 'X')).
                where(instr("ModelName", "Mount") > 0)
)

# COMMAND ----------

# MAGIC %md ### Defining and calling a custom scalar User Defined Function (UDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## you have to register the function with Spark if you intend to use afterward! Nothe that this UDF will be running in parallel on all nodes, not on the head node, neither on the driver node

# COMMAND ----------

# DBTITLE 1,Code Cell 12 - Define the function
from pyspark.sql.types import DoubleType

def margin_precent_type(productcost, saleamt):
  return (saleamt - productcost) / saleamt

spark.udf.register("margin_percent", margin_precent_type, DoubleType())

# COMMAND ----------

# DBTITLE 1,Code Cell 13 - Call your UDF from Spark SQL
# MAGIC %sql 
# MAGIC
# MAGIC SELECT margin_percent(TotalProductCost, SalesAmount) as gross_margin, TotalProductCost, SalesAmount 
# MAGIC FROM  awproject.factinternetsales LIMIT 2;

# COMMAND ----------

# DBTITLE 1,Code Cell 14 - Enable your UDF to be used with Python
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

margin_percent_udf = udf(margin_precent_type, DoubleType())

# COMMAND ----------

# DBTITLE 1,Code Cell 15 - Use UDF with Spark Dataframe
display(spdf_salesinfo.select("SalesAmount", "TotalProductCost", 
                              margin_percent_udf("TotalProductCost", "SalesAmount").alias("MarginPercent")))

# COMMAND ----------

# DBTITLE 1,Code Cell 16 - Define UDF via Method 2 using a decorator
from pyspark.sql.functions import udf

@udf("double")
def margin_percent_decorator_udf(productcost, saleamt):
  return (saleamt - productcost) / saleamt

# COMMAND ----------

# DBTITLE 1,Code Cell 17 - Calling the UDF Created using a Decorator
display(spdf_salesinfo.select("SalesAmount", "TotalProductCost", 
                               margin_percent_decorator_udf("TotalProductCost", "SalesAmount").alias("MarginPercent")).limit(3))

# COMMAND ----------

# DBTITLE 1,Code Cell 18 - Register the UDF that used the Decorator
# Note:  Does not accept the return type parameter, i.e. DoubleType()

spark.udf.register("margin_percent_decorator", margin_percent_decorator_udf)

# COMMAND ----------

# DBTITLE 1,Code Cell 19 - Use the UDF Created with a Decorator in Spark SQL
# MAGIC %sql 
# MAGIC
# MAGIC SELECT margin_percent_decorator(TotalProductCost, SalesAmount) as gross_margin, TotalProductCost, SalesAmount 
# MAGIC FROM  awproject.factinternetsales LIMIT 3;