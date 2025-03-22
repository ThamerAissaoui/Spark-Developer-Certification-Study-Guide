# Databricks notebook source
# MAGIC %md ## Reading in parallel from a SQL Database with Java Database Connectivity (JDBC)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Useful links:
# MAGIC https://karanasou.medium.com/pyspark-parallel-read-from-database-726f4aa910b
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Author:  Bryan Cafferky Copyright 11/06/2021
# MAGIC
# MAGIC ### Demo code only.  No warrantees.  It is the user responsibility to test and modify code as needed.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html

# COMMAND ----------

# MAGIC %md ### Accessing an Azure SQL database using Python

# COMMAND ----------

# MAGIC %md ### Confirm:
# MAGIC - Azure Services are allowed to access the Azure SQL database.
# MAGIC - Client IP Address has been added to the firewall rules.

# COMMAND ----------

# DBTITLE 1,Code Sample 01 - All connection details in clear text - Not Secure!!!

userid = "bcafferky"
dbpassword = "somepw123?"
jdbcHostname = "sqldbserverbpc.database.windows.net"
jdbcDatabase = "sqldb"
jdbcPort = 1433

# COMMAND ----------

# DBTITLE 1,Code Sample 02 - Build the connection url and properties

jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

connectionProperties = {
  "user" : userid,
  "password" : dbpassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Partionining Parameters:
# MAGIC - numPartitions: the number of data splits
# MAGIC - column: the column to partition by, e.g. id,
# MAGIC - lowerBound: the minimum value for the column — inclusive,
# MAGIC - upperBound: the maximum value of the column —be careful, it is exclusive

# COMMAND ----------

# DBTITLE 1,Code Sample 03 - Get the min and max keys to be used for partitioning

pushdown_query = """(
select min(CustomerID) as MinID, max(CustomerID) as MaxID
from SalesLT.Customer          c
) cust"""

bounds = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties).collect()[0]

bounds

# COMMAND ----------

type(bounds)

# COMMAND ----------

# DBTITLE 1,Code Sample 04 - Query a table into a dataframe

spdf_sales = spark.read.jdbc(url=jdbcUrl, table='SalesLT.Customer', 
                             properties=connectionProperties, 
                             numPartitions=6,
                             column="CustomerID",
                             lowerBound=bounds.MinID, 
                             upperBound=bounds.MaxID + 1)

display(spdf_sales)

# COMMAND ----------


spdf_sales.rdd.getNumPartitions()

# COMMAND ----------


pushdown_query = """(
select oh.CustomerID, oh.OrderDate, oh.Status, oh.SubTotal, oh.Freight, 
       oh.TaxAmt, c.CompanyName, c.SalesPerson
from       SalesLT.SalesOrderHeader oh
inner join SalesLT.Customer          c
on (oh.CustomerID = c.CustomerID)
) cust"""

spdf_pushdown = spark.read.jdbc(url=jdbcUrl, 
                                table=pushdown_query, 
                                properties=connectionProperties,
                                numPartitions=4,
                                column="CustomerID",
                                lowerBound=bounds.MinID, 
                                upperBound=bounds.MaxID + 1)

display(spdf_pushdown)

# COMMAND ----------


spdf_pushdown.rdd.getNumPartitions()