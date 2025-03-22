// Databricks notebook source
// MAGIC %md ## Reading from a SQL Database with Java Database Connectivity (JDBC)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Author:  Bryan Cafferky Copyright 11/06/2021
// MAGIC
// MAGIC ### Demo code only.  No warrantees.  It is the user responsibility to test and modify code as needed.

// COMMAND ----------

// MAGIC %md ### Documentation Links
// MAGIC
// MAGIC https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html

// COMMAND ----------

// MAGIC %md ### Accessing an Azure SQL database using Python

// COMMAND ----------

// MAGIC %md ### Confirm:
// MAGIC - Azure Services are allowed to access the Azure SQL database.
// MAGIC - Client IP Address has been added to the firewall rules.

// COMMAND ----------

// DBTITLE 1,Code Sample 01 - All connection details in clear text - Not Secure!!!
// MAGIC %python
// MAGIC
// MAGIC userid = "test"
// MAGIC dbpassword = "GhaithIlyan20142018!"
// MAGIC jdbcHostname = "databricks-server-sql.database.windows.net"
// MAGIC jdbcDatabase = "az-databricks-sql-db"
// MAGIC jdbcPort = 1433

// COMMAND ----------

// MAGIC %md ## Create an Azure Key Vault backed scope in Databricks
// MAGIC
// MAGIC https://westus.azuredatabricks.net#secrets/createScope
// MAGIC
// MAGIC https://eastus.azuredatabricks.net#secrets/createScope

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.secrets.listScopes()

// COMMAND ----------

// DBTITLE 1,Code Sample 02 - If you are using Azure Key Vault to secure userid and pw - Secure!!!
// MAGIC %python
// MAGIC
// MAGIC '''
// MAGIC This code is just to show the syntax and will not run as you need to 
// MAGIC # create an Azur Key Vault and load the credentials as secrets. 
// MAGIC '''
// MAGIC
// MAGIC userid = dbutils.secrets.get(scope = "az-keyvault-databricks", key = "sql-db-user")
// MAGIC dbpassword = dbutils.secrets.get(scope = "az-keyvault-databricks", key = "sql-db-pass")
// MAGIC
// MAGIC print('User ID: ', userid)
// MAGIC print('Password: ', dbpassword)

// COMMAND ----------

// DBTITLE 1,Code Sample 03 - All connection details in clear text - Not Secure!!!
// MAGIC %python
// MAGIC
// MAGIC userid = "bcafferky"
// MAGIC dbpassword = "somepw123?"
// MAGIC jdbcHostname = "sqldbserverbpc.database.windows.net"
// MAGIC jdbcDatabase = "sqldb"
// MAGIC jdbcPort = 1433

// COMMAND ----------

// DBTITLE 1,Code Sample 04 - Build the connection url and properties
// MAGIC %python
// MAGIC
// MAGIC jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
// MAGIC
// MAGIC connectionProperties = {
// MAGIC   "user" : userid,
// MAGIC   "password" : dbpassword,
// MAGIC   "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
// MAGIC }

// COMMAND ----------

// MAGIC %md ### Change Azure SQL server Fire Wall setting to allow Azure Services to access it.

// COMMAND ----------

// DBTITLE 1,Code Sample 05 - Query a table into a dataframe
// MAGIC %python
// MAGIC
// MAGIC spdf_sales = spark.read.jdbc(url=jdbcUrl, table='SalesLT.Customer', properties=connectionProperties)
// MAGIC
// MAGIC display(spdf_sales)

// COMMAND ----------

// DBTITLE 1,Code Sample 06 - Load a SQL query results into a dataframe
// MAGIC %python
// MAGIC
// MAGIC pushdown_query = """(
// MAGIC select oh.CustomerID, oh.OrderDate, oh.Status, oh.SubTotal, oh.Freight, 
// MAGIC        oh.TaxAmt, c.CompanyName, c.SalesPerson
// MAGIC from       SalesLT.SalesOrderHeader oh
// MAGIC inner join SalesLT.Customer          c
// MAGIC on (oh.CustomerID = c.CustomerID)
// MAGIC ) cust"""
// MAGIC
// MAGIC spdf_pushdown = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
// MAGIC
// MAGIC display(spdf_pushdown)

// COMMAND ----------

// DBTITLE 1,Code Sample 07 - Query the database catalog
// MAGIC %python
// MAGIC
// MAGIC display(spark.read.jdbc(url=jdbcUrl, 
// MAGIC                         table="""(SELECT TABLE_SCHEMA, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
// MAGIC                         WHERE TABLE_SCHEMA = 'SalesLT' and TABLE_NAME = 'Customer' ) meta""" , 
// MAGIC                         properties=connectionProperties))