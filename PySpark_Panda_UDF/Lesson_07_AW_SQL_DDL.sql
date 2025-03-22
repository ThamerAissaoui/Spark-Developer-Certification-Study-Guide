-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ### Demonstrate use of SQL Data Definition Language (DDL) Statements
-- MAGIC Databricks Documentation: https://docs.databricks.com/spark/latest/spark-sql/index.html

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Use SQL Data Definition Language (DDL)

-- COMMAND ----------

CREATE DATABASE mydb;

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

DESCRIBE DATABASE mydb;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED mydb;

-- COMMAND ----------

ALTER DATABASE mydb SET DBPROPERTIES (Production=true)

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED mydb

-- COMMAND ----------

CREATE TABLE mytable (name STRING, age int, city STRING)
TBLPROPERTIES ('created.by.user' = 'Bryan', 'created.date' = '02-21-2021');

-- COMMAND ----------

SHOW TBLPROPERTIES mytable;

-- COMMAND ----------

SHOW TBLPROPERTIES mytable (created.by.user);

-- COMMAND ----------

DESCRIBE TABLE EXTENDED mytable;

-- COMMAND ----------

ALTER TABLE mytable RENAME TO mytable2;

-- COMMAND ----------

ALTER TABLE mytable2 ADD columns (title string, DOB timestamp);

-- COMMAND ----------

DESCRIBE TABLE mytable2;

-- COMMAND ----------

ALTER TABLE mytable2  ALTER COLUMN city COMMENT "address city";

-- COMMAND ----------

DESCRIBE TABLE mytable2;

-- COMMAND ----------

ALTER TABLE mytable2 SET TBLPROPERTIES (sensitivedata=false)

-- COMMAND ----------

DESCRIBE TABLE EXTENDED mytable2 

-- COMMAND ----------

SHOW PARTITIONS mytable2;

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

SHOW TBLPROPERTIES mytable2;

-- COMMAND ----------

SHOW CREATE TABLE mytable2

-- COMMAND ----------

SHOW FUNCTIONS

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Clean up database objects...

-- COMMAND ----------

USE mydb;

SHOW TABLES;

-- COMMAND ----------

DROP DATABASE IF EXISTS mydb;

-- COMMAND ----------

DROP TABLE IF EXISTS mytable2;