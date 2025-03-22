# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Spark Developer Certification - Comprehensive Study Guide (python)
# MAGIC ## What is this? <br>
# MAGIC While studying for the [Spark certification exam](https://academy.databricks.com/exams) and going through various resources available online, [I](https://www.linkedin.com/in/mdrakiburrahman/) thought it'd be worthwhile to put together a comprehensive knowledge dump that covers the entire syllabus end-to-end, serving as a Study Guide for myself and hopefully others. <br>
# MAGIC
# MAGIC Note that I used inspiration from the [Spark Code Review guide](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/3249544772526824/521888783067570/7123846766950497/latest.html) - but whereas that covers a subset of the coding aspects only, I aimed for this to be more of a *comprehensive, one stop resource geared towards passing the exam*.
# MAGIC
# MAGIC just for test

# COMMAND ----------

# MAGIC %md
# MAGIC # Awesome Resources/References used throughout this guide <br>
# MAGIC ## References
# MAGIC - **Spark Code Review used for inspiration**: https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/3249544772526824/521888783067570/7123846766950497/latest.html
# MAGIC - **Syllabus**: https://academy.databricks.com/exam/crt020-python
# MAGIC - **Data Scientists Guide to Apache Spark**: https://drive.google.com/open?id=17KMSllwgMvQ8cuvTbcwznnLB6-4Oen9T
# MAGIC - **JVM Overview**: https://www.javaworld.com/article/3272244/what-is-the-jvm-introducing-the-java-virtual-machine.html
# MAGIC - **Spark Runtime Architecture Overview:** https://freecontent.manning.com/running-spark-an-overview-of-sparks-runtime-architecture
# MAGIC - **Spark Application Overview:** https://docs.cloudera.com/documentation/enterprise/5-6-x/topics/cdh_ig_spark_apps.html
# MAGIC - **Spark Architecture Overview:** http://queirozf.com/entries/spark-architecture-overview-clusters-jobs-stages-tasks-etc
# MAGIC - **Mastering Apache Spark:** https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-scheduler-Stage.html
# MAGIC - **Manually create DFs:** https://medium.com/@mrpowers/manually-creating-spark-dataframes-b14dae906393
# MAGIC - **PySpark SQL docs:** https://spark.apache.org/docs/latest/api/python/pyspark.sql.html
# MAGIC - **Introduction to DataFrames:** https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-dataframes-python.html
# MAGIC - **PySpark UDFs:** https://changhsinlee.com/pyspark-udf/
# MAGIC - **ORC File:** https://cwiki.apache.org/confluence/display/Hive/LanguageManual+ORC
# MAGIC - **SQL Server Stored Procedures from Databricks:** https://datathirst.net/blog/2018/10/12/executing-sql-server-stored-procedures-on-databricks-pyspark
# MAGIC - **Repartition vs Coalesce:** https://stackoverflow.com/questions/31610971/spark-repartition-vs-coalesce
# MAGIC - **Partitioning by Columns:** https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-dynamic-partition-inserts.html
# MAGIC - **Bucketing:** https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-bucketing.html
# MAGIC - **PySpark GroupBy and Aggregate Functions:** https://hendra-herviawan.github.io/pyspark-groupby-and-aggregate-functions.html
# MAGIC - **Spark Quickstart:** https://spark.apache.org/docs/latest/quick-start.html
# MAGIC - **Spark Caching - 1:** https://unraveldata.com/to-cache-or-not-to-cache/
# MAGIC - **Spark Caching - 2:** https://stackoverflow.com/questions/45558868/where-does-df-cache-is-stored
# MAGIC - **Spark Caching - 3:** https://changhsinlee.com/pyspark-dataframe-basics/
# MAGIC - **Spark Caching - 4:** http://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence
# MAGIC - **Spark Caching - 5:** https://www.tutorialspoint.com/pyspark/pyspark_storagelevel.htm
# MAGIC - **Spark Caching - 6:** https://spark.apache.org/docs/2.1.2/api/python/_modules/pyspark/storagelevel.html
# MAGIC - **Spark SQL functions examples:** https://spark.apache.org/docs/2.3.0/api/sql/index.html
# MAGIC - **Spark Built-in Higher Order Functions Examples:** https://docs.databricks.com/_static/notebooks/apache-spark-2.4-functions.html
# MAGIC - **Spark SQL Timestamp conversion:** https://docs.databricks.com/_static/notebooks/timestamp-conversion.html
# MAGIC - **RegEx Tutorial:** https://medium.com/factory-mind/regex-tutorial-a-simple-cheatsheet-by-examples-649dc1c3f285
# MAGIC - **Rank VS Dense Rank:** https://stackoverflow.com/questions/44968912/difference-in-dense-rank-and-row-number-in-spark
# MAGIC - **SparkSQL Windows:** https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html
# MAGIC - **Spark Certification Study Guide:** https://github.com/vivek-bombatkar/Databricks-Apache-Spark-2X-Certified-Developer
# MAGIC
# MAGIC ## Resources
# MAGIC #### PySpark Cheatsheet
# MAGIC ![PySpark Cheatshet](https://i.imgur.com/y75QeXH.png)<br>

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Spark Architecture Components
# MAGIC Candidates are expected to be familiar with the following architectural components and their relationship to each other:

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Spark Basic Architecture
# MAGIC A *cluster*, or group of machines, pools the resources of many machines together allowing us to use all the cumulative
# MAGIC resources as if they were one. Now a group of machines sitting somewhere alone is not powerful, you need a framework to coordinate
# MAGIC work across them. **Spark** is a tailor-made engine exactly for this, managing and coordinating the execution of tasks on data across a
# MAGIC cluster of computers. <br>
# MAGIC
# MAGIC The cluster of machines that Spark will leverage to execute tasks will be managed by a cluster manager like Spark’s
# MAGIC Standalone cluster manager, _**YARN**_ - **Y**et **A**nother **R**esource **N**egotiator, or [_**Mesos**_](http://mesos.apache.org/). We then submit Spark Applications to these cluster managers which will
# MAGIC grant resources to our application so that we can complete our work. <br>
# MAGIC
# MAGIC ### Spark Applications
# MAGIC ![Spark Basic Architecture](https://i.imgur.com/kDYSja6.png)<br>
# MAGIC
# MAGIC Spark Applications consist of a **driver** process and a set of **executor** processes. In the illustration we see above, our driver is on the left and four executors on the right. <br>
# MAGIC
# MAGIC ### What is a JVM?
# MAGIC *The JVM manages system memory and provides a portable execution environment for Java-based applications* <br>
# MAGIC
# MAGIC **Technical definition:** The JVM is the specification for a software program that executes code and provides the runtime environment for that code. <br>
# MAGIC **Everyday definition:** The JVM is how we run our Java programs. We configure the JVM's settings and then rely on it to manage program resources during execution. <br>
# MAGIC
# MAGIC The **Java Virtual Machine (JVM)** is a program whose purpose is to execute other programs. <br>
# MAGIC ![JVM](https://i.imgur.com/iZ5TkL2.png)
# MAGIC
# MAGIC The JVM has **two primary functions**: 
# MAGIC  1. To allow Java programs to run on any device or operating system (known as the _"Write once, run anywhere"_ principle)
# MAGIC  2. To manage and optimize program memory <br>
# MAGIC
# MAGIC ### JVM view of the Spark Cluster: *Drivers, Executors, Slots & Tasks*
# MAGIC
# MAGIC The Spark runtime architecture leverages JVMs:<br><br>
# MAGIC ![Spark Physical Cluster, slots](https://files.training.databricks.com/images/105/spark_cluster_slots.png)<br>
# MAGIC
# MAGIC And a slightly more detailed view:<br><br>
# MAGIC ![Spark Physical Cluster, slots](https://freecontent.manning.com/wp-content/uploads/bonaci_runtimeArch_01.png)<br>
# MAGIC
# MAGIC _Elements of a Spark application are in blue boxes and an application’s tasks running inside task slots are labeled with a “T”. Unoccupied task slots are in white boxes._
# MAGIC
# MAGIC #### Responsibilities of the client process component
# MAGIC
# MAGIC The **client** process starts the **driver** program. For example, the client process can be a `spark-submit` script for running applications, a spark-shell script, or a custom application using Spark API (like this Databricks **GUI** - **G**raphics **U**ser **I**nterface). The client process prepares the classpath and all configuration options for the Spark application. It also passes application arguments, if any, to the application running inside the **driver**.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 1A) Driver

# COMMAND ----------

# MAGIC %md
# MAGIC The **driver** orchestrates and monitors execution of a Spark application. There’s always **one driver per Spark application**. You can think of the driver as a wrapper around the application.<br>
# MAGIC
# MAGIC The **driver** process runs our `main()` function, sits on a node in the cluster, and is responsible for: <br>
# MAGIC 1. Maintaining information about the Spark Application
# MAGIC 2. Responding to a user’s program or input
# MAGIC 3. Requesting memory and CPU resources from cluster managers
# MAGIC 4. Breaking application logic into stages and tasks
# MAGIC 5. Sending tasks to executors
# MAGIC 6. Collecting the results from the executors
# MAGIC
# MAGIC The driver process is absolutely essential - it’s the heart of a Spark Application and
# MAGIC maintains all relevant information during the lifetime of the application.
# MAGIC
# MAGIC * The **Driver** is the JVM in which our application runs.
# MAGIC * The secret to Spark's awesome performance is parallelism:
# MAGIC   * Scaling **vertically** (_i.e. making a single computer more powerful by adding physical hardware_) is limited to a finite amount of RAM, Threads and CPU speeds, due to the nature of motherboards having limited physical slots in Data Centers/Desktops.
# MAGIC   * Scaling **horizontally** (_i.e. throwing more identical machines into the Cluster_) means we can simply add new "nodes" to the cluster almost endlessly, because a Data Center can theoretically have an interconnected number of ~infinite machines
# MAGIC * We parallelize at two levels:
# MAGIC   * The first level of parallelization is the **Executor** - a JVM running on a node, typically, **one executor instance per node**.
# MAGIC   * The second level of parallelization is the **Slot** - the number of which is **determined by the number of cores and CPUs of each node/executor**.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 1B) Executor

# COMMAND ----------

# MAGIC %md
# MAGIC The **executors** are responsible for actually executing the work that the **driver** assigns them. This means, each
# MAGIC executor is responsible for only two things:<br>
# MAGIC 1. Executing code assigned to it by the driver
# MAGIC 2. Reporting the state of the computation, on that executor, back to the driver node

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 1C) Cores/Slots/Threads

# COMMAND ----------

# MAGIC %md
# MAGIC * Each **Executor** has a number of **Slots** to which parallelized **Tasks** can be assigned to it by the **Driver**.
# MAGIC   * So for example:
# MAGIC     * If we have **3** identical home desktops (*nodes*) hooked up together in a LAN (like through your home router), each with i7 processors (**8** cores), then that's a **3** node Cluster:
# MAGIC       * **1** Driver node
# MAGIC       * **2** Executor nodes
# MAGIC     * The **8 cores per Executor node** means **8 Slots**, meaning the driver can assign each executor up to **8 Tasks** 
# MAGIC       * The idea is, an i7 CPU Core is manufactured by Intel such that it is capable of executing it's own Task independent of the other Cores, so **8 Cores = 8 Slots = 8 Tasks in parellel**<br>
# MAGIC
# MAGIC _For example: the diagram below is showing 2 Core Executor nodes:_ <br><br>
# MAGIC ![Spark Physical Cluster, tasks](https://files.training.databricks.com/images/105/spark_cluster_tasks.png)<br><br>
# MAGIC
# MAGIC * The JVM is naturally multithreaded, but a single JVM, such as our **Driver**, has a finite upper limit.
# MAGIC * By creating **Tasks**, the **Driver** can assign units of work to **Slots** on each **Executor** for parallel execution.
# MAGIC * Additionally, the **Driver** must also decide how to partition the data so that it can be distributed for parallel processing (see below).
# MAGIC   * Consequently, the **Driver** is assigning a **Partition** of data to each task - in this way each **Task** knows which piece of data it is to process.
# MAGIC   * Once started, each **Task** will fetch from the original data source (e.g. An Azure Storage Account) the **Partition** of data assigned to it.
# MAGIC   
# MAGIC #### Note relating to Tasks, Slots and Cores
# MAGIC You can set the number of task slots to a value two or three times (**i.e. to a multiple of**) the number of CPU cores. Although these task slots are often referred to as CPU cores in Spark, they’re implemented as **threads** that work on a **physical core's thread** and don’t need to correspond to the number of physical CPU cores on the machine (since different CPU manufacturer's can architect multi-threaded chips differently). <br>
# MAGIC
# MAGIC In other words:
# MAGIC * All processors of today have multiple cores (e.g. 1 CPU = 8 Cores)
# MAGIC * Most processors of today are multi-threaded (e.g. 1 Core = 2 Threads, 8 cores = 16 Threads)
# MAGIC * A Spark **Task** runs on a **Slot**. **1 Thread** is capable of doing **1 Task** at a time. To make use of all our threads on the CPU, we cleverly assign the **number of Slots** to correspond to a **multiple of the number of Cores** (which translates to multiple Threads).
# MAGIC   * _By doing this_, after the Driver breaks down a given command (`DO STUFF FROM massive_table`) into **Tasks** and **Partitions**, which are tailor-made to fit our particular Cluster Configuration (say _4 nodes - 1 driver and 3 executors, 8 cores per node, 2 threads per core_). By using our Clusters at maximum efficiency like this (utilizing all available threads), we can get our massive command executed as fast as possible (given our Cluster in this case, _3\*8\*2 Threads --> **48** Tasks, **48** Partitions_ - i.e. **1** Partition per Task)
# MAGIC   * _Say we don't do this_, even with a 100 executor cluster, the entire burden would go to 1 executor, and the other 99 will be sitting idle - i.e. slow execution.
# MAGIC   * _Or say, we instead foolishly assign **49** Tasks and **49** Partitions_, the first pass would execute **48** Tasks in parallel across the executors cores (say in **10 minutes**), then that **1** remaining Task in the next pass will execute on **1** core for another **10 minutes**, while the rest of our **47** cores are sitting idle - meaning the whole job will take double the time at **20 minutes**. This is obviously an inefficient use of our available resources, and could rather be fixed by setting the number of tasks/partitions to a multiple of the number of cores we have (in this setup - 48, 96 etc).  

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 1D) Partitions

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### DataFrames
# MAGIC A DataFrame is the most common Structured API and simply represents a **table of data with _rows_ and _columns_**. The
# MAGIC list of columns and the types in those columns is called **the schema**. <br>
# MAGIC
# MAGIC ![Table Schema](https://blog.brakmic.com/wp-content/uploads/2015/11/print_schema_of_dataframe.png)<br>
# MAGIC
# MAGIC A simple analogy would be a spreadsheet with named columns. The **fundamental difference** is that while a spreadsheet sits on **one computer** in one specific location (e.g. _C:\Users\raki.rahman\Documents\MyFile.csv_), a
# MAGIC Spark DataFrame can span **thousands of computers**. <br>
# MAGIC
# MAGIC The reason for putting the data on more than one computer is intuitive: <br>
# MAGIC - Either _**the data is too large to fit on one machine**_ or it would simply _**take too long to perform that computation on one machine**_.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Partitions
# MAGIC In order to allow every executor to perform work in parallel, Spark breaks up the data into _chunks_, called **partitions**. <br>
# MAGIC ![Partitions](https://i.imgur.com/cLVmK6A.png)<br>
# MAGIC
# MAGIC A **partition** is a collection of rows that sit on one physical machine in our cluster. A DataFrame’s partitions represent how
# MAGIC the data is physically distributed across your cluster of machines during execution:
# MAGIC - If you have _one_ partition, Spark will only have a parallelism of _one_, even if you have thousands of executors. 
# MAGIC - If you have _many_ partitions, but only _one_ executor, Spark will still only have a parallelism of _one_ because there is only one computation resource. <br>
# MAGIC
# MAGIC An important thing to note is that with DataFrames, we do not (for the most part) manipulate partitions manually
# MAGIC (on an individual basis). We simply specify high level transformations of data in the physical partitions and Spark
# MAGIC determines how this work will actually execute on the cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC #### The key points to understand are that:
# MAGIC * Spark employs a **Cluster Manager** that is responsible for provisioning nodes in our cluster.
# MAGIC   * Databricks provides a robust, high-performing **Cluster Manager** as part of its overall offerings.
# MAGIC * In each of these scenarios, the **Driver** is running on one node, with each **Executors** running on N different nodes.
# MAGIC   * Databricks abstracts away the Cluster Management aspects for us (which is a massive pain)
# MAGIC * From a developer's and student's perspective the primary focus is on:
# MAGIC   * The number of **Partitions** the data is divided into
# MAGIC   * The number of **Slots** available for parallel execution
# MAGIC   * How many **Jobs** are being triggered?
# MAGIC   * And lastly the **Stages** those jobs are divided into

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Spark Execution
# MAGIC Candidates are expected to be familiar with Spark’s execution model and the breakdown between the different elements:

# COMMAND ----------

# MAGIC %md
# MAGIC In Spark, the highest-level unit of computation is an **application**. A Spark application can be used for a single batch job, an interactive session with multiple jobs, or a long-lived server continually satisfying requests. <br>
# MAGIC
# MAGIC Spark **application execution**, alongside **drivers** and **executors**, also involves runtime concepts such as **tasks**, **jobs**, and **stages**. Invoking an **action** inside a Spark application triggers the launch of a **job** to fulfill it. Spark examines the dataset on which that action depends and formulates an **execution plan**. The **execution plan** assembles the dataset transformations into **stages**. A **stage** is a collection of tasks that run the same code, each on a different subset of the data. <br>
# MAGIC
# MAGIC ![Execution Model](https://docs.cloudera.com/documentation/enterprise/5-6-x/images/spark-tuning-f1.png)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Overview of DAGSchedular
# MAGIC
# MAGIC **DAGScheduler** is the scheduling layer of Apache Spark that implements stage-oriented scheduling. It transforms a _logical_ execution plan to a _physical_ execution plan (using stages).<br>
# MAGIC
# MAGIC ![DAGScheduler](https://i.imgur.com/w6BmKRq.png)<br>
# MAGIC
# MAGIC After an **action** (see below) has been called, `SparkContext` hands over a logical plan to **DAGScheduler** that it in turn translates to a set of **stages** that are submitted as a set of **tasks** for execution.
# MAGIC
# MAGIC The fundamental concepts of **DAGScheduler** are **jobs** and **stages** that it tracks through internal registries and counters.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 2A) Jobs

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC A **Job** is a sequence of **stages** and a stage is a collection of **tasks** that run the same code, triggered by an **action** such as `count()`, `collect()`, `read()` or `write()`. <br><br>
# MAGIC
# MAGIC * Each parallelized action is referred to as a **Job**.
# MAGIC * The results of each **Job** (parallelized/distributed action) is returned to the **Driver** from the **Executor**.
# MAGIC * Depending on the work required, multiple **Jobs** will be required.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 2B) Stages

# COMMAND ----------

# MAGIC %md
# MAGIC _Each **job** that gets divided into smaller **sets of tasks** is a **stage**._<br>
# MAGIC
# MAGIC A **Stage** is a sequence of **Tasks** that can all be run together - i.e. in parallel - without a **shuffle**. For example: using `.read` to read a file from disk, then runnning `.filter` can be done without a **shuffle**, so it can fit in a single **stage**. The number of **Tasks** in a **Stage** also depends upon the number of **Partitions** your datasets have. <br><br>
# MAGIC
# MAGIC ![Job Stages](https://i.imgur.com/wCMLYrH.png)<br>
# MAGIC
# MAGIC * Each **Job** is broken down into **Stages**.
# MAGIC * This would be analogous to building *a house* (the job) - attempting to do any of these steps out of order doesn't make sense:
# MAGIC   1. Lay the foundation
# MAGIC   2. Erect the walls
# MAGIC   3. Add the rooms<br>
# MAGIC   
# MAGIC ![Spark Stages](https://i.imgur.com/PLkW7S8.png)<br>
# MAGIC   
# MAGIC In other words: 
# MAGIC - A **stage** is a step in a physical execution plan - a physical unit of the execution plan 
# MAGIC - A **stage** is a set of parallel **tasks - one task per partition - the blue boxes on the right of the diagram above** (of an RDD that computes _partial_ results of a function executed as part of a Spark job).
# MAGIC - A Spark **job** is a computation with that computation sliced into **stages**
# MAGIC - A **stage** is uniquely identified by `id`. When a **stage** is created, **DAGScheduler** increments internal counter `nextStageId` to track the number of stage submissions.
# MAGIC - Each **stage** contains a sequence of **narrow transformations** (see below) that can be completed without **shuffling** the entire data set, separated at _shuffle boundaries_, i.e. where shuffle occurs. **Stages** are thus a result of breaking the RDD at shuffle boundaries.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### An example of Stages - _Inefficient_
# MAGIC
# MAGIC * When we shuffle data, it creates what is known as a _stage boundary_.
# MAGIC * Stage boundaries represent a **process bottleneck**.
# MAGIC
# MAGIC Take for example the following transformations:
# MAGIC
# MAGIC |Step |Transformation|
# MAGIC |----:|--------------|
# MAGIC | 1   | `Read`    |
# MAGIC | 2   | `Select`  |
# MAGIC | 3   | `Filter`  |
# MAGIC | 4   | `GroupBy` |
# MAGIC | 5   | `Select`  |
# MAGIC | 6   | `Filter`  |
# MAGIC | 7   | `Write`   |
# MAGIC
# MAGIC Spark will break this one job into two stages (steps _1-4b_ and steps _4c-8_):
# MAGIC
# MAGIC **Stage #1**
# MAGIC
# MAGIC |Step |Transformation|
# MAGIC |----:|--------------|
# MAGIC | 1   | `Read` |
# MAGIC | 2   | `Select` |
# MAGIC | 3   | `Filter` |
# MAGIC | 4a | `GroupBy` _**1**/2_ |
# MAGIC | 4b | `shuffle write` |
# MAGIC
# MAGIC **Stage #2**
# MAGIC
# MAGIC |Step |Transformation|
# MAGIC |----:|--------------|
# MAGIC | 4c | `shuffle read` |
# MAGIC | 4d | `GroupBy`  _**2**/2_ |
# MAGIC | 5   | `Select` |
# MAGIC | 6   | `Filter` |
# MAGIC | 7   | `Write` |
# MAGIC
# MAGIC In **Stage #1**, Spark will create a **pipeline of transformations** in which the data is read into RAM (Step #1), and then perform steps #2, #3, #4a & #4b
# MAGIC
# MAGIC All partitions must complete **Stage #1** before continuing to **Stage #2**
# MAGIC * It's not possible to `Group` all records across all partitions until every task is completed.
# MAGIC * This is the point at which all the tasks (across the executor slots) must synchronize.
# MAGIC * This creates our **bottleneck**.
# MAGIC * Besides the bottleneck, this is also a significant **performance hit**: _disk IO, network IO and more disk IO_.
# MAGIC
# MAGIC Once the data is shuffled, we can resume execution.
# MAGIC
# MAGIC For **Stage #2**, Spark will again create a pipeline of transformations in which the shuffle data is read into RAM (Step #4c) and then perform transformations #4d, #5, #6 and finally the write action, step #7.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 2C) Tasks

# COMMAND ----------

# MAGIC %md
# MAGIC ![Spark Task](https://i.imgur.com/HcyLVyw.png)<br>
# MAGIC
# MAGIC A **task** is a unit of work that is sent to the executor. Each **stage** has some tasks, one **task per partition**. The same task is done over different partitions of the RDD.
# MAGIC
# MAGIC In the _example of Stages_ above, each **Step** is a **Task**.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### An example of Stages - _Efficient_
# MAGIC
# MAGIC #### Working Backwards
# MAGIC From the developer's perspective, we start with a read and conclude (in this case) with a write
# MAGIC
# MAGIC |Step |Transformation|
# MAGIC |----:|--------------|
# MAGIC | 1   | `Read`    |
# MAGIC | 2   | `Select`  |
# MAGIC | 3   | `Filter`  |
# MAGIC | 4   | `GroupBy` |
# MAGIC | 5   | `Select`  |
# MAGIC | 6   | `Filter`  |
# MAGIC | 7   | `Write`   |
# MAGIC
# MAGIC However, Spark starts backwards with the **action** (`write(..)` in this case).
# MAGIC
# MAGIC Next, it asks the question, **what do I need to do first**?
# MAGIC
# MAGIC It then proceeds to determine which transformation precedes this step until it identifies the first transformation.
# MAGIC
# MAGIC |Step |Transformation|Dependencies |
# MAGIC |----:|--------------|-|
# MAGIC | 7   | `Write`   | Depends on #6 |
# MAGIC | 6   | `Filter`  | Depends on #5 |
# MAGIC | 5   | `Select`  | Depends on #4 |
# MAGIC | 4   | `GroupBy` | Depends on #3 |
# MAGIC | 3   | `Filter` | Depends on #2 |
# MAGIC | 2   | `Select`  | Depends on #1 |
# MAGIC | 1   | `Read`    | First |
# MAGIC
# MAGIC This would be equivalent to understanding your own lineage.
# MAGIC * You don't ask if you are related to _Genghis Khan_ and then work through the ancestry of all his children (5% of all people in Asia).
# MAGIC  * You start with your mother.
# MAGIC  * Then your grandmother
# MAGIC  * Then your great-grandmother
# MAGIC  * ... and so on
# MAGIC  * Until you discover you are actually related to _Catherine Parr, the last queen of Henry the VIII_.
# MAGIC  
# MAGIC #### Why Work Backwards?
# MAGIC **Question:** So what is the benefit of working backward through your action's lineage?<br>
# MAGIC **Answer:** It allows Spark to determine if it is necessary to execute every transformation.
# MAGIC
# MAGIC Take another look at our example:
# MAGIC * Say we've executed this once already
# MAGIC * On the first execution, **Step #4** resulted in a shuffle
# MAGIC * Those shuffle files are on the various **executors** already (src & dst)
# MAGIC * Because the **transformations** (or DataFrames) are **immutable**, no aspect of our lineage can change (meaning that DataFrame is sitting on a chunk of the executor's RAM from the last time it was calculated, ready to be referenced again).
# MAGIC * That means the results of our _previous_ **shuffle** (if still available) can be reused.
# MAGIC
# MAGIC |Step |Transformation| |
# MAGIC |----:|--------------|-|
# MAGIC | 7   | `Write`   | Depends on #6 |
# MAGIC | 6   | `Filter`  | Depends on #5 |
# MAGIC | 5   | `Select`  | Depends on #4 |
# MAGIC | 4   | `GroupBy` | <<< shuffle |
# MAGIC | 3   | `Filter`  | *don't care* |
# MAGIC | 2   | `Select`  | *don't care* |
# MAGIC | 1   | `Read`    | *don't care* |
# MAGIC
# MAGIC In this case, what we end up executing is only the operations from **Stage #2**.
# MAGIC
# MAGIC This saves us the initial network read and all the transformations in **Stage #1**
# MAGIC
# MAGIC |Step |Transformation|   |
# MAGIC |----:|---------------|:-:|
# MAGIC | 1   | `Read`          | *skipped* |
# MAGIC | 2   | `Select`        | *skipped* |
# MAGIC | 3   | `Filter`        | *skipped* |
# MAGIC | 4a  | `GroupBy` _**1**/2_   | *skipped* |
# MAGIC | 4b  | `shuffle write` | *skipped* |
# MAGIC | 4c  | `shuffle read`  | - |
# MAGIC | 4d  | `GroupBy`  _**2**/2_  | - |
# MAGIC | 5   | `Select`        | - |
# MAGIC | 6   | `Filter`        | - |
# MAGIC | 7   | `Write`         | - |

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Summary: Jobs, Stages, Tasks
# MAGIC
# MAGIC #### Jobs
# MAGIC - Highest element of Spark’s execution hierarchy.
# MAGIC - Each Spark job corresponds to one **Action** <br>
# MAGIC
# MAGIC #### Stages
# MAGIC - As mentioned above, a job is defined by calling an action.
# MAGIC - The action may include several **transformations**, which breaks down **jobs** into **stages**.
# MAGIC - Several **transformations** with _narrow_ dependencies can be grouped into one stage
# MAGIC - It is possible to execute **stages** in _parallel_ if they are used to compute different RDDs
# MAGIC - _Wide_ **transformations** that are needed to compute one RDD have to be computed in sequence
# MAGIC - One **stage** can be computed without moving data across the partitions
# MAGIC - Within one **stage**, the **tasks** are the unit of work done for each partition of the data
# MAGIC
# MAGIC #### Tasks
# MAGIC - A **stage** consists of **tasks**
# MAGIC - The **task** is the smallest unit in the execution hierarchy
# MAGIC - Each **task** can represent one local computation
# MAGIC - One **task** cannot be executed on more than one **executor**
# MAGIC - However, each **executor** has a dynamically allocated number of **slots** for running **tasks**
# MAGIC - The number of **tasks** per **stage** corresponds to the number of partitions in the output RDD of that **stage**

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Spark Concepts
# MAGIC Candidates are expected to be familiar with the following concepts:

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 3A) Caching
# MAGIC
# MAGIC The reuse of shuffle files (aka our temp files) is just one example of Spark optimizing queries anywhere it can.
# MAGIC
# MAGIC We cannot assume this will be available to us.
# MAGIC
# MAGIC **Shuffle files** are by _definition_ **temporary files** and will eventually be removed.
# MAGIC
# MAGIC However, we can cache data to **explicitly** to accomplish the same thing that happens inadvertently (i.e. we get _lucky_) with shuffle files.
# MAGIC
# MAGIC In this case, the lineage plays the same role. Take for example:
# MAGIC
# MAGIC |Step |Transformation| |
# MAGIC |----:|--------------|-|
# MAGIC | 7   | `Write`   | Depends on #6 |
# MAGIC | 6   | `Filter`  | Depends on #5 |
# MAGIC | 5   | `Select`  | <<< cache |
# MAGIC | 4   | `GroupBy` | <<< shuffle files |
# MAGIC | 3   | `Filter`  | ? |
# MAGIC | 2   | `Select`  | ? |
# MAGIC | 1   | `Read`    | ? |
# MAGIC
# MAGIC In this case we **explicitly asked Spark to cache** the DataFrame resulting from the `select(..)` in Step 5 (after the shuffle across the network due to `GroupBy`.
# MAGIC
# MAGIC As a result, we never even get to the part of the lineage that involves the shuffle, let alone **Stage #1** (i.e. we skip the whole thing, making our job execute faster).
# MAGIC
# MAGIC Instead, we pick up with the cache and resume execution from there:
# MAGIC
# MAGIC |Step |Transformation|   |
# MAGIC |----:|---------------|:-:|
# MAGIC | 1   | `Read`          | *skipped* |
# MAGIC | 2   | `Select`        | *skipped* |
# MAGIC | 3   | `Filter`        | *skipped* |
# MAGIC | 4a  | `GroupBy` _**1**/2_   | *skipped* |
# MAGIC | 4b  | `shuffle write` | *skipped* |
# MAGIC | 4c  | `shuffle read`  | *skipped* |
# MAGIC | 4d  | `GroupBy`  _**2**/2_  | *skipped* |
# MAGIC | 5a  | `cache read`    | - |
# MAGIC | 5b  | `Select`        | - |
# MAGIC | 6   | `Filter`        | - |
# MAGIC | 7   | `Write`         | - |

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 3B) Shuffling

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC A Shuffle refers to an operation where data is _re-partitioned_ across a **Cluster** - i.e. when data needs to move between executors.
# MAGIC
# MAGIC `join` and any operation that ends with `ByKey` will trigger a **Shuffle**. It is a costly operation because a lot of data can be sent via the network.
# MAGIC
# MAGIC For example, to group by color, it will serve us best if...
# MAGIC   * All the reds are in one partitions
# MAGIC   * All the blues are in a second partition
# MAGIC   * All the greens are in a third
# MAGIC
# MAGIC From there we can easily sum/count/average all of the reds, blues, and greens.
# MAGIC
# MAGIC To carry out the shuffle operation Spark needs to
# MAGIC * Convert the data to the UnsafeRow (if it isn't already), commonly refered to as **Tungsten Binary Format**.
# MAGIC   * **Tungsten** is a new Spark SQL component that provides more efficient Spark operations by working directly at the byte level.
# MAGIC   * Includes specialized in-memory data structures tuned for the type of operations required by Spark
# MAGIC   * Improved code generation, and a specialized wire protocol.
# MAGIC * Write that data to disk on the **local node** - at this point the slot is free for the next task.
# MAGIC * Send that data across the **network** to another **executor**
# MAGIC   * **Driver** decides which **executor** gets which partition of the data.
# MAGIC   * Then the **executor** pulls the data it needs from the other executor's shuffle files.
# MAGIC * Copy the data back into **RAM** on the new executor
# MAGIC   * The concept, if not the action, is just like the initial read "every" `DataFrame` starts with.
# MAGIC   * The main difference being it's the 2nd+ stage.
# MAGIC
# MAGIC This amounts to a free cache from what is effectively temp files.
# MAGIC
# MAGIC ** *Note:* ** *Some actions induce in a shuffle.*<br/>
# MAGIC *Good examples would include the operations `count()` and `reduce(..)`.*

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 3C) Partitioning

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC A **Partition** is a logical chunk of your **DataFrame**
# MAGIC
# MAGIC Data is split into **Partitions** so that each **Executor** can operate on a single part, enabling **parallelization**.
# MAGIC
# MAGIC It can be processed by a **single Executor core/thread**.
# MAGIC
# MAGIC For example: If you have **4** data partitions and you have **4** executor cores/threads, you can process everything in parallel, in a single pass.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 3D) DataFrame Transformations vs. Actions vs. Operations

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC Spark allows two distinct kinds of operations by the user: **transformations** and **actions**. <br>
# MAGIC
# MAGIC _Transformations are **LAZY**, Actions are **EAGER**_<br>
# MAGIC
# MAGIC ### Transformations - Overview
# MAGIC
# MAGIC **Transformations** are operations that will not be completed at the time you write and execute the code in a cell (they're **lazy**) - they will only get executed once you have called an **action**. An example of a transformation might be to convert an `integer` into a `float` or to `filter` a set of values: i.e. they can be procrastinated and don't have to be done _right now_ - but later after we have a full view of the task at hand.
# MAGIC
# MAGIC *Here's an analogy:*
# MAGIC * Let's say you're cleaning your closet, and want to donate clothes that don't fit (there's a lot of these starting from childhood days), and sort out and store the rest by color before storing in your closet.
# MAGIC * If you're **inefficient**, you could sort out all the clothes by color (let's say that takes *60 minutes*), then from there pick the ones that fit (*5 minutes*), and then take the rest and put it into one big plastic bag for donation (where all that sorting effort you did went to waste because it's all jumbled up in the same plastic bag now anyway)
# MAGIC * If you're **efficient**, you'd first pick out clothes that fit very quickly (*5 minutes*), then sort those into colors (*10 minutes*), and then take the rest and put it into one big plastic bag for donation (where there's no wasted effort)
# MAGIC
# MAGIC In other words, by evaluating the full view of the **job** at hand, and by being **lazy** (not in the traditional sense - but in the smart way by **not eagerly** sorting everything by color for no reason), you were able to achieve the same goal in *15 minutes* vs *65 minutes* (clothes that fit are sorted by color in the closet, clothes that don' fit are in plastic bag).
# MAGIC
# MAGIC ### Actions - Overview
# MAGIC
# MAGIC **Actions** are commands that are computed by Spark right at the time of their execution (they're **eager**). They consist of running all of the previous transformations in order to get back an actual result. An **action** is composed of one or more **jobs** which consists of **tasks** that will be executed by the **executor slots** in parallel - i.e. a **stage** - where possible.
# MAGIC
# MAGIC Here are some simple examples of transformations and actions. 
# MAGIC
# MAGIC ![transformations and actions](http://training.databricks.com/databricks_guide/gentle_introduction/trans_and_actions.png)
# MAGIC
# MAGIC Spark `pipelines` a computation as we can see in the image below. This means that certain computations can all be performed at once (like a `map` and a `filter`) rather than having to do one operation for all pieces of data, and then the following operation.
# MAGIC
# MAGIC ![transformations and actions](http://training.databricks.com/databricks_guide/gentle_introduction/pipeline.png)
# MAGIC
# MAGIC ### Why is Laziness So Important?
# MAGIC
# MAGIC It has a number of benefits:
# MAGIC * Not forced to load all data at step #1
# MAGIC   * Technically impossible with **REALLY** large datasets.
# MAGIC * Easier to parallelize operations
# MAGIC   * _N_ different transformations can be processed on a single data element, on a single thread, on a single machine.
# MAGIC * Most importantly, it allows the framework to automatically apply various optimizations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Actions
# MAGIC
# MAGIC Transformations always return a `DataFrame`.
# MAGIC
# MAGIC In contrast, Actions either return a _result_ or _write to disk_. For example:
# MAGIC * The number of records in the case of `count()`
# MAGIC * An array of objects in the case of `collect()` or `take(n)`
# MAGIC
# MAGIC We've seen a good number of the actions - most of them are listed below.
# MAGIC
# MAGIC For the complete list, one needs to review the API docs.
# MAGIC
# MAGIC | Method | Return | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | `collect()` | Collection | Returns an array that contains all of Rows in this Dataset. |
# MAGIC | `count()` | Long | Returns the number of rows in the Dataset. |
# MAGIC | `first()` | Row | Returns the first row. |
# MAGIC | `foreach(f)` | - | Applies a function f to all rows. |
# MAGIC | `foreachPartition(f)` | - | Applies a function f to each partition of this Dataset. |
# MAGIC | `head()` | Row | Returns the first row. |
# MAGIC | `reduce(f)` | Row | Reduces the elements of this Dataset using the specified binary function. |
# MAGIC | `show(..)` | - | Displays the top 20 rows of Dataset in a tabular form. |
# MAGIC | `take(n)` | Collection | Returns the first n rows in the Dataset. |
# MAGIC | `toLocalIterator()` | Iterator | Return an iterator that contains all of Rows in this Dataset. |
# MAGIC
# MAGIC ** *Note:* ** *The databricks command `display(..)` is not included here because it's not part of the Spark API, even though it ultimately calls an action. *

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Transformations
# MAGIC
# MAGIC Transformations have the following key characteristics:
# MAGIC * They eventually return another `DataFrame`.
# MAGIC * DataFrames are **immutable** - that is each instance of a `DataFrame` cannot be altered once it's instantiated.
# MAGIC   * This means other optimizations are possible - such as the use of shuffle files (see below)
# MAGIC * Are classified as either a **Wide** or **Narrow** transformation
# MAGIC
# MAGIC ** *Note:* ** The list of transformations varies significantly between each language - because Java & Scala are _strictly_ typed languages compared Python & R which are _loosely_ typed.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Pipelining Operations
# MAGIC <img src="https://files.training.databricks.com/images/pipelining-2.png" style="float: right"/>
# MAGIC
# MAGIC <ul>
# MAGIC   <li>Pipelining is the idea of executing as many **operations** as possible on a single partition of data.</li>
# MAGIC   <li>Once a single partition of data is read into RAM, Spark will combine as many **narrow operations** as it can into a single **task**</li>
# MAGIC   <li>**Wide operations** force a shuffle, conclude, a stage and end a pipeline.</li>
# MAGIC   <li>Compare to MapReduce where: </li>
# MAGIC   <ol>
# MAGIC     <li>Data is read from disk</li>
# MAGIC     <li>A single transformation takes place</li>
# MAGIC     <li>Data is written to disk</li>
# MAGIC     <li>Repeat steps 1-3 until all transformations are completed</li>
# MAGIC   </ol>
# MAGIC   <li>By avoiding all the extra network and disk IO, Spark can easily out perform traditional MapReduce applications.</li>
# MAGIC </ul>

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 3E) Wide vs. Narrow Transformations

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Wide vs. Narrow Transformations
# MAGIC
# MAGIC Regardless of language, transformations break down into two broad categories: **wide** and **narrow**.
# MAGIC
# MAGIC **Narrow Transformations**: The data required to compute the records in a single partition reside in at most one partition of the parent RDD.
# MAGIC
# MAGIC Examples include:
# MAGIC * `filter(..)`
# MAGIC * `drop(..)`
# MAGIC * `coalesce()`
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/105/transformations-narrow.png" alt="Narrow Transformations" style="height:300px"/>
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC **Wide Transformations**: The data required to compute the records in a single partition may reside in many partitions of the parent RDD.
# MAGIC
# MAGIC Examples include:
# MAGIC * `distinct()`
# MAGIC * `groupBy(..).sum()`
# MAGIC * `repartition(n)`
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/105/transformations-wide.png" alt="Wide Transformations" style="height:300px"/>

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 3F) High-level Cluster Configuration

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Spark cluster types
# MAGIC
# MAGIC Spark can run in **local mode** (on your laptop) and inside **Spark standalone**, **YARN**, and **Mesos** clusters. Although Spark runs on all of them, one might be more applicable for your environment and use cases. In this section, you’ll find the pros and cons of each cluster type.
# MAGIC
# MAGIC ##### Spark local modes
# MAGIC
# MAGIC Spark local mode and Spark local cluster mode are special cases of a Spark standalone cluster running on a single machine. Because these cluster types are easy to set up and use, they’re convenient for quick tests, but they shouldn’t be used in a production environment.
# MAGIC
# MAGIC Furthermore, in these local modes, the workload isn’t distributed, and it creates the resource restrictions of a single machine and suboptimal performance. True high availability isn’t possible on a single machine, either.
# MAGIC
# MAGIC ##### Spark standalone cluster
# MAGIC
# MAGIC A Spark standalone cluster is a Spark-specific cluster. Because a standalone cluster is built specifically for Spark applications, it doesn’t support communication with an HDFS secured with Kerberos authentication protocol. If you need that kind of security, use YARN for running Spark. <br>
# MAGIC
# MAGIC ##### YARN cluster
# MAGIC
# MAGIC **YARN** is Hadoop’s resource manager and execution system. It’s also known as _MapReduce 2_ because it superseded the _MapReduce_ engine in _Hadoop 1_ that supported only MapReduce jobs.
# MAGIC
# MAGIC Running Spark on YARN has several advantages:
# MAGIC - Many organizations already have YARN clusters of a significant size, along with the technical know-how, tools, and procedures for managing and monitoring them.
# MAGIC - Furthermore, YARN lets you run different types of Java applications, not only Spark, and you can mix legacy Hadoop and Spark applications with ease.
# MAGIC - YARN also provides methods for isolating and prioritizing applications among users and organizations, a functionality the standalone cluster doesn’t have.
# MAGIC - It’s the only cluster type that supports **Kerberos-secured HDFS**.
# MAGIC - Another advantage of YARN over the standalone cluster is that you don’t have to install Spark on every node in the cluster.
# MAGIC
# MAGIC ##### Mesos cluster
# MAGIC
# MAGIC **Mesos** is a scalable and fault-tolerant “distributed systems kernel” written in C++. Running Spark in a Mesos cluster also has its advantages. Unlike YARN, Mesos also supports C++ and Python applications,  and unlike YARN and a standalone Spark cluster that only schedules memory, Mesos provides scheduling of other types of resources (for example, CPU, disk space and ports), although these additional resources aren’t used by Spark currently. Mesos has some additional options for job scheduling that other cluster types don’t have (for example, fine-grained mode).
# MAGIC
# MAGIC And, Mesos is a “scheduler of scheduler frameworks” because of its two-level scheduling architecture. The jury’s still out on which is better: YARN or Mesos; but now, with the [Myriad project](http://myriad.incubator.apache.org/),  you can run YARN on top of Mesos to solve the dilemma.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # *DataFrames API*
# MAGIC Candidates are expected to have a command of the following APIs.

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. SparkContext

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overview
# MAGIC Once the **driver** is started, it configures an instance of `SparkContext`. Your Spark context is already preconfigured and available as the variable `sc`. When running a standalone Spark application by submitting a jar file, or by using Spark API from another program, your Spark application starts and configures the Spark context (i.e. Databricks). <br>
# MAGIC
# MAGIC **Note:** There is usually one Spark context per JVM. Although the configuration option `spark.driver.allowMultipleContexts` exists, it’s misleading because usage of multiple Spark contexts is discouraged. This option is used only for Spark internal tests and we recommend you don’t use that option in your user programs. If you do, you may get unexpected results while running more than one Spark context in a single JVM. <br>
# MAGIC
# MAGIC A Spark context comes with many useful methods for creating DataFrames, loading data (e.g. `spark.read.format("csv")`), and is the main interface for accessing Spark runtime.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 4A) `SparkContext` to control basic configuration settings such as `spark.sql.shuffle.partitions`.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Definitions
# MAGIC **`spark.sql.shuffle.partitions`**: Configures the number of partitions to use when shuffling data for joins or aggregations. <br>
# MAGIC **`spark.executor.memory`**: Amount of memory to use per executor process, in the same format as JVM memory strings with a size unit suffix ("k", "m", "g" or "t") (e.g. 512m, 2g). <br>
# MAGIC **`spark.default.parallelism`**: Default number of partitions in RDDs returned by transformations like `join`, `reduceByKey`, and `parallelize` when not set by user. Note that this is __ignored for DataFrames__, and we can use `df.repartition(numOfPartitions)` instead. <br>
# MAGIC
# MAGIC Let's set the value of `spark.sql.shuffle.partitions` and `spark.executor.memory` using PySpark and SQL syntax.

# COMMAND ----------

# Print the default values of shuffle partition and the executor memory
print(spark.conf.get("spark.sql.shuffle.partitions"), ",", spark.conf.get("spark.executor.memory"))

# COMMAND ----------

# Set the number of shuffle partitions to 6
spark.conf.set("spark.sql.shuffle.partitions", 6)
# Set the memory of executors to 2 GB
spark.conf.set("spark.executor.memory", "2g")
# Print the values of the shuffle partition and the executor memory
print(spark.conf.get("spark.sql.shuffle.partitions"), ",", spark.conf.get("spark.executor.memory"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.sql.shuffle.partitions = 200;

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.executor.memory = 7284m;

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. SparkSession
# MAGIC Candidates are expected to know how to:

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 5A) Create a *DataFrame/Dataset* from a collection (e.g. `list` or `set`)

# COMMAND ----------

# import relevant modules
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import *
from pyspark import StorageLevel
import sys

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Example: Create DataFrame from `list` with `DataType` specified

# COMMAND ----------

list_df = spark.createDataFrame([1, 2, 3, 4], IntegerType())
display(list_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Example: Create DataFrame from `Row`
# MAGIC _class_ `pyspark.sql.Row`<br>
# MAGIC A row in `DataFrame`. The fields in it can be accessed:
# MAGIC - like attributes (`row.key`)
# MAGIC - like dictionary values (`row[key]`) <br>
# MAGIC
# MAGIC In this scenario we have two tables to be joined `employee` and `department`. Both tables contains only a few records, but we need to join them to get to know the department of each employee. So, we join them using Spark DataFrames like this:

# COMMAND ----------

# Create Example Data - Departments and Employees

# Create the Employees
Employee = Row("name") # Define the Row `Employee' with one column/key
employee1 = Employee('Bob') # Define against the Row 'Employee'
employee2 = Employee('Sam') # Define against the Row 'Employee'

# Create the Departments
Department = Row("name", "department") # Define the Row `Department' with two columns/keys
department1 = Department('Bob', 'Accounts') # Define against the Row 'Department'
department2 = Department('Alice', 'Sales') # Define against the Row 'Department'
department3 = Department('Sam', 'HR') # Define against the Row 'Department'

# Create DataFrames from rows
employeeDF = spark.createDataFrame([employee1, employee2]) 
departmentDF = spark.createDataFrame([department1, department2, department3])

# Join employeeDF to departmentDF on "name"
display(employeeDF.join(departmentDF, "name"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Example: Create DataFrame from `Row`, with Schema specified
# MAGIC `createDataFrame`(_data, schema=None, samplingRatio=None, verifySchema=True_) <br>
# MAGIC Creates a `DataFrame` from an `RDD`, a `list` or a `pandas.DataFrame`.
# MAGIC When `schema` is a list of column names, the type of each column will be inferred from `data`.
# MAGIC When `schema` is `None`, it will try to infer the schema (column names and types) from data, which should be an RDD of `Row`, or `namedtuple`, or `dict`.

# COMMAND ----------

schema = StructType([
  StructField("letter", StringType(), True),
  StructField("position", IntegerType(), True)])

df = spark.createDataFrame([('A', 0),('B', 1),('C', 2)], schema)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example: Create DataFrame from a list of `Rows`

# COMMAND ----------

# Create Example Data - Departments and Employees

# Create the Departments
Department = Row("id", "name")
department1 = Department('123456', 'Computer Science')
department2 = Department('789012', 'Mechanical Engineering')
department3 = Department('345678', 'Theater and Drama')
department4 = Department('901234', 'Indoor Recreation')
department5 = Department('000000', 'All Students')

# Create the Employees
Employee = Row("firstName", "lastName", "email", "salary")
employee1 = Employee('michael', 'armbrust', 'no-reply@berkeley.edu', 100000)
employee2 = Employee('xiangrui', 'meng', 'no-reply@stanford.edu', 120000)
employee3 = Employee('matei', None, 'no-reply@waterloo.edu', 140000)
employee4 = Employee(None, 'wendell', 'no-reply@berkeley.edu', 160000)
employee5 = Employee('michael', 'jackson', 'no-reply@neverla.nd', 80000)

# Create the DepartmentWithEmployees instances from Departments and Employees
DepartmentWithEmployees = Row("department", "employees")
departmentWithEmployees1 = DepartmentWithEmployees(department1, [employee1, employee2])
departmentWithEmployees2 = DepartmentWithEmployees(department2, [employee3, employee4])
departmentWithEmployees3 = DepartmentWithEmployees(department3, [employee5, employee4])
departmentWithEmployees4 = DepartmentWithEmployees(department4, [employee2, employee3])
departmentWithEmployees5 = DepartmentWithEmployees(department5, [employee1, employee2, employee3, employee4, employee5])

print(department1)
print(employee2)
print(departmentWithEmployees1.employees[0].email)

# COMMAND ----------

departmentsWithEmployeesSeq1 = [departmentWithEmployees1, departmentWithEmployees2, departmentWithEmployees3, departmentWithEmployees4, departmentWithEmployees5]
df1 = spark.createDataFrame(departmentsWithEmployeesSeq1)

display(df1)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 5B) Create a *DataFrame* for a range of numbers

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC `range`(_start, end=None, step=1, numPartitions=None_)<br>
# MAGIC Create a `DataFrame` with single `pyspark.sql.types.LongType` column named `id`, containing elements in a range from start to end (exclusive) with step value `step`.
# MAGIC
# MAGIC **Parameters**
# MAGIC * __start__ – the start value
# MAGIC * __end__ – the end value (exclusive)
# MAGIC * __step__ – the incremental step (default: 1)
# MAGIC * __numPartitions__ – the number of partitions of the DataFrame
# MAGIC
# MAGIC **Returns**:
# MAGIC * __`DataFrame`__

# COMMAND ----------

df = spark.range(1,8,2).toDF("number")
display(df)

# COMMAND ----------

df = spark.range(5).toDF("number")
display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 5C) Access the *DataFrameReaders*

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### DataFrameReader — Loading Data From External Data Sources<br>
# MAGIC class `pyspark.sql.DataFrameReader`(spark)<br>
# MAGIC Interface used to load a `DataFrame` from external storage systems (e.g. file systems, key-value stores, etc). Use `spark.read()` to access this.<br>
# MAGIC
# MAGIC Before we start with using `DataFrameReaders`, let's mount a storage container that will contain different files for us to use.

# COMMAND ----------

# Localize with Storage Account name and key
storageAccountName = "databrickscertifaccount"
storageAccountAccessKey = "9wmjrV6pQdrfntDZ/A6Jikd941Kmi5mpOHzGPgmN+SyFWoZs3xicObcbtDCtCb3O4bwCmnV3zp1w+AStGxDWiQ=="

# Function to mount a storage container
def mountStorageContainer(storageAccount, storageAccountKey, storageContainer, blobMountPoint):
  print("Mounting {0} to {1}:".format(storageContainer, blobMountPoint))
  # Attempt mount only if the storage container is not already mounted at the mount point
  if not any(mount.mountPoint == blobMountPoint for mount in dbutils.fs.mounts()):
    print("....Container is not mounted; Attempting mounting now..")
    mountStatus = dbutils.fs.mount(
                  source = "wasbs://{0}@{1}.blob.core.windows.net/".format(storageContainer, storageAccount),
                  mount_point = blobMountPoint,
                  extra_configs = {"fs.azure.account.key.{0}.blob.core.windows.net".format(storageAccount): storageAccountKey})
    print("....Status of mount is: " + str(mountStatus))
  else:
    print("....Container is already mounted.")
    print() # Provide a blank line between mounts
    
# Mount "bronze" storage container
mountStorageContainer(storageAccountName,storageAccountAccessKey,"bronze","/mnt/GoFast/bronze")

# Display directory
display(dbutils.fs.ls("/mnt/GoFast/bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We read in the `auto-mpg.csv` with a simple `spark.read.csv` command.
# MAGIC
# MAGIC **Note**: This particular csv doesn't have a header specified.

# COMMAND ----------

df = spark.read.csv("/mnt/GoFast/bronze/diabetes.csv", header=False, inferSchema=True)
display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 5D) Register User Defined Functions (UDFs).

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC If we have a `function` that can use values from a row in the `dataframe` as input, then we can map it to the entire dataframe. The only difference is that with PySpark UDFs we have to specify the `output` data type.
# MAGIC
# MAGIC We first __define the udf__ below:

# COMMAND ----------

# Define UDF
def square(s):
  return s * s

# Register UDF to spark as 'squaredWithPython'
spark.udf.register("squaredWithPython", square, LongType())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Note that we can call `squaredWithPython` immediately from SparkSQL. We first register a `temp` table called 'table':

# COMMAND ----------

# Register temptable with range of numbers
spark.range(0, 19, 3).toDF("num").createOrReplaceTempView("table")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT num, squaredWithPython(num) as num_sq FROM table

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC But note that with `DataFrames`, this will not work until we define the UDF __explicitly__ (on top of registering it above) with the respective return `DataType`. In other words, when we call `spark.udf.register` above, that registers it with spark SQL only, and for DataFrame it must be explicitly defined as an UDF.

# COMMAND ----------

# Convert temp table to DataFrame
tabledf = spark.table("table")

# Define UDF
squaredWithPython = udf(square, LongType())

display(tabledf.select("num", squaredWithPython("num").alias("num_sq")))

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. DataFrameReader
# MAGIC Candidates are expected to be familiar with the following architectural components and their relationship to each other.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 6A) Read data for the “core” data formats (CSV, JSON, JDBC, ORC, Parquet, text and tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CSV
# MAGIC
# MAGIC Let's read `airbnb-sf-listings.csv` from our mount, with the header specified, and infer schema on read.

# COMMAND ----------

csvdf = spark.read.csv("airbnb-sf-listings.csv", header=True, inferSchema=True)
display(csvdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### JSON
# MAGIC
# MAGIC Let's first view `zip.json` from our mount, and then load it into a DataFrame.

# COMMAND ----------

# MAGIC %fs head "dbfs:/mnt/GoFast/bronze/zips.json"

# COMMAND ----------

jsondf = spark.read.json("/mnt/GoFast/bronze/zips.json")
display(jsondf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### JDBC
# MAGIC
# MAGIC We are going to be reading a table from this Azure SQL Database for this activity.
# MAGIC
# MAGIC #### Azure SQL DB
# MAGIC ![Azure SQL DB](https://i.imgur.com/vAW5NFd.png)
# MAGIC
# MAGIC #### Reading table in SQL Server Management Studio
# MAGIC ![SQL Query](https://i.imgur.com/Dxd3zCc.png)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Set up JDBC connection:

# COMMAND ----------

jdbcUsername = "test"
jdbcPassword = "GhaithIlyan20142018!"
driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
jdbcHostname = "databricks-sqlserver-certif.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "databricks_cert"

# Create the JDBC URL without passing in the user and password parameters.
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

# Create a Properties() object to hold the parameters.
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : driverClass
}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run Query against JDBC connection:

# COMMAND ----------

pushdown_query = "(SELECT * FROM SalesLT.Customer) Customers"
jdbcdf = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
display(jdbcdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ORC
# MAGIC
# MAGIC The **Optimized Row Columnar (ORC)** file format provides a highly efficient way to store Hive data. It was designed to overcome limitations of the other Hive file formats. Using ORC files improves performance when Hive is reading, writing, and processing data. <br>
# MAGIC
# MAGIC Let's read `TestVectorOrcFile.testLzo.orc` from our mount.

# COMMAND ----------

orcdf = spark.read.orc('/mnt/GoFast/bronze/TestVectorOrcFile.testLzo.orc')
display(orcdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parquet
# MAGIC
# MAGIC **Apache Parquet** is a free and open-source column-oriented data storage format of the Apache Hadoop ecosystem. It is similar to the other columnar-storage file formats available in Hadoop namely RCFile and ORC. It is compatible with most of the data processing frameworks in the Hadoop environment. It provides efficient data compression and encoding schemes with enhanced performance to handle complex data in bulk. <br>
# MAGIC
# MAGIC **Parquet** is a columnar format that is supported by many other data processing systems. Spark SQL provides support for both reading and writing Parquet files that automatically preserves the schema of the original data. When writing Parquet files, all columns are automatically converted to be nullable for compatibility reasons.
# MAGIC
# MAGIC Let's read `wine.parquet` from our mount.

# COMMAND ----------

parquetDF = spark.read.parquet("/mnt/GoFast/bronze/wine.parquet")
display(parquetDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Text
# MAGIC
# MAGIC Let's read `tweets.txt` from our mount, then use a similar command as a `csv` earlier to load into DataFrame.

# COMMAND ----------

# MAGIC %fs head "dbfs:/mnt/GoFast/bronze/tweets.txt"

# COMMAND ----------

# Delimit on '='
txtdf = (spark.read
         .option("header", "false")
         .option("delimiter", "=")
         .csv("/mnt/GoFast/bronze/tweets.txt")
        ).toDF("Text","Tweet")

# Display "Tweet" Column only
display(txtdf.select("Tweet"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tables
# MAGIC
# MAGIC Let's read from one of our default Databricks Tables from the Hive Metastore:

# COMMAND ----------

tabledf = spark.sql("""SELECT * FROM databricks.citydata""")
display(tabledf)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 6B) How to configure options for specific formats

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Let's read `searose-env.csv` from our mount, specify the schema, header and delimiter.

# COMMAND ----------

# Schema
Schema = StructType([
    StructField("UTC_date_time", StringType(), True),
    StructField("Unix_UTC_timestamp", FloatType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("callsign", StringType(), True),
    StructField("wind_from", StringType(), True),
    StructField("knots", StringType(), True),
    StructField("gust", StringType(), True),
    StructField("barometer", StringType(), True),
    StructField("air_temp", StringType(), True),
    StructField("dew_point", StringType(), True),
    StructField("water_temp", StringType(), True)
])

WeatherDF = (spark.read                                     # The DataFrameReader
             .option("header", "true")                      # Use first line of file as header
             .schema(Schema)                                # Enforce Schema
             .option("delimiter", ",")                      # Set delimiter to ,
             .csv("/mnt/GoFast/bronze/searose-env.csv")     # Creates a DataFrame from CSV after reading in the file
            )

display(WeatherDF)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 6C) How to read data from non-core formats using `format()` and `load()`

# COMMAND ----------

# MAGIC %md
# MAGIC We can read the same file above `searose-env.csv` with `.format()` and `.load()` syntax - as well as other "non-core" files.

# COMMAND ----------

Weather2DF = (spark.read
              .format("csv")
              .schema(Schema)
              .option("header","true")
              .option("delimiter", ",")
              .load("/mnt/GoFast/bronze/searose-env.csv")
             )
display(Weather2DF)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 6D) How to specify a `DDL`-formatted schema

# COMMAND ----------

# MAGIC %md
# MAGIC ### DDL - Data Definition Language
# MAGIC
# MAGIC **Data Definition Language (DDL)** is a standard for commands that define the different structures in a database. DDL statements `create`, `modify`, and `remove` database objects such as `tables`, `indexes`, and `users`. Common DDL statements are `CREATE`, `ALTER`, and `DROP`.
# MAGIC
# MAGIC We want to be able to specify our Schema in a DDL format prior to reading a file. Let's try this for our `searose-env.csv` above

# COMMAND ----------

SchemaDDL = """UTC_date_time VARCHAR(255),
               Unix_UTC_timestamp float,
               latitude float,
               longitude float,
               callsign VARCHAR(255),
               wind_from VARCHAR(255),
               knots VARCHAR(255),
               gust VARCHAR(255),
               barometer VARCHAR(255),
               air_temp VARCHAR(255),
               dew_point VARCHAR(255),
               water_temp VARCHAR(255)"""

Weather3DF = spark.read.csv('/mnt/GoFast/bronze/searose-env.csv', header=True, schema=SchemaDDL)
display(Weather3DF)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 6E) How to construct and specify a schema using the `StructType` classes

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We've done this above already for task `6B`.

# COMMAND ----------

# MAGIC %md
# MAGIC # 7. DataFrameWriter
# MAGIC Candidates are expected to know how to:

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 7A) Write data to the “core” data formats (csv, json, jdbc, orc, parquet, text and tables)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Let's create a DataFrame `CustomerAddressDF` from the table `SalesLT.CustomerAddress` in our Azure SQL Database. We will then write this to various different formats to our Storage Account's `silver` Container.

# COMMAND ----------

# Mount "silver" storage container
mountStorageContainer(storageAccountName,storageAccountAccessKey,"silver","/mnt/GoFast/silver")

# COMMAND ----------

pushdown_query = "(SELECT * FROM SalesLT.CustomerAddress) CustomerAddresses"
CustomerAddressDF = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
display(CustomerAddressDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CSV

# COMMAND ----------

# Clean up directory
dbfsDirPath = "/mnt/GoFast/silver/CustomerAddresses/CSV"
dbutils.fs.rm(dbfsDirPath, recurse=True)
dbutils.fs.mkdirs(dbfsDirPath)

# Write DataFrame to path with header
CustomerAddressDF.write.format("csv").option("header","true").mode("overwrite").save(dbfsDirPath)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We see this file in our Storage Account: <br>
# MAGIC ![Committed CSV file](https://i.imgur.com/qVcgHVz.png)<br>
# MAGIC
# MAGIC And the contents are equal to the length of the data received from Azure SQL: <br>
# MAGIC ![CSV file contents](https://i.imgur.com/8jN1d19.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### JSON

# COMMAND ----------

# Clean up directory
dbfsDirPath = "/mnt/GoFast/silver/CustomerAddresses/JSON"
dbutils.fs.rm(dbfsDirPath, recurse=True)
dbutils.fs.mkdirs(dbfsDirPath)

# Write DataFrame to path
CustomerAddressDF.write.format("json").mode("overwrite").save(dbfsDirPath)

# COMMAND ----------

# MAGIC %md
# MAGIC ### JDBC
# MAGIC
# MAGIC This time, we do an `overwrite` to our Azure SQL Database Table, by restoring `CustomerAddresses` to a DataFrame from our bucket.

# COMMAND ----------

# Restore a backup of our DataFrame from Silver Zone
CustomerAddressBackupDF = spark.read.csv("/mnt/GoFast/silver/CustomerAddresses/CSV", header=True, inferSchema=True)

# Create temporary view
CustomerAddressBackupDF.createOrReplaceTempView("CustomerAddressView")

# Perform overwrite on SQL table
spark.sql("""SELECT * FROM CustomerAddressView""").write \
    .format("jdbc") \
    .mode("overwrite")  \
    .option("url", jdbcUrl) \
    .option("dbtable", "SalesLT.CustomerAddress") \
    .option("user", jdbcUsername) \
    .option("password", jdbcPassword) \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ORC

# COMMAND ----------

# Clean up directory
dbfsDirPath = "/mnt/GoFast/silver/CustomerAddresses/ORC"
dbutils.fs.rm(dbfsDirPath, recurse=True)
dbutils.fs.mkdirs(dbfsDirPath)

# Write DataFrame to path
CustomerAddressDF.write.format("orc").mode("overwrite").save(dbfsDirPath)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parquet

# COMMAND ----------

# Clean up directory
dbfsDirPath = "/mnt/GoFast/silver/CustomerAddresses/Parquet"
dbutils.fs.rm(dbfsDirPath, recurse=True)
dbutils.fs.mkdirs(dbfsDirPath)

# Write DataFrame to path
CustomerAddressDF.write.format("parquet").mode("overwrite").save(dbfsDirPath)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Text

# COMMAND ----------

# Clean up directory
dbfsDirPath = "/mnt/GoFast/silver/CustomerAddresses/Text"
dbutils.fs.rm(dbfsDirPath, recurse=True)
dbutils.fs.mkdirs(dbfsDirPath)

# Note that text file does not support `int` data type, and also expects one column, so we must convert to one '|'' seperated string
CustomerAddressConcatDF = CustomerAddressDF \
                                     .withColumn("CustomerID", col("CustomerID").cast("string")) \
                                     .withColumn("AddressID", col("AddressID").cast("string")) \
                                     .withColumn("ModifiedDate", col("ModifiedDate").cast("string")) \
                                     .withColumn("Concatenated", concat(col("CustomerID"), lit('|'), \
                                                                        col("AddressID"), lit('|'), \
                                                                        col("AddressType"), lit('|'), \
                                                                        col("rowguid"), lit('|'), \
                                                                        col("ModifiedDate"))) \
                                     .drop(col("CustomerID")) \
                                     .drop(col("AddressID")) \
                                     .drop(col("AddressType")) \
                                     .drop(col("rowguid")) \
                                     .drop(col("ModifiedDate"))
                                      
# Write DataFrame to path
CustomerAddressConcatDF.write.format("text").mode("overwrite").save(dbfsDirPath)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tables
# MAGIC
# MAGIC Let's create a Hive table usign `Parquet` (location above).

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC USE awproject;
# MAGIC
# MAGIC DROP TABLE IF EXISTS CustomerAddress;
# MAGIC CREATE TABLE IF NOT EXISTS CustomerAddress
# MAGIC USING parquet
# MAGIC OPTIONS  (path "/mnt/GoFast/silver/CustomerAddresses/Parquet");
# MAGIC
# MAGIC SELECT COUNT(*) FROM awproject.CustomerAddress

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 7B) Overwriting existing files

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We've already achieved this above when persisting a DataFrame to mount, by using `.mode("overwrite")` - we also do this below.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 7C) How to configure options for specific formats

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Previously, we wrote a CSV file with the `CustomerAddress` data that was `,` seperated. Let's overwrite that file with a `|` seperated version now.
# MAGIC
# MAGIC **Output:**<br>
# MAGIC ![Pipe seperated CSV](https://i.imgur.com/Uuw8uvX.png)

# COMMAND ----------

# Specify CSV directory
dbfsDirPath = "/mnt/GoFast/silver/CustomerAddresses/CSV"

# Write DataFrame to path with header and seperator
CustomerAddressDF.write.format("csv").option("header","true").option("delimiter", "|").mode("overwrite").save(dbfsDirPath)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 7D) How to write a data source to 1 single file or N separate files

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Difference between `coalesce` and `repartition`
# MAGIC
# MAGIC `repartition()` literally reshuffles the data to form as many partitions as we specify, i.e. the number of partitions can be **increased/decreased**. Whereas with `coalesce()` we avoid data movement and use the existing partitions, meaning the number of partitions can only be **decreased**.
# MAGIC
# MAGIC Note that `coalesce()` results in partitions with different amounts of data per partition, whereas `repartition()` is distributed evenly. As a result, the `coalesce()` operation may run faster than `repartition()`, but the partitions themselves may work slower further on because Spark is built to work with equal sized partitions across the task slots on the executors.
# MAGIC
# MAGIC **50 partitions**: <br>
# MAGIC ![50](https://i.imgur.com/BViVytU.png) <br><br>
# MAGIC **10 partitions**: <br>
# MAGIC ![10](https://i.imgur.com/soPSrpI.png) <br><br>
# MAGIC **1 partition**: <br>
# MAGIC ![1](https://i.imgur.com/1L0Mi5N.png)

# COMMAND ----------

# Clean up directory for 50 part CSV
dbfsDirPath = "/mnt/GoFast/silver/CustomerAddresses/CSV-50"
dbutils.fs.rm(dbfsDirPath, recurse=True)
dbutils.fs.mkdirs(dbfsDirPath)

# Redefine DataFrame with 50 partitions
CustomerAddressDF = CustomerAddressDF.repartition(50)

# Write DataFrame to path with header and repartition to 50 partitions
CustomerAddressDF.write.format("csv").option("header","true").mode("overwrite").save(dbfsDirPath)

# Clean up directory for 10 part CSV
dbfsDirPath = "/mnt/GoFast/silver/CustomerAddresses/CSV-10"
dbutils.fs.rm(dbfsDirPath, recurse=True)
dbutils.fs.mkdirs(dbfsDirPath)

# Write DataFrame to path with header and coalesce to 10 partitions
CustomerAddressDF.coalesce(10).write.format("csv").option("header","true").mode("overwrite").save(dbfsDirPath)

# Clean up directory for 1 part CSV
dbfsDirPath = "/mnt/GoFast/silver/CustomerAddresses/CSV-1"
dbutils.fs.rm(dbfsDirPath, recurse=True)
dbutils.fs.mkdirs(dbfsDirPath)

# Write DataFrame to path with header and coalesce to 1 partition
CustomerAddressDF.coalesce(1).write.format("csv").option("header","true").mode("overwrite").save(dbfsDirPath)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 7E) How to write partitioned data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Partitioning by Columns
# MAGIC
# MAGIC **Partitioning** uses **partitioning columns** to divide a dataset into smaller chunks (based on the values of certain columns) that will be written into separate directories.
# MAGIC
# MAGIC With a partitioned dataset, Spark SQL can load only the parts (partitions) that are really needed (and avoid doing filtering out unnecessary data on JVM). That leads to faster load time and more efficient memory consumption which gives a better performance overall.
# MAGIC
# MAGIC With a partitioned dataset, Spark SQL can also be executed over different subsets (directories) in parallel at the same time.

# COMMAND ----------

display(CustomerAddressDF)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Given our dataset above, let's partition by `AddressType` and write to storage.

# COMMAND ----------

# Clean up directory
dbfsDirPath = "/mnt/GoFast/silver/CustomerAddresses/CSV-partitioned-by-AddressType"
dbutils.fs.rm(dbfsDirPath, recurse=True)
dbutils.fs.mkdirs(dbfsDirPath)

# Write DataFrame to path with header and partition by AddressType
CustomerAddressDF.write.format("csv").option("header","true").mode("overwrite").partitionBy("AddressType").save(dbfsDirPath)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **And we see:**<br>
# MAGIC ![Partition](https://i.imgur.com/3Yr1Eyu.png)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 7F) How to bucket data by a given set of columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### What is Bucketing?
# MAGIC
# MAGIC **Bucketing** is an optimization technique that uses **buckets** (and **bucketing columns**) to determine data partitioning and avoid data shuffle.
# MAGIC
# MAGIC The motivation is to optimize performance of a join query by avoiding shuffles (aka _exchanges_) of tables participating in the join. Bucketing results in fewer exchanges (and so stages).

# COMMAND ----------

# Note that this is only supported to a table (and not to a location)
CustomerAddressDF.write \
  .mode("overwrite") \
  .bucketBy(10, "ModifiedDate") \
  .saveAsTable("CustomerAddress_bucketed")

display(sql('''SELECT * FROM CustomerAddress_bucketed'''))

# COMMAND ----------

# MAGIC %md
# MAGIC # 8. DataFrame

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 8A) Have a working understanding of every action such as `take()`, `collect()`, and `foreach()`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Let's demo the following table of **Actions** on our DataFrame:<br>
# MAGIC
# MAGIC | Method | Return | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | `collect()` | Collection | Returns an array that contains all of Rows in this Dataset. |
# MAGIC | `count()` | Long | Returns the number of rows in the Dataset. |
# MAGIC | `first()` | Row | Returns the first row. |
# MAGIC | `foreach(f)` | - | Applies a function f to all rows. |
# MAGIC | `foreachPartition(f)` | - | Applies a function f to each partition of this Dataset. |
# MAGIC | `head()` | Row | Returns the first row. |
# MAGIC | `reduce(f)` | Row | Reduces the elements of this Dataset using the specified binary function. |
# MAGIC | `show(..)` | - | Displays the top 20 rows of Dataset in a tabular form. |
# MAGIC | `take(n)` | Collection | Returns the first n rows in the Dataset. |
# MAGIC | `toLocalIterator()` | Iterator | Return an iterator that contains all of Rows in this Dataset. |
# MAGIC
# MAGIC Note once again that while **Transformations** always return a _DataFrame_, **Actions** either return a _result_ or _write to disk_.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### `collect()`
# MAGIC
# MAGIC Return a list that contains all of the elements in this RDD.
# MAGIC
# MAGIC **Note**: This method should only be used if the resulting array is expected to be small, as all the data is loaded into the driver’s memory.

# COMMAND ----------

Array = CustomerAddressDF.collect()
print(Array[0])
print("\n")
print(Array[0][0])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### `count()`
# MAGIC
# MAGIC Return the number of elements in this RDD.

# COMMAND ----------

CustomerAddressDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### `first()`
# MAGIC
# MAGIC Return the first element in this RDD/DataFrame.

# COMMAND ----------

CustomerAddressDF.first()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### `foreach(f)`
# MAGIC
# MAGIC Applies a function `f` to all elements of this RDD/DataFrame.
# MAGIC
# MAGIC **Note**: `RDD.foreach` method runs on the cluster for each _worker_, and so we don't see the print output.

# COMMAND ----------

def f(x): print(x)
CustomerAddressDF.foreach(f)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### `foreachPartition(f)`
# MAGIC
# MAGIC Applies a function to each partition of this RDD.
# MAGIC
# MAGIC **Note**: `RDD.foreachPartition` method runs on the cluster for each _worker_, and so we don't see the print output.

# COMMAND ----------

def f(x): print(x)
CustomerAddressDF.foreachPartition(f)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### `head()`
# MAGIC
# MAGIC Returns the first row.
# MAGIC
# MAGIC At first glance, this looks identical to `first()` - let's look at the difference:
# MAGIC
# MAGIC **Sorted Data**
# MAGIC
# MAGIC If your data is **_sorted_** using either `sort()` or `ORDER BY`, these operations will be deterministic and return either the 1st element using first()/head() or the top-n using head(n)/take(n).
# MAGIC
# MAGIC show()/show(n) return Unit (void) and will print up to the first 20 rows in a tabular form.
# MAGIC
# MAGIC These operations may require a shuffle if there are any aggregations, joins, or sorts in the underlying query.
# MAGIC
# MAGIC **Unsorted Data**
# MAGIC
# MAGIC If the data is **_not sorted_**, these operations are not guaranteed to return the 1st or top-n elements - and a shuffle may not be required.
# MAGIC
# MAGIC `show()`/`show(n)` return Unit (void) and will print up to 20 rows in a tabular form and in no particular order.
# MAGIC
# MAGIC If no shuffle is required (no aggregations, joins, or sorts), these operations will be optimized to inspect enough partitions to satisfy the operation - likely a much smaller subset of the overall partitions of the dataset.

# COMMAND ----------

CustomerAddressDF.head()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### `reduce(f)`
# MAGIC
# MAGIC Reduces the elements of this **RDD** (note that it doesn't work on DataFrames) using the specified commutative and associative binary operator. Currently reduces partitions locally.

# COMMAND ----------

from operator import add
sc.parallelize([2, 4, 6]).reduce(add)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### `show(id)`
# MAGIC
# MAGIC Displays the top 20 (_default, can be overwritten_) rows of Dataset in a tabular form

# COMMAND ----------

CustomerAddressDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### `take(n)`
# MAGIC
# MAGIC Take the first **n** elements of the RDD.
# MAGIC
# MAGIC It works by first scanning one partition, and using the results from that partition to estimate the number of additional partitions needed to satisfy the limit.
# MAGIC
# MAGIC **Note:** this method should only be used if the resulting array is expected to be small, as all the data is loaded into the driver’s memory.

# COMMAND ----------

CustomerAddressDF.take(2)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### `toLocalIterator()`
# MAGIC
# MAGIC Return an iterator that contains all of the elements in this RDD/DataFrame. The iterator will consume as much memory as the largest partition in this RDD.

# COMMAND ----------

[x for x in CustomerAddressDF.toLocalIterator()]

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 8B) Have a working understanding of the various transformations and how they work such as producing a `distinct` set, `filter`ing data, re`partition`ing and `coalesce`ing, performing `join`s and `union`s as well as producing `aggregates`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### `select(*cols)`
# MAGIC
# MAGIC Projects a set of expressions and returns a new `DataFrame`.
# MAGIC
# MAGIC **Parameters:**	
# MAGIC - **cols** – list of column names (`string`) or expressions (`Column`). If one of the column names is `*`, that column is expanded to include all columns in the current DataFrame.
# MAGIC
# MAGIC Using our `CustomerAddressDF` from above, let's `select` out a couple of the rows only.

# COMMAND ----------

TruncatedDF = CustomerAddressDF.select("CustomerID", "rowguid")
display(TruncatedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### `distinct()`
# MAGIC
# MAGIC Returns a new `DataFrame` containing the distinct rows in this `DataFrame`.
# MAGIC
# MAGIC Let's create a DF with duplicates, and then run `distinct` on it.

# COMMAND ----------

DuplicateDF = TruncatedDF.withColumn("CustomerID", lit(29772)).withColumn("rowguid", lit("BF40660E-40B6-495B-99D0-753CE987B1D1"))
display(DuplicateDF)

# COMMAND ----------

display(DuplicateDF.distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### `groupBy(*cols)`
# MAGIC
# MAGIC Groups the `DataFrame` using the specified columns, so we can run aggregation on them. See `GroupedData` for all the available aggregate functions.
# MAGIC
# MAGIC `groupby()` is an alias for `groupBy()`.
# MAGIC
# MAGIC **Parameters:**
# MAGIC - **cols** – list of columns to group by. Each element should be a column name (`string`) or an expression (`Column`).
# MAGIC
# MAGIC **Note**: `groupBy` by itself doesn't return anything quantitative, we also have to `agg` by an aggregation function to get a tangible result back.

# COMMAND ----------

CustomerAddressAggDF = CustomerAddressDF.groupBy("AddressType").agg({'AddressType':'count'})
display(CustomerAddressAggDF)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### `sum(*cols)`
# MAGIC
# MAGIC Compute the sum for each numeric columns for each group.
# MAGIC
# MAGIC **Parameters:**
# MAGIC - **cols** – list of column names (`string`). Non-numeric columns are ignored.

# COMMAND ----------

# Let's rename the column "count(AddressType)"
CustomerAddressAggDF2 = CustomerAddressAggDF.withColumn("AddressTypeCount", col("count(AddressType)")).drop(col("count(AddressType)"))

# We do a groupBy followed by a sum - to ultimately get back our number of rows in the original DataFrame
display(CustomerAddressAggDF2.groupBy().sum("AddressTypeCount"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### `orderBy(*cols, **kwargs)`
# MAGIC
# MAGIC Returns a new `DataFrame` sorted by the specified column(s).
# MAGIC
# MAGIC **Parameters:**
# MAGIC - **cols** – list of `Column` or column names to sort by.
# MAGIC - **ascending** – boolean or list of boolean (default True). Sort ascending vs. descending. Specify list for multiple sort orders. If a list is specified, length of the list must equal length of the cols.

# COMMAND ----------

display(CustomerAddressDF.orderBy("ModifiedDate", ascending = 0))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### `filter()`
# MAGIC
# MAGIC Filters rows using the given condition.
# MAGIC
# MAGIC **where()** is an alias for **filter()**.
# MAGIC
# MAGIC **Parameters:**
# MAGIC - **condition** – a Column of types.BooleanType or a string of SQL expression.

# COMMAND ----------

display(CustomerAddressDF.filter("AddressID = 484"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### `limit()`
# MAGIC
# MAGIC Limits the result count to the number specified.

# COMMAND ----------

display(CustomerAddressDF.limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### `partition()` and `coalesce`
# MAGIC
# MAGIC We already discussed this in detail earlier in **7D**.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### `join(other, on=None, how=None)`
# MAGIC
# MAGIC Joins with another `DataFrame`, using the given join expression.
# MAGIC
# MAGIC **Parameters**:
# MAGIC - **other** – Right side of the join
# MAGIC - **on** – a string for the join column name, a list of column names, a join expression (Column), or a list of Columns. If on is a string or a list of strings indicating the name of the join column(s), the column(s) must exist on both sides, and this performs an equi-join.
# MAGIC - **how** – str, default `inner`. Must be one of: `inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`, `right`, `right_outer`, `left_semi`, and `left_anti`.

# COMMAND ----------

# Let's query Azure SQL directly DataFrame we want to recreate
pushdown_query = """(SELECT DISTINCT C.CompanyName, A.City
                    FROM [SalesLT].[Customer] C
                    INNER JOIN [SalesLT].[CustomerAddress] CA ON C.CustomerID = CA.CustomerID
                    INNER JOIN [SalesLT].[Address] A ON CA.AddressID = A.AddressID
                    WHERE CA.AddressID IS NOT NULL AND A.AddressLine1 IS NOT NULL) CompanyAndAddress"""

CompanyAndAddressDF = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
display(CompanyAndAddressDF.orderBy("CompanyName", ascending = 1))

# COMMAND ----------

# Get the underlying Tables as DataFrames from Azure SQL
pushdown_query = "(SELECT CustomerID, CompanyName FROM SalesLT.Customer) Customer"
CustomerDF = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)

pushdown_query = "(SELECT CustomerID, AddressID FROM SalesLT.CustomerAddress) CustomerAddress"
CustomerAddressDF = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)

pushdown_query = "(SELECT AddressID,City FROM SalesLT.Address) Address"
AddressDF = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)

# COMMAND ----------

# Perform joins identical to goal DataFrame above
display(CustomerDF.join(CustomerAddressDF, CustomerDF.CustomerID == CustomerAddressDF.CustomerID, 'inner') \
                  .join(AddressDF, CustomerAddressDF.AddressID == AddressDF.AddressID, 'inner') \
                  .select(CustomerDF.CompanyName, AddressDF.City) \
                  .orderBy("CompanyName", ascending = 1) \
                  .distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC And we get back the same DataFrame as `CompanyAndAddressDF` from our above SQL query.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### `union()`
# MAGIC
# MAGIC Return a new `DataFrame` containing union of rows in this and another frame.
# MAGIC
# MAGIC This is equivalent to _UNION ALL_ in SQL. To do a SQL-style set union (that does deduplication of elements), use this function followed by `distinct()`.
# MAGIC
# MAGIC Also as standard in SQL, this function resolves columns by position (not by name).

# COMMAND ----------

# Create two DataFrames with fruit lists, with some multiple occurences
df1 = spark.createDataFrame(["apple", "orange", "apple", "mango"], StringType())
df2 = spark.createDataFrame(["cherries", "orange", "blueberry", "apple"], StringType())

# Perform union and display data
df3 = df1.unionByName(df2)
display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### `agg(*exprs)`
# MAGIC
# MAGIC Compute aggregates and returns the result as a `DataFrame`.
# MAGIC
# MAGIC The available aggregate functions can be:
# MAGIC 1. built-in aggregation functions, such as _avg, max, min, sum, count_
# MAGIC 2. group aggregate pandas UDFs, created with `pyspark.sql.functions.pandas_udf()`
# MAGIC
# MAGIC **Note:** There is no partial aggregation with group aggregate UDFs, i.e., a full shuffle is required. Also, all the data of a group will be loaded into memory, so the user should be aware of the potential OOM risk if data is skewed and certain groups are too large to fit in memory.

# COMMAND ----------

# Perform aggregation on `count` of the fruit names - i.e. the number of occurences in the union list per fruit.
display(df3.groupby("value").agg({'value':'count'}).orderBy("value", ascending = 1))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 8C) Know how to `cache` data, specifically to `disk`, `memory` or both

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Caching in Spark
# MAGIC As we discussed earlier, Spark supports pulling data sets into a cluster-wide in-memory `cache`. This is very useful when data is accessed repeatedly, such as when querying a small “hot” dataset (required in many operations) or when running an iterative algorithm. 
# MAGIC
# MAGIC Here are some relevant functions:
# MAGIC
# MAGIC ##### `cache()`
# MAGIC Persists the `DataFrame` with the default storage level (`MEMORY_AND_DISK`). `persist()` without an argument is equivalent with `cache()`.
# MAGIC
# MAGIC ##### `unpersist()`
# MAGIC Marks the `DataFrame` as non-persistent, and remove all blocks for it from memory and disk.
# MAGIC
# MAGIC ##### `persist(storageLevel)`
# MAGIC Sets the storage level to persist the contents of the `DataFrame` across operations after the first time it is computed. This can only be used to assign a new storage level if the `DataFrame` does not have a storage level set yet. If no storage level is specified defaults to (`MEMORY_AND_DISK`).
# MAGIC
# MAGIC As we see below, there are 4 caching levels that can be fine-tuned with `persist()`. Let's look into the options available in more detail.
# MAGIC
# MAGIC | Storage Level	     | Equivalent                                    | Description                                                 |
# MAGIC | ------------------ | --------------------------------------------- | ----------------------------------------------------------- |
# MAGIC | `MEMORY_ONLY`      | `StorageLevel(False, True, False, False, 1)`  | Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, some partitions will not be cached and will be recomputed on the fly each time they're needed. This is the default level.|
# MAGIC | `MEMORY_AND_DISK`  | `StorageLevel(True, True, False, False, 1)`   | Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, store the partitions that don't fit on disk, and read them from there when they're needed.                          |
# MAGIC | `DISK_ONLY`        | `StorageLevel(True, False, False, False, 1)`  | 	Store the RDD partitions only on disk.                     |	
# MAGIC | `MEMORY_ONLY_2`    | `StorageLevel(False, True, False, False, 2)`  | Same as the levels above, but replicate each partition on two cluster nodes.|
# MAGIC | `MEMORY_AND_DISK_2`| `StorageLevel(True, True, False, False, 2)`   | Same as the levels above, but replicate each partition on two cluster nodes. |
# MAGIC | `OFF_HEAP `        | `StorageLevel(True, True, True, False, 1)`    | Similar to MEMORY_ONLY_SER, but store the data in off-heap memory. This requires off-heap memory to be enabled. |

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC As a simple example, let’s mark our `CompanyAndAddressDF` dataset to be cached. While it may seem silly to use Spark to explore and cache a small `DataFrame`, the interesting part is that these same functions can be used on very large data sets, even when they are striped across tens or hundreds of nodes.

# COMMAND ----------

# Check the cache level before we do anything
CompanyAndAddressDF.storageLevel

# COMMAND ----------

# Let's cache the DataFrame with default - MEMORY_AND_DISK, and check the cache level
CompanyAndAddressDF.cache().storageLevel

# COMMAND ----------

# Unpersist and proceed to Memory Only Cache
CompanyAndAddressDF.unpersist()
CompanyAndAddressDF.persist(storageLevel = StorageLevel.MEMORY_ONLY).storageLevel

# COMMAND ----------

# Unpersist and proceed to Memory and Disk 2 Cache
CompanyAndAddressDF.unpersist()
CompanyAndAddressDF.persist(storageLevel = StorageLevel.MEMORY_AND_DISK_2).storageLevel

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC And so on.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 8D) Know how to `uncache` previously cached data

# COMMAND ----------

# Let's uncache the DataFrame
CompanyAndAddressDF.unpersist()

# Check the Cache level
CompanyAndAddressDF.storageLevel

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 8E) Converting a *DataFrame* to a global or temp view.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Global Temporary View vs. Temporary View
# MAGIC Temporary views in Spark SQL are session-scoped and will disappear if the session that creates it terminates. If you want to have a temporary view that is shared among all sessions and keep alive until the Spark application terminates, you can create a global temporary view. Global temporary view is tied to a system preserved database `global_temp`, and we must use the qualified name to refer it, e.g. `SELECT * FROM global_temp.view1`. We use these two commands:
# MAGIC
# MAGIC - **Global Temp View**: `df.createOrReplaceGlobalTempView("tempViewName")` creates a global temporary view with this dataframe df. Lifetime of this view is dependent to spark application itself, if you want to drop this view: `spark.catalog.dropGlobalTempView("tempViewName")`
# MAGIC - **Temp View**: `df.createOrReplaceTempView("tempViewName")` creates or replaces a local temporary view with this dataframe df. Lifetime of this view is dependent to SparkSession class, if you want to drop this view: `spark.catalog.dropTempView("tempViewName")`
# MAGIC
# MAGIC

# COMMAND ----------

# Create Global Temporary View
CompanyAndAddressDF.createOrReplaceGlobalTempView("CompanyAndAddressGlobal")
display(spark.sql("SELECT * FROM global_temp.CompanyAndAddressGlobal"))

# COMMAND ----------

# Create SQL Temporary View
CompanyAndAddressDF.createOrReplaceTempView("CompanyAndAddressTemp")
display(spark.sql("SELECT * FROM CompanyAndAddressTemp"))

# COMMAND ----------

# Drop Temporary Views
spark.catalog.dropGlobalTempView("CompanyAndAddressGlobal")
spark.catalog.dropTempView("CompanyAndAddressTemp")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 8F) Applying hints

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Structured queries can be optimized using **Hint Framework** that allows for specifying query hints.
# MAGIC
# MAGIC **Query hints** allow for annotating a query and give a hint to the query optimizer how to optimize logical plans. This can be very useful when the query optimizer cannot make optimal decision, e.g. with respect to join methods due to conservativeness or the lack of proper statistics.
# MAGIC
# MAGIC Spark SQL supports COALESCE and REPARTITION and BROADCAST hints. All remaining unresolved hints are silently removed from a query plan at analysis.

# COMMAND ----------

display(CustomerDF.join(CustomerAddressDF.hint("broadcast"), "CustomerID"))

# COMMAND ----------

# MAGIC %md
# MAGIC # 9. Row & Column

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 9A) Candidates are expected to know how to work with row and columns to successfully extract data from a *DataFrame*

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We can easily get back a single Column or Row and perform manipulations in Spark. For example, with our DataFrame `CustomerDF`:

# COMMAND ----------

display(CustomerDF)

# COMMAND ----------

# Extract First Row
display(CustomerDF.filter("CompanyName == 'A Bike Store' and CustomerID == 1"))

# COMMAND ----------

# Extract Distinct Second Column 'CompanyName', Not like "A **** ****", Order By descending, Pick Top 5
display(CustomerDF.select("CompanyName").filter("CompanyName not like 'A%'").distinct().orderBy("CompanyName", ascending = 1).take(5))

# COMMAND ----------

# MAGIC %md
# MAGIC # 10. Spark SQL Functions
# MAGIC When instructed what to do, candidates are expected to be able to employ the multitude of Spark SQL functions. Examples include, but are not limited to:

# COMMAND ----------

# MAGIC %md
# MAGIC Let's use this Databricks table `databricks.citydata` for this section wherever applicable:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM awproject.dimgeography

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 10A) Aggregate functions: getting the `first` or `last` item from an array or computing the `min` and `max` values of a column.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **`first(expr[, isIgnoreNull])`** <br>
# MAGIC
# MAGIC Returns the first value of expr for a group of rows. If `isIgnoreNull` is true, returns only non-null values.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT first(city), last(city) FROM 
# MAGIC   (SELECT City FROM  awproject.dimgeography
# MAGIC   ORDER BY City ASC)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **`min(col)`** <br>
# MAGIC
# MAGIC Aggregate function: returns the minimum value of the expression in a group.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT min(GeographyKey), max(GeographyKey) FROM 
# MAGIC   (SELECT GeographyKey FROM  awproject.dimgeography)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 10B) Collection functions: testing if an array `contains` a value, `explode`ing or `flatten`ing data.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT array_contains(array('a','b','c','d'), 'a') AS contains_a;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT explode(array('a','b','c','d')); /* array to a table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT flatten(array(array(1, 2), array(3, 4)));

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 10C) Date time functions: parsing `string`s into `timestamp`s or formatting `timestamp`s into `string`s

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **`to_date(date_str[, fmt])`** <br>
# MAGIC Parses the `date_str` expression with the `fmt` expression to a date. Returns `null` with invalid input. By default, it follows casting rules to a date if the `fmt` is omitted.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT to_date('2009-07-30 04:17:52') AS date, to_date('2016-12-31', 'yyyy-MM-dd') AS date_formatted

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **`to_timestamp(timestamp[, fmt])`** <br>
# MAGIC Parses the `timestamp` expression with the `fmt` expression to a `timestamp`. Returns `null` with invalid input. By default, it follows casting rules to a `timestamp` if the fmt is omitted.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT to_timestamp('2016-12-31 00:12:00') AS timestamp, to_timestamp('2016-12-31', 'yyyy-MM-dd') AS timestamp_formatted;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **`date_format(timestamp, fmt)`** <br>
# MAGIC Converts `timestamp` to a value of `string` in the format specified by the date format `fmt`.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT date_format('2016-12-31T00:12:00.000+0000', 'y') AS year_only

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 10D) Math functions: computing the cosign, floor or log of a number

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **`cos(expr)`** <br>
# MAGIC Returns the cosine of `expr`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cos(0) AS cos

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **`floor(expr)`** <br>
# MAGIC Returns the largest integer not greater than `expr`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT floor(5.9) AS floored

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **`log(base, expr)`** <br>
# MAGIC Returns the logarithm of `expr` with base.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT log(2, 8) AS log_2

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 10E) Misc functions: converting a value to crc32, md5, sha1 or sha2

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **`crc32(expr)`** <br>
# MAGIC Returns a cyclic redundancy check value of the `expr` as a `bigint`.

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT crc32('hello')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **`md5(expr)`** <br>
# MAGIC Returns an MD5 128-bit checksum as a hex string of `expr`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT md5('hello')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **`sha1(expr)`** <br>
# MAGIC Returns a sha1 hash value as a hex string of the `expr`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sha1('hello')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **`sha2(expr, bitLength)`** <br>
# MAGIC Returns a checksum of SHA-2 family as a hex string of expr. SHA-224, SHA-256, SHA-384, and SHA-512 are supported. Bit length of 0 is equivalent to 256.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sha2('hello', 256)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 10F) Non-aggregate functions: creating an array, testing if a column is null, not-null, nan, etc

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **`array(expr, ...)`** <br>
# MAGIC Returns an array with the given elements.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT array(1, 2, 3) AS array

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **`isnull(expr)`** <br>
# MAGIC Returns `true` if expr is `null`, or `false` otherwise.

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT isnull(GeographyKey) FROM awproject.dimgeography
# MAGIC limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **`isnotnull(expr)`** <br>
# MAGIC Returns `true` if `expr` is *not* `null`, or `false` otherwise.

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT isnotnull(GeographyKey) FROM awproject.dimgeography
# MAGIC limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Difference between `null` and `NaN` <br>
# MAGIC `null` values represents "no value" or "nothing", it's not even an empty string or zero. It can be used to represent that nothing useful exists.
# MAGIC
# MAGIC `NaN` stands for "Not a Number", it's usually the result of a mathematical operation that doesn't make sense, e.g. 0.0/0.0.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **`isnan(expr)`** <br>
# MAGIC Returns `true` if expr is `NaN`, or `false` otherwise.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT cast('NaN' as double), isnan(cast('NaN' as double)), cast('hello' as double), isnan(cast('hello' as double))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 10G) Sorting functions: sorting data in descending order, ascending order, and sorting with proper null handling

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Let's use our `WeatherDF` for this section:

# COMMAND ----------

display(WeatherDF)

# COMMAND ----------

# We replace the `NULL` in column `gust` and `air_temp` above with `null`, and typecast to float
WeatherDFtemp = WeatherDF.withColumn("gust",when((ltrim(col("gust")) == "NULL"),lit(None)).otherwise(col("gust").cast("float"))) \
                     .withColumn("air_temp",when((ltrim(col("air_temp")) == "NULL"),lit(None)).otherwise(col("air_temp").cast("float")))

# COMMAND ----------

# Create temp SQL view
WeatherDFtemp.createOrReplaceTempView('Weather')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We demonstrate:
# MAGIC - Filter for values where **gust** is not `null` and **air_temp** is not `null`
# MAGIC - Order by **gust** `ascending`
# MAGIC - Order by **air_temp** `descending`

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT gust, air_temp 
# MAGIC FROM Weather
# MAGIC WHERE isnotnull(gust) AND isnotnull(air_temp)
# MAGIC ORDER BY gust ASC, air_temp DESC

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 10H) String functions: applying a provided regular expression, trimming string and extracting substrings.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **`regexp_extract(str, regexp[, idx])`** <br>
# MAGIC Extracts a group that matches `regexp`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT regexp_extract('Verified by Stacy', '( by)')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **`regexp_replace(str, regexp, rep)`** <br>
# MAGIC Replaces all substrings of `str` that match regexp with `rep`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT regexp_replace('Verified by Stacy', '( by)', ':')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **`trim(str)`** <br>
# MAGIC Removes the leading and trailing space characters from str.
# MAGIC
# MAGIC **`trim(BOTH trimStr FROM str)`** <br>
# MAGIC Remove the leading and trailing trimStr characters from str
# MAGIC
# MAGIC **`trim(LEADING trimStr FROM str)`** <br>
# MAGIC Remove the leading trimStr characters from str
# MAGIC
# MAGIC **`trim(TRAILING trimStr FROM str)`** <br>
# MAGIC Remove the trailing trimStr characters from str <br>
# MAGIC
# MAGIC **Arguments:** <br>
# MAGIC `str` - a string expression <br>
# MAGIC `trimStr` - the trim string characters to trim, the default value is a single space <br>
# MAGIC `BOTH`, `FROM` - these are keywords to specify trimming string characters from both ends of the string <br>
# MAGIC `LEADING`, `FROM` - these are keywords to specify trimming string characters from the left end of the string <br>
# MAGIC `TRAILING`, `FROM` - these are keywords to specify trimming string characters from the right end of the string

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT trim('    SparkSQL   '),
# MAGIC        trim('SL', 'SSparkSQLS'), 
# MAGIC        trim(BOTH 'SL' FROM 'SSparkSQLS'), 
# MAGIC        trim(LEADING 'SL' FROM 'SSparkSQLS'), 
# MAGIC        trim(TRAILING 'SL' FROM 'SSparkSQLS')

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 10I) UDF functions: employing a UDF function.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We call the `squaredWithPython` UDF we defined earlier.

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT gust, squaredWithPython(floor(gust))
# MAGIC FROM Weather
# MAGIC WHERE isnotnull(gust)
# MAGIC ORDER BY gust ASC

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 10J) Window functions: computing the rank or dense rank.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Let's create a `DataFrame` to demo these functions, and create a `Window`.
# MAGIC
# MAGIC ##### What are `Window` Functions? <br>
# MAGIC A window function calculates a return value for every input row of a table based on a group of rows, called the **Frame**. Every input row can have a unique frame associated with it. This characteristic of window functions makes them more powerful than other functions and allows users to express various data processing tasks that are hard (if not impossible) to be expressed without window functions in a concise way.

# COMMAND ----------

schema = StructType([
  StructField("letter", StringType(), True),
  StructField("position", IntegerType(), True)])

df = spark.createDataFrame([("a", 10), ("a", 10), ("a", 20)], schema)
windowSpec = Window.partitionBy("letter").orderBy("position")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **`rank()`** <br>
# MAGIC Computes the rank of a value in a group of values. The result is one plus the number of rows preceding or equal to the current row in the ordering of the partition. The values will produce gaps in the sequence.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **`dense_rank()`** <br>
# MAGIC Computes the rank of a value in a group of values. The result is one plus the previously assigned rank value. Unlike the function `rank`, `dense_rank` will not produce gaps in the ranking sequence.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **`row_number()`** <br>
# MAGIC Returns a sequential number starting at 1 within a window partition.

# COMMAND ----------

display(df.withColumn("rank", rank().over(windowSpec))
  .withColumn("dense_rank", dense_rank().over(windowSpec))
  .withColumn("row_number", row_number().over(windowSpec)))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Note that the value "10" exists twice in **position** within the same window (**letter** = "a"). That's when you see a difference between the three functions `rank`, `dense_rank`, `row`.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Another example
# MAGIC
# MAGIC We create a _productRevenue_ table as seen below. <br>
# MAGIC
# MAGIC **We want to answer two questions:** <br>
# MAGIC 1. What are the best-selling and the second best-selling products in every category?
# MAGIC 2. What is the difference between the revenue of each product and the revenue of the best-selling product in the same category of that product?

# COMMAND ----------

# Create the Products
Product = Row("product", "category", "revenue")


# Create DataFrames from rows
ProductDF = spark.createDataFrame([
                                   Product('Thin', 'Cell phone', 6000),
                                   Product('Normal', 'Tablet', 1500),
                                   Product('Mini', 'Tablet', 5500),
                                   Product('Ultra thin', 'Cell phone', 5500),
                                   Product('Very thin', 'Cell phone', 6000),
                                   Product('Big', 'Tablet', 2500),
                                   Product('Bendable', 'Cell phone', 3000),
                                   Product('Foldable', 'Cell phone', 3000),
                                   Product('Pro', 'Tablet', 4500),
                                   Product('Pro2', 'Tablet', 6500)
                                  ]) 
ProductDF.createOrReplaceTempView("productRevenue")
display(ProductDF)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC To answer the first question “**What are the best-selling and the second best-selling products in every category?**”, we need to rank _products_ in a _category_ based on their _revenue_, and to pick the best selling and the second best-selling products based the ranking. <br>
# MAGIC
# MAGIC Below is the SQL query used to answer this question by using window function `dense_rank`.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   product,
# MAGIC   category,
# MAGIC   revenue
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     product,
# MAGIC     category,
# MAGIC     revenue,
# MAGIC     dense_rank() OVER (PARTITION BY category ORDER BY revenue DESC) as rank
# MAGIC   FROM productRevenue) tmp
# MAGIC WHERE
# MAGIC   rank <= 2
# MAGIC ORDER BY category DESC

# COMMAND ----------

# MAGIC %md
# MAGIC For the second question “**What is the difference between the revenue of each product and the revenue of the best selling product in the same category as that product?**”, to calculate the revenue difference for a _product_, we need to find the highest revenue value from _products_ in the same _category_ for each product. <br>
# MAGIC
# MAGIC Below is a Python DataFrame program used to answer this question.

# COMMAND ----------

windowSpec = Window.partitionBy(col("category")).orderBy(col("revenue").desc()).rangeBetween(-sys.maxsize, sys.maxsize)
revenue_difference = (max(col("revenue")).over(windowSpec) - col("revenue"))
dataFrame = sqlContext.table("productRevenue")
display(dataFrame.select("product", "category", "revenue", revenue_difference.alias("revenue_difference")).orderBy("category", ascending = 0))