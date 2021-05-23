# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Spark NLP and Piplines Demonstration

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##Introduction
# MAGIC In this demonstration, we will ingest data from databricks datasets concerning wikipedia articles into Bronze level delta tables.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Setup Dataset 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## File list for dataset

# COMMAND ----------

# DBTITLE 1,Get a list of all datasets
# MAGIC %fs ls /databricks-datasets/wikipedia-datasets/data-001/en_wikipedia/articles-only-parquet

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Load the Dataset

# COMMAND ----------

# DBTITLE 1,Generate a Dataframe for the Wiki Dataset
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Create a dataframe for the Wiki dataset
# MAGIC """
# MAGIC 
# MAGIC df = spark.read\
# MAGIC           .format("parquet")\
# MAGIC           .load("dbfs:/databricks-datasets/wikipedia-datasets/data-001/en_wikipedia/articles-only-parquet/part-*.gz.parquet")
# MAGIC 
# MAGIC # create a temporary view linked to the dataframe for cross language interoperability
# MAGIC df.createOrReplaceTempView("wiki_raw")
# MAGIC 
# MAGIC # show only the first 10 rows
# MAGIC display(df.take(10))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Schema of Dataset

# COMMAND ----------

# DBTITLE 1,Get the Schema of the Wiki Dataset
# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Get the schema of the databricks wiki dataset
# MAGIC --
# MAGIC 
# MAGIC DESCRIBE WIKI_RAW;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Clean the dataset

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Remove any duplicates from the dataset
# MAGIC """
# MAGIC 
# MAGIC df_clean = df.distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Ingest data into Bronze Layer

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Add data to a table in delta lake
# MAGIC """
# MAGIC 
# MAGIC df_clean.limit(10000)\
# MAGIC         .write\
# MAGIC         .format("delta")\
# MAGIC         .mode("append")\
# MAGIC         .saveAsTable("wiki_bronze")
