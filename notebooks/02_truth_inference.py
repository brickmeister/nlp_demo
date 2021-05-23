# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Spark NLP Annotation

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##Introduction
# MAGIC In this notebook we are going to check the results of our annotations

# COMMAND ----------

# MAGIC %md
# MAGIC #Setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Setup the Dataframe

# COMMAND ----------

# DBTITLE 1,Get the Schema of the Dataset
# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Get details on the annotated data
# MAGIC --
# MAGIC 
# MAGIC DESCRIBE WIKI_SILVER;

# COMMAND ----------

# DBTITLE 1,Load the Data into a Dataframe
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Load the data from delta as a dataframe
# MAGIC """
# MAGIC 
# MAGIC df = spark.table("wiki_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Truth Inferencing

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Setup a truth inferencing function

# COMMAND ----------

# DBTITLE 1,Python UDF for Truth Inferencing
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Check if text has been correctly annotated
# MAGIC """
# MAGIC 
# MAGIC def truth_check(row) -> bool:
# MAGIC   """
# MAGIC   Check if a value is missing 
# MAGIC   """
# MAGIC   if row is None:
# MAGIC     return False
# MAGIC   else:
# MAGIC     return True
# MAGIC   
# MAGIC spark.udf.register("truth_check", truth_check)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Run the Truth Inferencing

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Check if the data passes quality measures
# MAGIC --
# MAGIC 
# MAGIC select *, 
# MAGIC        truth_check(lemma) as check
# MAGIC from wiki_silver;

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Run some truth inferencing
# MAGIC """
# MAGIC 
# MAGIC df_checked = spark.sql(f"SELECT *, truth_check(lemma) as check FROM wiki_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Ingest to Silver table

# COMMAND ----------

# DBTITLE 1,Write results to a silver table
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Write annotated text to a silver table
# MAGIC """
# MAGIC 
# MAGIC df_annotated.select("title", "id", "revisionId", "revisionTimestamp", "revisionUsername", "revisionUsernameID", "text", "lemma", "stem")\
# MAGIC             .write\
# MAGIC             .format("delta")\
# MAGIC             .mode("append")\
# MAGIC             .saveAsTable("wiki_silver")
