# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Spark NLP Annotation

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##Introduction
# MAGIC In this notebook we are going to annotate data retrieved from a bronze table in the previous notebook to generate labels for our data set. These labels will be stored in silver level tables.

# COMMAND ----------

# MAGIC %md
# MAGIC #Setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Install Spark NLP Libraries

# COMMAND ----------

# DBTITLE 1,Spark NLP
# MAGIC %pip install spark-nlp

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Setup Tensorboard

# COMMAND ----------

# DBTITLE 1,Load Tensorboard Extension
# MAGIC %load_ext tensorboard

# COMMAND ----------

# DBTITLE 1,Setup Tensorboard
try:
  username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
except:
  username = str(uuid.uuid1()).replace("-", "")
experiment_log_dir = "/dbfs/user/{}/annotators_log/".format(username)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### View Tensorboard

# COMMAND ----------

# DBTITLE 1,Tensorboard
# MAGIC %tensorboard --logdir $experiment_log_dir

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Setup the Dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Load data

# COMMAND ----------

# DBTITLE 1,Load the Data into a Dataframe
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Load the data from delta as a dataframe
# MAGIC """
# MAGIC 
# MAGIC df = spark.table("wiki_bronze")
# MAGIC 
# MAGIC # show the dataframe
# MAGIC display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Schema

# COMMAND ----------

# DBTITLE 1,Get the Schema of the Dataset
# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Get details on the table that will be annotated
# MAGIC --
# MAGIC 
# MAGIC DESCRIBE WIKI_BRONZE;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Wiki Annotation

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Download NLP Annotation Model

# COMMAND ----------

# DBTITLE 1,Setup Spark NLP and Download a Pretrained Pipeline
from sparknlp.base import *
from sparknlp.annotator import *              
from sparknlp.pretrained import PretrainedPipeline
import sparknlp

# Start Spark Session with Spark NLP
spark = sparknlp.start()

# Download a pre-trained pipeline 
pipeline = PretrainedPipeline(name = 'explain_document_dl', lang='en')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Annotate data

# COMMAND ----------

# DBTITLE 1,Annotate the dataframe
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Annotate the text in our wiki dataset
# MAGIC """
# MAGIC 
# MAGIC df_annotated = pipeline.annotate(df.limit(10), column = "text")
# MAGIC 
# MAGIC # cache the dataframe for the purposes of the demo
# MAGIC df_annotated.cache()

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
