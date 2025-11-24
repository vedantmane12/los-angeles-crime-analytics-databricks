# Databricks notebook source
# MAGIC %md
# MAGIC # Los Angeles Crime Analytics: ETL Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Medallion Architecture Implementation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importing Libraries

# COMMAND ----------

#Libraries management
from pyspark import pipelines as pl
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Environment Setup

# COMMAND ----------

# Environment Setup
catalog = "workspace"
schema = "la_crime_schema"
volume = "datastore"
file_name = "crime_data_raw.csv"

# Paths
path_volume = f"/Volumes/{catalog}/{schema}/{volume}"
volume_path = f"{path_volume}/{file_name}"

# bronze_table_name = f"{catalog}.{schema}.bronze_lacrime_incidents"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer Data Ingestion

# COMMAND ----------

# Bronze layer table
pl.create_streaming_table(
    name="lacrime_incidents_bronze",
    comment="Bronze layer: Raw LA crime data ingested from CSV files in Unity Catalog Volume")

# Ingest the raw data into the bronze table using append flow
@pl.append_flow(
  target = "lacrime_incidents_bronze", # object name
  name = "lacrime_incidents_bronze_ingest_flow", # flow name
  comment="Ingests raw crime data from CSV using Auto Loader"
)
def cust_bronze_sd_rescue_ingest_flow():
  # Read CSV file and ingest data into Bronze layer
  df = (
      spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("cloudFiles.inferColumnTypes", "true")
          .option("header", "true")
          .option("cloudFiles.schemaLocation", f"{path_volume}/bronze_schema_checkpoint")
          .load(f"{path_volume}")
  )
  return df.withColumn("ingestion_datetime", current_timestamp()) \
           .withColumn("source_filename", col("_metadata.file_path")) \
           .withColumn("created_by", lit("lacrime_incidents_bronze_ingest_flow"))

