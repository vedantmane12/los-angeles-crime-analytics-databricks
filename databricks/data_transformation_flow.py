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

spark.sql("USE CATALOG `workspace`")
spark.sql("USE SCHEMA `la_crime_schema`")

# COMMAND ----------

# Environment Setup
catalog = "workspace"
schema = "la_crime_schema"
volume = "datastore"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer Data Transformations

# COMMAND ----------

pl.create_streaming_table(
    name="lacrime_incidents_silver",
    comment="Silver layer: Cleaned and enriched LA crime data with derived fields and quality checks",
    expect_all_or_drop={
        # Expectation 1: Primary key must not be null
        "valid_primary_key": "dr_no IS NOT NULL",        
        # Expectation 2: Report date should be >= occurrence date (logical consistency)
        "valid_date_logic": "date_rptd >= date_occ OR date_rptd IS NULL OR date_occ IS NULL",
        # Expectation 3: Dates must be from 2020 onwards (fail pipeline if violated)
        "valid_date_range": "date_occ >= '2020-01-01'"
    }
)
@pl.append_flow(
    target="lacrime_incidents_silver",
    name="lacrime_incidents_silver_transformation_flow",
    comment="Applies cleaning, enrichment, and quality rules to Bronze data"
)
def lacrime_incidents_silver_transformation_flow():
    # Read from Bronze layer (streaming), apply transformations, and write to Silver layer

    # Read from Bronze layer (streaming)
    df = pl.read_stream("lacrime_incidents_bronze")
    
    # Clean victim age: Remove negative ages and ages > 120
    df_cleaned = df.withColumn("vict_age_clean", 
        when(col("vict_age") < 0, None)
        .when(col("vict_age") > 120, None)
        .otherwise(col("vict_age"))
    )
    
    # Clean latitude: Remove zero coordinates and out-of-bounds values
    # LA County bounds: Latitude 33.7°N to 34.3°N
    df_cleaned = df_cleaned.withColumn("lat_clean",
        when((col("lat") == 0) & (col("lon") == 0), None)  # Zero coordinates
        # .when((col("lat") < 33.7) | (col("lat") > 34.3), None)  # Out of bounds
        .otherwise(col("lat"))
    )
    
    # Clean longitude: Remove zero coordinates and out-of-bounds values
    # LA County bounds: Longitude -118.7°W to -118.1°W
    df_cleaned = df_cleaned.withColumn("lon_clean",
        when((col("lat") == 0) & (col("lon") == 0), None)  # Zero coordinates
        # .when((col("lon") < -118.7) | (col("lon") > -118.1), None)  # Out of bounds
        .otherwise(col("lon"))
    )
    
    # Derive age group from vict_age_clean
    df_enriched = df_cleaned.withColumn("age_group",
        when(col("vict_age_clean").isNull(), "Unknown")
        .when(col("vict_age_clean") == 0, "Unknown")
        .when(col("vict_age_clean") < 18, "0-17 (Juvenile)")
        .when(col("vict_age_clean") < 25, "18-24")
        .when(col("vict_age_clean") < 35, "25-34")
        .when(col("vict_age_clean") < 45, "35-44")
        .when(col("vict_age_clean") < 55, "45-54")
        .when(col("vict_age_clean") < 65, "55-64")
        .otherwise("65+ (Senior)")
    )
        
    # Parse time_occ into hour and minute
    df_enriched = df_enriched \
        .withColumn("time_str", lpad(col("time_occ").cast("string"), 4, "0")) \
        .withColumn("hour", substring("time_str", 1, 2).cast("int")) \
        .withColumn("minute", substring("time_str", 3, 2).cast("int"))
    
    # Create time periods for analysis
    df_enriched = df_enriched.withColumn("time_period",
        when((col("hour") >= 0) & (col("hour") < 6), "Night")
        .when((col("hour") >= 6) & (col("hour") < 12), "Morning")
        .when((col("hour") >= 12) & (col("hour") < 18), "Afternoon")
        .otherwise("Evening")
    )
    
    # Calculate days between crime occurrence and report
    df_enriched = df_enriched.withColumn("days_to_report", 
        datediff(col("date_rptd"), col("date_occ"))
    )
    
    # Flag: Was a weapon involved in the crime?
    df_enriched = df_enriched.withColumn("is_weapon_involved", 
        col("weapon_used_cd").isNotNull()
    )
    
    # Flag: Is the victim a juvenile (under 18)?
    df_enriched = df_enriched.withColumn("is_juvenile_victim", 
        when(col("vict_age_clean") < 18, True).otherwise(False)
    )
    
    # Flag: Was an arrest made? (status = "AA" for Adult Arrest or "JA" for Juvenile Arrest)
    df_enriched = df_enriched.withColumn("is_arrested", 
        col("status").isin(["AA", "JA"])
    )
    
    # Add Silver layer processing timestamp
    df_enriched = df_enriched.withColumn("silver_processed_datetime", current_timestamp())
    df_enriched = df_enriched.withColumn("created_by", lit("lacrime_incidents_silver_transformation_flow"))

    
    return df_enriched

