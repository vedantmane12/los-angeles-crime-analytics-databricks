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
# MAGIC ## Loading Dimension Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load DIM_STATUS

# COMMAND ----------

pl.create_streaming_table(
    name="dim_status",
    comment="Status dimension - Case investigation status codes and descriptions (streaming)"
)

@pl.append_flow(
    target="dim_status",
    name="dim_status_flow",
    comment="Creates status dimension from distinct status values in Silver layer (streaming mode)"
)
def dim_status_flow():    
    # Read from Silver layer using STREAMING mode
    df_silver = pl.read_stream("lacrime_incidents_silver")
    
    # Get distinct status values using dropDuplicates (works in streaming)
    dim = df_silver.select(
        "status", 
        "status_desc"
    ).filter(col("status").isNotNull()) \
     .dropDuplicates(["status"])  # Streaming-compatible deduplication
    
    # Create surrogate key using hash (deterministic and streaming-compatible)
    dim = dim.withColumn("status_key", 
        abs(hash(col("status"))) % 1000000  # Hash-based key (6 digits)
    )
    
    # Add audit columns
    dim = dim.withColumn("created_by", lit("dim_status_flow")) \
             .withColumn("created_date", current_timestamp())
    
    # Select final columns in proper order
    return dim.select(
        "status_key",      # Surrogate key (hash-based)
        "status",          # Natural key
        "status_desc",     # Description
        "created_by",      # Audit
        "created_date"     # Audit
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load DIM_LOCATION

# COMMAND ----------

pl.create_streaming_table(
    name="dim_location",
    comment="Location dimension - LAPD geographic areas and reporting districts"
)

@pl.append_flow(
    target="dim_location",
    name="dim_location_flow",
    comment="Creates location dimension from distinct areas in Silver layer"
)
def dim_location_flow():
    """
    Creates location dimension with 21 LAPD areas and geographic regions.
    """
    df_silver = pl.read_stream("lacrime_incidents_silver")
    
    # Get distinct location values
    dim = df_silver.select(
        "area",
        "area_name",
        "rpt_dist_no"
    ).filter(col("area").isNotNull()) \
     .dropDuplicates(["area", "rpt_dist_no"])
    
    # Add geographic regions (grouping of areas)
    dim = dim.withColumn("geographic_region",
        when(col("area").isin([1, 2, 4, 13]), "Central")
        .when(col("area").isin([6, 7, 20]), "West")
        .when(col("area").isin([9, 10, 15, 17, 21]), "Valley")
        .when(col("area").isin([3, 8, 12, 18]), "South")
        .otherwise("Other")
    )
    
    # Hash-based surrogate key (using area + district for uniqueness)
    dim = dim.withColumn("location_key", 
        abs(hash(concat(col("area"), lit("_"), col("rpt_dist_no")))) % 10000000
    )
    
    # Audit columns
    dim = dim.withColumn("created_by", lit("dim_location_flow")) \
             .withColumn("created_date", current_timestamp())
    
    return dim.select(
        "location_key",
        "area",
        "area_name",
        "rpt_dist_no",
        "geographic_region",
        "created_by",
        "created_date"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load DIM_PREMISE

# COMMAND ----------

pl.create_streaming_table(
    name="dim_premise",
    comment="Premise dimension - Location types where crimes occurred"
)

@pl.append_flow(
    target="dim_premise",
    name="dim_premise_flow",
    comment="Creates premise dimension from distinct premise types in Silver layer"
)
def dim_premise_flow():
    """
    Creates premise dimension with premise categories.
    """
    df_silver = pl.read_stream("lacrime_incidents_silver")
    
    # Get distinct premise values
    dim = df_silver.select(
        "premis_cd",
        "premis_desc"
    ).filter(col("premis_cd").isNotNull()) \
     .dropDuplicates(["premis_cd"])
    
    # Categorize premises
    dim = dim.withColumn("premise_category",
        when(col("premis_desc").rlike("(?i)DWELLING|HOUSE|APARTMENT|RESIDENCE"), "Residential")
        .when(col("premis_desc").rlike("(?i)STREET|SIDEWALK|ALLEY|PARKING"), "Street")
        .when(col("premis_desc").rlike("(?i)STORE|RESTAURANT|BANK|MARKET"), "Commercial")
        .when(col("premis_desc").rlike("(?i)VEHICLE|CAR"), "Vehicle")
        .otherwise("Other")
    )
    
    # Hash-based surrogate key
    dim = dim.withColumn("premise_key", 
        abs(hash(col("premis_cd"))) % 1000000
    )
    
    # Audit columns
    dim = dim.withColumn("created_by", lit("dim_premise_flow")) \
             .withColumn("created_date", current_timestamp())
    
    return dim.select(
        "premise_key",
        "premis_cd",
        "premis_desc",
        "premise_category",
        "created_by",
        "created_date"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load DIM_WEAPON

# COMMAND ----------

pl.create_streaming_table(
    name="dim_weapon",
    comment="Weapon dimension - Weapons used in crimes (including No Weapon option)"
)

@pl.append_flow(
    target="dim_weapon",
    name="dim_weapon_flow",
    comment="Creates weapon dimension including 'No Weapon' for crimes without weapons"
)
def dim_weapon_flow():
    """
    Creates weapon dimension with weapon categories.
    
    FIXED APPROACH:
    - No union needed
    - Just deduplicate weapons from Silver
    - "No Weapon" will be handled in fact table (weapon_key = -1 when weapon_used_cd IS NULL)
    """
    df_silver = pl.read_stream("lacrime_incidents_silver")
    
    # Get distinct weapon values (where weapon exists)
    # Include both NULL and non-NULL to create complete weapon dimension
    dim = df_silver.select(
        "weapon_used_cd",
        "weapon_desc"
    ).dropDuplicates(["weapon_used_cd"])
    
    # Add weapon categories
    # dim = dim.withColumn("weapon_category",
    #     when(col("weapon_used_cd").isNull(), "None")
    #     .when(col("weapon_desc").rlike("(?i)HAND GUN|SEMI-AUTO|REVOLVER|RIFLE|SHOTGUN"), "Firearm")
    #     .when(col("weapon_desc").rlike("(?i)KNIFE|CUTTING"), "Knife")
    #     .when(col("weapon_desc").rlike("(?i)CLUB|PIPE|ROCK|BOTTLE"), "Blunt")
    #     .when(col("weapon_desc").rlike("(?i)STRONG-ARM|HANDS|FEET"), "Body")
    #     .otherwise("Other")
    # )
    
    # Add "No Weapon" description for NULL values
    dim = dim.withColumn("weapon_desc",
        when(col("weapon_used_cd").isNull(), "No Weapon")
        .otherwise(col("weapon_desc"))
    )
    
    # Hash-based surrogate key (handle NULL as -1)
    dim = dim.withColumn("weapon_key", 
        when(col("weapon_used_cd").isNull(), -1)
        .otherwise(abs(hash(col("weapon_used_cd"))) % 1000000)
    )
    
    # Set weapon_used_cd to -1 for NULL (consistency)
    dim = dim.withColumn("weapon_used_cd",
        when(col("weapon_used_cd").isNull(), -1.0)
        .otherwise(col("weapon_used_cd"))
    )
    
    # Audit columns
    dim = dim.withColumn("created_by", lit("dim_weapon_flow")) \
             .withColumn("created_date", current_timestamp())
    
    return dim.select(
        "weapon_key",
        "weapon_used_cd",
        "weapon_desc",
        # "weapon_category",
        "created_by",
        "created_date"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load DIM_CRIME_TYPE

# COMMAND ----------

pl.create_streaming_table(
    name="dim_crime_type",
    comment="Crime type dimension - Crime codes and classifications"
)

@pl.append_flow(
    target="dim_crime_type",
    name="dim_crime_type_flow",
    comment="Creates crime type dimension with crime categories"
)
def dim_crime_type_flow():
    """
    Creates crime type dimension with categories (Violent/Property/Drug/Other).
    """
    df_silver = pl.read_stream("lacrime_incidents_silver")
    
    # Get distinct crime type values
    dim = df_silver.select(
        "crm_cd",
        "crm_cd_desc",
        "part_1_2"
    ).filter(col("crm_cd").isNotNull()) \
     .dropDuplicates(["crm_cd"])
    
    # Categorize crimes
    # dim = dim.withColumn("crime_category",
    #     when(col("crm_cd_desc").rlike("(?i)ASSAULT|BATTERY|HOMICIDE|RAPE|ROBBERY"), "Violent")
    #     .when(col("crm_cd_desc").rlike("(?i)THEFT|BURGLARY|VANDALISM|STOLEN"), "Property")
    #     .when(col("crm_cd_desc").rlike("(?i)DRUG|NARCOTIC"), "Drug")
    #     .otherwise("Other")
    # )
    
    # Flag violent crimes
    # dim = dim.withColumn("is_violent", col("crime_category") == "Violent")
    
    # Hash-based surrogate key
    dim = dim.withColumn("crime_type_key", 
        abs(hash(col("crm_cd"))) % 1000000
    )
    
    # Audit columns
    dim = dim.withColumn("created_by", lit("dim_crime_type_flow")) \
             .withColumn("created_date", current_timestamp())
    
    return dim.select(
        "crime_type_key",
        "crm_cd",
        "crm_cd_desc",
        "part_1_2",
        # "crime_category",
        # "is_violent",
        "created_by",
        "created_date"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load DIM_VICTIM_DEMOGRAPHICS

# COMMAND ----------

pl.create_streaming_table(
    name="dim_victim_demographics",
    comment="Victim demographics dimension - Age groups, sex, and descent"
)

@pl.append_flow(
    target="dim_victim_demographics",
    name="dim_victim_demographics_flow",
    comment="Creates victim demographics dimension"
)
def dim_victim_demographics_flow():
    """
    Creates victim demographics dimension with age groups and ethnicity descriptions.
    """
    df_silver = pl.read_stream("lacrime_incidents_silver")
    
    # Get distinct demographic combinations
    dim = df_silver.select(
        "vict_age_clean",
        "age_group",
        "vict_sex",
        "vict_descent"
    ).dropDuplicates(["vict_age_clean", "vict_sex", "vict_descent"])
    
    # Sex description
    dim = dim.withColumn("sex_desc",
        when(col("vict_sex") == "M", "Male")
        .when(col("vict_sex") == "F", "Female")
        .when(col("vict_sex") == "X", "Unknown")
        .when(col("vict_sex") == "H", "Non-binary")
        .otherwise("Unknown")
    )
    
    # Descent description
    dim = dim.withColumn("descent_desc",
        when(col("vict_descent") == "H", "Hispanic/Latin/Mexican")
        .when(col("vict_descent") == "W", "White")
        .when(col("vict_descent") == "B", "Black")
        .when(col("vict_descent") == "A", "Other Asian")
        .when(col("vict_descent") == "O", "Other")
        .when(col("vict_descent") == "X", "Unknown")
        .when(col("vict_descent") == "C", "Chinese")
        .when(col("vict_descent") == "K", "Korean")
        .when(col("vict_descent") == "F", "Filipino")
        .when(col("vict_descent") == "J", "Japanese")
        .when(col("vict_descent") == "V", "Vietnamese")
        .when(col("vict_descent") == "I", "American Indian/Alaskan Native")
        .when(col("vict_descent") == "Z", "Asian Indian")
        .otherwise("Unknown")
    )
    
    # Hash-based surrogate key (composite of age, sex, descent)
    dim = dim.withColumn("victim_demographic_key", 
        abs(hash(concat(
            coalesce(col("vict_age_clean").cast("string"), lit("null")),
            lit("_"),
            coalesce(col("vict_sex"), lit("null")),
            lit("_"),
            coalesce(col("vict_descent"), lit("null"))
        ))) % 10000000
    )
    
    # Audit columns
    dim = dim.withColumn("created_by", lit("dim_victim_demographics_flow")) \
             .withColumn("created_date", current_timestamp())
    
    return dim.select(
        "victim_demographic_key",
        "vict_age_clean",
        "age_group",
        "vict_sex",
        "sex_desc",
        "vict_descent",
        "descent_desc",
        "created_by",
        "created_date"
    )
