# Databricks notebook source
#Libraries management
from pyspark import pipelines as pl
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark.sql("USE CATALOG `workspace`")
spark.sql("USE SCHEMA `la_crime_schema`")

# COMMAND ----------

# Environment Setup
catalog = "workspace"
schema = "la_crime_schema"
volume = "datastore"

# COMMAND ----------

# Fact Table Load
pl.create_streaming_table(
    name="fact_crime_incidents",
    comment="Fact table - Crime incidents with foreign keys to all dimensions and measures"
)

@pl.append_flow(
    target="fact_crime_incidents",
    name="fact_crime_incidents_flow",
    comment="Creates fact table by joining Silver with all dimension tables"
)
def fact_crime_incidents_flow():
    """
    Creates fact table with foreign keys to all 8 dimensions.
    
    KEY FIXES: 
    - Read Silver as STREAMING (pl.read_stream)
    - Read ALL dimensions as BATCH (dlt.read)
    - Use hash(dr_no) for surrogate key instead of monotonically_increasing_id()
    
    Grain: One row per crime incident (dr_no is unique)
    """
    
    #  Read Silver layer as STREAMING
    df_silver = pl.read_stream("lacrime_incidents_silver")
    
    #  Read ALL dimensions as BATCH (DLT materializes streaming tables for us)
    dim_date = dlt.read("dim_date")
    dim_time = dlt.read("dim_time")
    dim_location = dlt.read("dim_location")
    dim_crime_type = dlt.read("dim_crime_type")
    dim_victim = dlt.read("dim_victim_demographics")
    dim_premise = dlt.read("dim_premise")
    dim_weapon = dlt.read("dim_weapon")
    dim_status = dlt.read("dim_status")
    
    # Start with Silver layer
    fact = df_silver
    
    # ========================================================================
    # JOIN 1: dim_date for date_occurred (date_occ)
    # ========================================================================
    fact = fact.join(
        dim_date.select(
            col("date_key").alias("date_occurred_key"),
            col("full_date").alias("date_occ_join")
        ),
        fact.date_occ.cast("date") == col("date_occ_join"),
        "left"
    ).drop("date_occ_join")
    
    # ========================================================================
    # JOIN 2: dim_date for date_reported (date_rptd)
    # ========================================================================
    fact = fact.join(
        dim_date.select(
            col("date_key").alias("date_reported_key"),
            col("full_date").alias("date_rptd_join")
        ),
        fact.date_rptd.cast("date") == col("date_rptd_join"),
        "left"
    ).drop("date_rptd_join")
    
    # ========================================================================
    # JOIN 3: dim_time for time_occurred
    # ========================================================================
    fact = fact.join(
        dim_time.select(
            col("time_key").alias("time_occurred_key"),
            col("time_occ").alias("time_occ_join")
        ),
        fact.time_occ == col("time_occ_join"),
        "left"
    ).drop("time_occ_join")
    
    # ========================================================================
    # JOIN 4: dim_location (area + rpt_dist_no)
    # ========================================================================
    fact = fact.join(
        dim_location.select("location_key", "area", "rpt_dist_no"),
        (fact.area == dim_location.area) & 
        (fact.rpt_dist_no == dim_location.rpt_dist_no),
        "left"
    ).drop(dim_location.area, dim_location.rpt_dist_no)
    
    # ========================================================================
    # JOIN 5: dim_crime_type (crm_cd)
    # ========================================================================
    fact = fact.join(
        dim_crime_type.select("crime_type_key", "crm_cd"),
        fact.crm_cd == dim_crime_type.crm_cd,
        "left"
    ).drop(dim_crime_type.crm_cd)
    
    # ========================================================================
    # JOIN 6: dim_victim_demographics (age + sex + descent)
    # ========================================================================
    # Use coalesce for NULL-safe joins
    fact = fact.join(
        dim_victim.select("victim_demographic_key", "vict_age_clean", "vict_sex", "vict_descent"),
        (coalesce(fact.vict_age_clean, lit(-999)) == coalesce(dim_victim.vict_age_clean, lit(-999))) &
        (coalesce(fact.vict_sex, lit("NULL")) == coalesce(dim_victim.vict_sex, lit("NULL"))) &
        (coalesce(fact.vict_descent, lit("NULL")) == coalesce(dim_victim.vict_descent, lit("NULL"))),
        "left"
    ).drop(dim_victim.vict_age_clean, dim_victim.vict_sex, dim_victim.vict_descent)
    
    # ========================================================================
    # JOIN 7: dim_premise (premis_cd)
    # ========================================================================
    fact = fact.join(
        dim_premise.select("premise_key", "premis_cd"),
        fact.premis_cd == dim_premise.premis_cd,
        "left"
    ).drop(dim_premise.premis_cd)
    
    # ========================================================================
    # JOIN 8: dim_weapon (weapon_used_cd, handle NULLs as -1)
    # ========================================================================
    # Create weapon code for join (NULL → -1.0 to match dim_weapon)
    fact = fact.withColumn("weapon_cd_for_join", 
        when(col("weapon_used_cd").isNull(), -1.0)
        .otherwise(col("weapon_used_cd"))
    )
    
    fact = fact.join(
        dim_weapon.select("weapon_key", "weapon_used_cd"),
        col("weapon_cd_for_join") == dim_weapon.weapon_used_cd,
        "left"
    ).drop("weapon_cd_for_join", dim_weapon.weapon_used_cd)
    
    # ========================================================================
    # JOIN 9: dim_status (status)
    # ========================================================================
    fact = fact.join(
        dim_status.select("status_key", "status"),
        fact.status == dim_status.status,
        "left"
    ).drop(dim_status.status)
    
    # ========================================================================
    # CREATE FINAL FACT TABLE
    # ========================================================================
    
    fact_final = fact.select(
        # ✅ Surrogate key using hash (streaming-compatible)
        abs(hash(col("dr_no"))).cast("long").alias("incident_key"),
        
        # Natural/Business key
        col("dr_no"),
        
        # ===== FOREIGN KEYS TO DIMENSIONS =====
        col("date_occurred_key"),
        col("date_reported_key"),
        col("time_occurred_key"),
        col("location_key"),
        col("crime_type_key"),
        col("victim_demographic_key"),
        col("premise_key"),
        col("weapon_key"),
        col("status_key"),
        
        # ===== DEGENERATE DIMENSIONS (High cardinality attributes) =====
        col("location").alias("location_address"),
        col("cross_street"),
        col("lat_clean").alias("latitude"),
        col("lon_clean").alias("longitude"),
        col("mocodes"),
        
        # ===== MEASURES (Numeric values for aggregation) =====
        col("days_to_report"),
        lit(1).alias("crime_count"),  # For COUNT aggregations
        
        # ===== BOOLEAN FLAGS (For filtering and analysis) =====
        col("is_weapon_involved"),
        col("is_juvenile_victim"),
        col("is_arrested"),
        
        # ===== AUDIT COLUMNS =====
        col("ingestion_datetime").alias("bronze_load_datetime"),
        col("silver_processed_datetime"),
        current_timestamp().alias("gold_processed_datetime"),
        lit("fact_crime_incidents_flow").alias("created_by")
    )
    
    return fact_final
