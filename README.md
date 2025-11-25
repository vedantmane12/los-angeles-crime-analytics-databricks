# Los Angeles Crime Analytics Project
## Data Engineering Project using Databricks and Tableau

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)](https://databricks.com/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD4?style=flat&logo=delta&logoColor=white)](https://delta.io/)
[![Tableau](https://img.shields.io/badge/Tableau-E97627?style=flat&logo=tableau&logoColor=white)](https://www.tableau.com/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apache-spark&logoColor=white)](https://spark.apache.org/)

> **A production-grade crime analytics platform built with Medallion Architecture, processing 1M+ crime incidents from the LA Open Data Portal to deliver actionable insights through interactive dashboards.**

---

## 📋 **Table of Contents**
- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Data Pipeline](#data-pipeline)
- [Features](#features)
- [Business Questions](#business-questions)
- [Setup & Installation](#setup--installation)
- [Project Structure](#project-structure)
- [Results](#results)
- [Future Enhancements](#future-enhancements)
- [Author](#author)

---

## 🎯 **Overview**

**LA Crime Analytics** is an end-to-end data engineering project that transforms raw crime data from the Los Angeles Police Department into actionable business intelligence. The project implements the **Medallion Architecture** (Bronze → Silver → Gold) using **Databricks Delta Live Tables** and delivers insights through interactive **Tableau dashboards**.

### **Project Highlights**

- 📊 **1M+ crime records** processed (2020-2025)
- 🏗️ **Medallion Architecture** with 3 data quality layers
- ⭐ **Star Schema** with 8 dimensions + 1 fact table
- ⚡ **Delta Live Tables** for automated pipeline orchestration
- 📈 **Interactive Dashboards** answering 10+ business questions
- ✅ **100% data quality** with automated validation

### **Key Metrics**

```
📊 Dataset Size:        1,004,991 crime incidents
📅 Time Range:          2020-01-01 to 2025-05-29 (5+ years)
🗂️  Total Tables:       11 (1 Bronze + 1 Silver + 9 Gold)
⚡ Pipeline Duration:   1 minute 48 seconds
✅ Data Quality:        100% (all expectations passed)
🎯 Business Questions:  10+ answered with visualizations
```

---

## 🏗️ **Architecture**

### **Medallion Architecture**

The project implements the industry-standard **Medallion Architecture** for data quality and governance:

```
┌─────────────────────────────────────────────────────────────┐
│  DATA SOURCE                                                │
│  LA Open Data Portal API                                    │
│  https://data.lacity.org/resource/2nrs-mtv8.csv            │
└─────────────────────┬───────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────────────────┐
│  🥉 BRONZE LAYER - Raw Data Archive                         │
│  ├─ lacrime_incidents_bronze (1M rows)                     │
│  ├─ Auto Loader ingestion                                  │
│  ├─ Schema inference                                       │
│  └─ Audit columns                                          │
└─────────────────────┬───────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────────────────┐
│  🥈 SILVER LAYER - Cleaned & Enriched                       │
│  ├─ lacrime_incidents_silver (1M rows)                     │
│  ├─ Data cleaning (ages, coordinates)                      │
│  ├─ Data enrichment (17 new columns)                       │
│  ├─ Quality expectations (3 rules)                         │
│  └─ Derived measures & flags                               │
└─────────────────────┬───────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────────────────┐
│  🥇 GOLD LAYER - Dimensional Model (Star Schema)            │
│  ├─ DIMENSIONS (8 tables, ~10K rows total)                 │
│  │  ├─ dim_date (4K) - Calendar dimension                  │
│  │  ├─ dim_time (1.4K) - Time of day                       │
│  │  ├─ dim_location (1.2K) - LAPD areas                    │
│  │  ├─ dim_crime_type (140) - Crime classifications        │
│  │  ├─ dim_victim_demographics (2.9K) - Age/sex/ethnicity  │
│  │  ├─ dim_premise (314) - Location types                  │
│  │  ├─ dim_weapon (80) - Weapons used                      │
│  │  └─ dim_status (6) - Case statuses                      │
│  │                                                          │
│  └─ FACT TABLE (1M rows)                                   │
│     └─ fact_crime_incidents - Star schema center           │
│        ├─ 9 foreign keys to dimensions                     │
│        ├─ 5 degenerate dimensions                          │
│        ├─ 2 measures                                        │
│        └─ 3 boolean flags                                   │
└─────────────────────┬───────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────────────────┐
│  📊 VISUALIZATION LAYER                                      │
│  Tableau Dashboards (6 dashboards, 20+ charts)             │
│  ├─ Crime Trends Over Time                                  │
│  ├─ Geographic Hotspots                                     │
│  ├─ Temporal Patterns                                       │
│  ├─ Weapon Analysis                                         │
│  ├─ Demographic Insights                                    │
│  └─ Arrest Performance                                      │
└─────────────────────────────────────────────────────────────┘
```

---

## 🛠️ **Tech Stack**

### **Data Engineering**

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Platform** | Databricks Community Edition | Cloud data platform |
| **Compute** | Serverless | Scalable compute resources |
| **Storage** | Unity Catalog + Delta Lake | ACID-compliant data storage |
| **Orchestration** | Delta Live Tables (DLT) | Pipeline automation |
| **Processing** | PySpark | Distributed data processing |
| **Language** | Python 3.x | Transformation logic |

### **Data Visualization**

| Component | Technology | Purpose |
|-----------|------------|---------|
| **BI Tool** | Tableau Desktop | Interactive dashboards |
| **Connection** | JDBC/ODBC | Databricks connector |

### **Data Governance**

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Catalog** | Unity Catalog | Metadata management |
| **Quality** | DLT Expectations | Data validation rules |
| **Lineage** | DLT Pipeline Graph | End-to-end traceability |

---

## 🔄 **Data Pipeline**

### **Pipeline Stages**

#### **Stage 1: Data Profiling**
- **Tool:** Databricks Notebook
- **Purpose:** Understand data quality and characteristics
- **Output:** Profiling report with quality metrics
- **Key Findings:**
  - 80% overall completeness
  - 100% completeness on critical fields
  - 1M+ records, 28 columns
  - 5+ years of crime data (2020-2025)

#### **Stage 2: Bronze Layer (Raw Ingestion)**
- **Tool:** Delta Live Tables (Auto Loader)
- **Input:** CSV from Unity Catalog Volume
- **Output:** `lacrime_incidents_bronze` (1M rows, 31 columns)
- **Features:**
  - Streaming ingestion with cloudFiles
  - Schema inference and evolution
  - Audit columns (ingestion_datetime, source_filename)
- **Processing Time:** 8 seconds

#### **Stage 3: Silver Layer (Cleaning & Enrichment)**
- **Input:** lacrime_incidents_bronze
- **Output:** `lacrime_incidents_silver` (1M rows, 48 columns)
- **Transformations:**
  - **Cleaning:** Fix negative ages, invalid coordinates
  - **Enrichment:** Age groups, time periods, reporting lag
  - **Flags:** is_weapon_involved, is_juvenile_victim, is_arrested
- **Quality:** 3 DLT expectations (100% pass rate)
- **Processing Time:** 7 seconds

#### **Stage 4: Gold Layer - Dimensions**
- **Input:** lacrime_incidents_silver
- **Output:** 8 dimension tables
- **Approach:** 
  - Streaming tables for data-driven dimensions
  - Batch materialized views for reference dimensions (date, time)
- **Total Rows:** ~10,000 across all dimensions
- **Processing Time:** 1-4 seconds per dimension

#### **Stage 5: Gold Layer - Fact Table**
- **Input:** Silver + All 8 dimensions
- **Output:** `fact_crime_incidents` (1M rows, 26 columns)
- **Joins:** 9 left joins to populate foreign keys
- **Processing Time:** 5-10 minutes

#### **Stage 6: Visualization**
- **Tool:** Tableau Desktop
- **Connection:** JDBC to Databricks SQL Warehouse
- **Dashboards:** 6 dashboards with 20+ visualizations
- **Delivery:** Interactive reports for stakeholders

---

## 🚀 **Setup & Installation**

### **Quick Start**

#### **1. Clone Repository**
```bash
git clone https://github.com/vedantmane/la-crime-analytics.git
cd la-crime-analytics
```

#### **2. Setup Databricks**

**Create Unity Catalog Structure:**
```sql
CREATE CATALOG IF NOT EXISTS workspace;
CREATE SCHEMA IF NOT EXISTS workspace.la_crime_schema;
CREATE VOLUME IF NOT EXISTS workspace.la_crime_schema.datastore;
```

**Upload Data:**
```python
import pandas as pd

# Download crime data
url = "https://data.lacity.org/resource/2nrs-mtv8.csv?$limit=10000000"
df = pd.read_csv(url)

# Save to Unity Catalog Volume
df.to_csv("/Volumes/workspace/la_crime_schema/datastore/crime_data_raw.csv", index=False)
```

#### **3. Create DLT Pipeline**

1. Navigate to **Workflows** → **Delta Live Tables** → **Create Pipeline**
2. Configure:
   ```
   Pipeline Name: LA Crime Analytics
   Notebook: notebooks/etl_pipeline.py
   Target: workspace.la_crime_schema
   Cluster Mode: Serverless
   ```
3. Click **Create** → **Start**

#### **4. Verify Tables**

```sql
-- Check all tables created
SHOW TABLES IN workspace.la_crime_schema;

-- Verify row counts
SELECT 'Bronze' as layer, COUNT(*) as rows FROM workspace.la_crime_schema.lacrime_incidents_bronze
UNION ALL SELECT 'Silver', COUNT(*) FROM workspace.la_crime_schema.lacrime_incidents_silver
UNION ALL SELECT 'Fact', COUNT(*) FROM workspace.la_crime_schema.fact_crime_incidents;
```

#### **5. Connect Tableau**

1. Open Tableau Desktop
2. Connect to **Databricks**
3. Enter your SQL Warehouse connection details
4. Import star schema (fact + 8 dimensions)
5. Build dashboards!

---

## 🥉 **Data Pipeline Details**

### **Bronze Layer**
**Purpose:** Raw data archive (immutable)

```python
pl.create_streaming_table("lacrime_incidents_bronze")

@pl.append_flow(target="lacrime_incidents_bronze")
def bronze_ingest():
    return spark.readStream.format("cloudFiles") \
                .option("cloudFiles.format", "csv") \
                .load("/Volumes/workspace/la_crime_schema/datastore/")
```

**Output:** 1,004,991 rows × 31 columns

---

### **Silver Layer**
**Purpose:** Cleaned, validated, enriched data

**Transformations:**
- Clean invalid ages (negative → NULL)
- Clean invalid coordinates (0,0 → NULL)
- Create age groups (Juvenile, 18-24, 25-34, etc.)
- Parse time periods (Night, Morning, Afternoon, Evening)
- Calculate reporting lag
- Add boolean flags (weapon_involved, arrested, juvenile_victim)

**Quality Expectations:**
```python
expect_all_or_drop={
    "valid_primary_key": "dr_no IS NOT NULL",
    "valid_date_logic": "date_rptd >= date_occ OR ...",
    "valid_date_range": "date_occ >= '2020-01-01'"
}
```

**Output:** 1,004,991 rows × 48 columns (100% retention)

---

### **Gold Layer - Star Schema**

**8 Dimension Tables:**

| Dimension | Rows | Purpose |
|-----------|------|---------|
| **dim_date** | 4,018 | Calendar (2019-2029) |
| **dim_time** | 1,440 | Time of day (all minutes) |
| **dim_location** | 1,210 | LAPD areas & districts |
| **dim_crime_type** | 140 | Crime classifications |
| **dim_victim_demographics** | 2,900 | Age/sex/ethnicity combos |
| **dim_premise** | 314 | Location types |
| **dim_weapon** | 80 | Weapons used |
| **dim_status** | 6 | Case statuses |

**1 Fact Table:**

| Table | Rows | Grain |
|-------|------|-------|
| **fact_crime_incidents** | 1,004,991 | One row per crime |

**Schema:**
- 9 foreign keys to dimensions
- 5 degenerate dimensions (lat, lon, address, etc.)
- 2 measures (days_to_report, crime_count)
- 3 boolean flags (weapon, juvenile, arrested)

---

## 🎨 **Tableau Dashboards**

### **Dashboard 1: Crime Trends Over Time**
- 📈 Yearly trend line with YoY % change
- 📊 Monthly heatmap
- 📉 Quarterly bar chart with comparisons

### **Dashboard 2: Geographic Hotspots**
- 🗺️ Interactive map with crime density
- 📍 Top 10 areas bar chart
- 🏙️ Regional performance tree map

### **Dashboard 3: Temporal Patterns**
- 📅 Day of week analysis
- ⏰ Hourly crime distribution
- 🌡️ Crime type × time period heatmap

### **Dashboard 4: Weapon Analysis**
- 🔫 Top 20 weapons used
- 📊 Weapon categories (pie chart)
- 📈 Weapon trends over time

### **Dashboard 5: Demographic Insights**
- 👥 Age group distribution
- 🚻 Gender breakdown
- 🌍 Ethnicity patterns

### **Dashboard 6: Arrest Performance**
- ⚖️ Overall arrest rate KPI
- 📊 Juvenile vs Adult arrests
- 📈 Arrest trends by area

---

## 📚 **Documentation**

Detailed documentation available in `/docs`:

- [Data Profiling Report](docs/data_profiling_report.md) - Complete quality analysis
- [Architecture Guide](docs/architecture.md) - Medallion architecture deep dive
- [Star Schema Design](docs/star_schema.md) - Dimensional model documentation
- [Pipeline Operations](docs/pipeline_guide.md) - How to run and maintain
- [Query Examples](sql/business_questions.sql) - Sample analytical queries

---


## 🙏 **Acknowledgments**

- **LA Open Data Portal** - For providing public crime data
- **Northeastern University** - Academic support and resources
- **Databricks Community** - Documentation and tutorials

---

## 📄 **License**

This project is created for educational purposes as part of coursework at Northeastern University.

**Data Source:** Los Angeles Open Data Portal (Public Domain)

---

## 📞 **Support**

For questions or issues:
1. Check the [documentation](docs/)
2. Review [sample queries](sql/business_questions.sql)
3. Open an issue on GitHub
4. Contact the author

---

## ⭐ **If You Find This Helpful**

If this project helped you learn about:
- Medallion Architecture
- Delta Live Tables
- Dimensional Modeling
- Databricks

**Please give it a ⭐ star on GitHub!**

---

**Built with ❤️ using Databricks, Delta Lake, and Tableau**

