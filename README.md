# 🏠 Airbnb + Census Data Integration Pipeline

**Modern ELT Pipeline with Airflow, dbt, and PostgreSQL | Medallion Architecture Implementation**

---

## 📋 Table of Contents

- [Project Overview](#-project-overview)
- [System Architecture](#-system-architecture)
- [Technologies Stack](#-technologies-stack)
- [Pipeline Workflow](#-pipeline-workflow)
- [Project Structure](#-project-structure)
- [Prerequisites](#-prerequisites)
- [Setup & Installation](#-setup--installation)
- [Execution Guide](#-execution-guide)
- [Data Models & Insights](#-data-models--insights)
- [Example Queries & Outputs](#-example-queries--outputs)
- [Project Deliverables](#-project-deliverables)
- [Credits](#-credits)

---

## 🎯 Project Overview

This project implements a production-ready ELT (Extract, Load, Transform) data pipeline that integrates Airbnb listing data with Australian Census data to enable comprehensive market analysis. The pipeline orchestrates data ingestion from multiple sources, applies incremental transformations using dbt, and builds analytical models following the Medallion Architecture pattern. Built with Apache Airflow for workflow orchestration, dbt for data transformations, and PostgreSQL for data warehousing, this system processes raw listings, applies data quality checks, enriches with demographic data, and delivers business-ready datasets for stakeholder reporting and decision-making.

---

## 🏗️ System Architecture

### Medallion Architecture

This pipeline implements the **Medallion Architecture** pattern, organizing data into three quality tiers:

```
┌─────────────────┐
│  Raw Data       │
│  (CSV Files)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌─────────────────┐
│   BRONZE LAYER  │ ────▶│   SILVER LAYER  │ ────▶  ┌─────────────────┐
│                 │      │                 │        │   GOLD LAYER     │
│ • Raw ingestion │      │ • Data cleaning │        │                 │
│ • Minimal       │      │ • Standardized │        │ • Business      │
│   transformation│      │ • Validated    │        │   metrics       │
│ • Preserve      │      │ • Typed        │        │ • Aggregated    │
│   source data   │      │ • Deduplicated │        │ • Joined        │
└─────────────────┘      └─────────────────┘        │ • Report-ready  │
                                                      └─────────────────┘
```

#### 🔵 Bronze Layer
- **Purpose**: Raw data ingestion and preservation
- **Content**: Untransformed source data from Airbnb listings and Census datasets
- **Characteristics**: 
  - Preserves original data structure
  - Minimal processing (basic partitioning)
  - Serves as audit trail and recovery point

#### ⚪ Silver Layer  
- **Purpose**: Cleaned, validated, and standardized data
- **Content**: 
  - Data quality checks and validation
  - Type casting and schema standardization
  - Deduplication and null handling
- **Characteristics**:
  - Production-ready for transformations
  - Enforces data quality rules
  - Normalized structures

#### 🟡 Gold Layer
- **Purpose**: Business-ready analytical datasets
- **Content**:
  - Joined Airbnb and Census data
  - Aggregated metrics (LGA summaries, monthly facts)
  - Dimensional models for reporting
- **Characteristics**:
  - Optimized for analytics
  - Business-friendly column names
  - Ready for visualization and BI tools

---

## 🛠️ Technologies Stack

### Core Technologies

| Category | Technology | Purpose |
|----------|-----------|---------|
| **🔄 Orchestration** | [Apache Airflow](https://airflow.apache.org/) | Workflow scheduling and task orchestration |
| **🔧 Transformation** | [dbt (data build tool)](https://www.getdbt.com/) | SQL-based transformations and data modeling |
| **💾 Data Warehouse** | [PostgreSQL](https://www.postgresql.org/) | Relational database for data storage and warehousing |
| **☁️ Cloud Platform** | [Google Cloud Platform (GCP)](https://cloud.google.com/) | Cloud infrastructure, storage (Cloud Storage), and compute services |
| **🖥️ Database Tool** | [DBeaver](https://dbeaver.io/) | Universal database management and SQL client for PostgreSQL |
| **🐍 Programming** | Python 3.9+ | Data processing and automation scripts |
| **📊 Data Sources** | Airbnb Listings API, 2016 Australian Census | Raw data sources |

### Key Libraries

- `pandas` - Data manipulation and processing
- `sqlalchemy` - Database connectivity
- `apache-airflow` - Workflow orchestration
- `dbt-postgres` - dbt adapter for PostgreSQL
- `psycopg2` - PostgreSQL database adapter for Python
- `google-cloud-storage` - GCP Cloud Storage client library

### Tools & Platforms

- **GCP (Google Cloud Platform)**: Used for cloud storage (GCS buckets), hosting data files, and scalable infrastructure
- **PostgreSQL**: Enterprise-grade relational database system for data warehousing, supporting complex queries and ACID transactions
- **DBeaver**: Database administration tool for connecting to PostgreSQL, writing SQL queries, and managing database schemas

---

## ⚙️ Pipeline Workflow

### Step-by-Step Execution Flow

```
┌─────────────────────────────────────────────────────────────┐
│                   1. DATA INGESTION                         │
│  ┌──────────────┐      ┌──────────────┐                    │
│  │ Airbnb CSV   │      │ Census CSV   │                    │
│  │ Files        │      │ Files        │                    │
│  └──────┬───────┘      └──────┬───────┘                    │
│         │                      │                             │
│         └──────────┬───────────┘                             │
│                    ▼                                          │
│         ┌──────────────────────┐                            │
│         │  Bronze DAG (Airflow) │                            │
│         │  - Load raw data      │                            │
│         │  - Basic partitioning  │                            │
│         └──────────┬─────────────┘                            │
└────────────────────┼─────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                   2. DATA TRANSFORMATION                     │
│         ┌──────────────────────┐                            │
│         │  Silver DAG (Airflow) │                            │
│         │  - Trigger dbt run    │                            │
│         │  - Data quality checks │                            │
│         └──────────┬─────────────┘                            │
│                    │                                          │
│                    ▼                                          │
│         ┌──────────────────────┐                            │
│         │  dbt Silver Models   │                            │
│         │  - Clean & validate  │                            │
│         │  - Standardize types │                            │
│         └──────────┬─────────────┘                            │
└────────────────────┼─────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                   3. ANALYTICAL MODELS                       │
│         ┌──────────────────────┐                            │
│         │  Gold DAG (Airflow)  │                            │
│         │  - Build marts        │                            │
│         └──────────┬─────────────┘                            │
│                    │                                          │
│                    ▼                                          │
│         ┌──────────────────────┐                            │
│         │  dbt Gold Models      │                            │
│         │  - Join tables        │                            │
│         │  - Create facts/dims  │                            │
│         │  - Aggregate metrics  │                            │
│         └──────────┬─────────────┘                            │
└────────────────────┼─────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                   4. DATA EXPORT                             │
│         ┌──────────────────────┐                            │
│         │  Export Script       │                            │
│         │  - Gold → CSV        │                            │
│         │  - For reporting     │                            │
│         └─────────────────────┘                            │
└─────────────────────────────────────────────────────────────┘
```

### Detailed Workflow

1. **Bronze Layer Processing**
   - Airflow DAG `airbnb_census_bronze.py` executes
   - Loads raw CSV files from **GCP Cloud Storage** into PostgreSQL `bronze` schema
   - Applies basic partitioning by month/LGA
   - Preserves original data without transformation
   - Uses **DBeaver** or SQL client to monitor and validate data ingestion

2. **Silver Layer Transformation**
   - Airflow DAG triggers dbt execution
   - dbt runs Silver models from `dbt/models/silver/`
   - Applies data quality tests
   - Standardizes data types and handles nulls
   - Creates validated intermediate tables

3. **Gold Layer Aggregation**
   - Airflow DAG `build_dbt_airbnb.py` orchestrates Gold layer
   - dbt executes Gold models from `dbt/models/gold/`
   - Joins Airbnb and Census data on LGA codes
   - Creates fact and dimension tables
   - Generates business metrics and summaries

4. **Data Export**
   - Python script exports Gold layer to CSV
   - Prepares data for visualization tools
   - Generates files for stakeholder reports

---

## 📁 Project Structure

```
airbnb-census-elt-pipeline/
│
├── dags/                           # Apache Airflow DAG definitions
│   ├── airbnb_census_bronze.py    # Bronze layer ingestion DAG
│   └── build_dbt_airbnb.py        # Gold layer dbt execution DAG
│
├── dbt/                            # dbt project directory
│   ├── models/
│   │   ├── bronze/                 # Bronze layer models
│   │   ├── silver/                 # Silver layer models
│   │   └── gold/                   # Gold layer analytical models
│   │       ├── airbnb_census_gold_joined.sql
│   │       ├── dm_lga_summary.sql
│   │       └── fact_listing_monthly.sql
│   ├── snapshots/                  # dbt snapshots for SCD Type 2
│   │   └── snapshots.sql
│   └── dbt_project.yml             # dbt project configuration
│
├── sql/                            # Standalone SQL queries
│   ├── part_1.sql                 # Exploratory queries
│   ├── part_4.sql                 # Business analysis queries
│   └── datamarts.sql              # Data mart queries
│
├── scripts/                        # Utility scripts
│   └── export_gold_to_csv.py      # Export Gold layer to CSV
│
├── data/                           # Processed data outputs
│   ├── airbnb_census_gold_joined.csv
│   └── dm_lga_summary.csv
│
├── report/                         # Project documentation
│   ├── BDA_25217353.pdf           # Main project report
│   └── submission_manifest.pdf    # Submission documentation
│
├── README.md                       # This file
└── LICENSE                         # Project license
```

---

## 📦 Prerequisites

Before setting up the project, ensure you have:

- **Python 3.9+** installed
- **PostgreSQL 12+** database server (local or remote)
- **Apache Airflow 2.x** (can be installed via pip)
- **dbt-core** and **dbt-postgres** installed
- **DBeaver** (or similar database client) for PostgreSQL management
- **Git** for version control
- **Google Cloud Platform (GCP)** account and access (for Cloud Storage)
- **GCP Cloud Storage** bucket for source data files

### System Requirements

- **RAM**: Minimum 4GB (8GB recommended)
- **Disk Space**: 5GB+ for data storage
- **OS**: Linux, macOS, or Windows (WSL recommended)

---

## 🚀 Setup & Installation

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/airbnb-census-elt-pipeline.git
cd airbnb-census-elt-pipeline
```

### 2. Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Python Dependencies

```bash
pip install -r requirements.txt
```

**Or install manually:**

```bash
pip install apache-airflow==2.7.0
pip install dbt-postgres==1.6.0
pip install pandas sqlalchemy psycopg2-binary
```

### 4. Configure PostgreSQL Database

Create a PostgreSQL database and user. You can use **DBeaver** or `psql` command-line tool:

```sql
CREATE DATABASE airbnb_census_db;
CREATE USER airflow_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE airbnb_census_db TO airflow_user;
```

**Using DBeaver:**
1. Open DBeaver and create a new PostgreSQL connection
2. Connect to your local or remote PostgreSQL server
3. Execute the above SQL commands in the SQL editor
4. Test the connection and verify database creation

### 5. Configure GCP (Google Cloud Platform)

If using GCP Cloud Storage for source data:

1. **Install Google Cloud SDK:**
   ```bash
   pip install google-cloud-storage
   ```

2. **Set up GCP credentials:**
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account-key.json"
   ```

3. **Create GCS bucket** (if not already created):
   ```bash
   gsutil mb gs://your-bucket-name
   ```

4. **Upload source CSV files to GCS** (or configure Airflow DAG to read from GCS)

### 6. Configure Airflow

Initialize Airflow:

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow db init
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

### 7. Configure dbt

Edit `dbt/profiles.yml` (create if it doesn't exist):

```yaml
airbnb_census:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: airflow_user
      password: *******
      port: 5432
      dbname: airbnb_census_db
      schema: analytics
```

Test connection:

```bash
cd dbt
dbt debug
```

**Using DBeaver to verify PostgreSQL connection:**
1. Connect to `airbnb_census_db` database in DBeaver
2. Verify schemas: `bronze`, `silver`, `gold`
3. Run test queries to confirm database connectivity

---

## ▶️ Execution Guide

### Starting Airflow

1. **Start Airflow Scheduler** (in one terminal):

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow scheduler
```

2. **Start Airflow Webserver** (in another terminal):

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow webserver --port 8080
```

3. **Access Airflow UI**: Open `http://localhost:8080` in your browser
   - Username: `admin`
   - Password: `admin` (or your configured password)

### Running dbt Models

#### Run all models:

```bash
cd dbt
dbt run
```

#### Run specific layer:

```bash
dbt run --select silver.*      # Run all Silver models
dbt run --select gold.*        # Run all Gold models
```

#### Run with tests:

```bash
dbt test
dbt run --select gold.* && dbt test --select gold.*
```

### Executing Export Script

```bash
python scripts/export_gold_to_csv.py
```

This exports Gold layer tables to CSV files in the `data/` directory.

---

## 📊 Data Models & Insights

### Gold Layer Models

#### `airbnb_census_gold_joined`
- **Purpose**: Master dataset joining Airbnb listings with Census demographics
- **Key Joins**: LGA codes, geographic boundaries
- **Metrics**: Revenue, population, median age, household characteristics

#### `dm_lga_summary`
- **Purpose**: Aggregated metrics by Local Government Area (LGA)
- **Granularity**: LGA × Month
- **Metrics**:
  - Average listing prices
  - Total listings count
  - Demographic statistics (median age, household size)
  - Median rent proxy

#### `fact_listing_monthly`
- **Purpose**: Time-series fact table for trend analysis
- **Granularity**: Listing × Month
- **Metrics**: Price changes, availability, review counts

---

## 💡 Example Queries & Outputs

### Example 1: Top Performing LGAs

```sql
-- Query: Identify top 3 and bottom 3 LGAs by average revenue
SELECT 
    lga_name,
    ROUND(AVG(avg_price), 2) AS avg_revenue,
    ROUND(AVG(avg_median_age), 2) AS median_age,
    ROUND(AVG(avg_household_size), 2) AS household_size
FROM gold.dm_lga_summary
GROUP BY lga_name
HAVING lga_name IN (
    SELECT lga_name
    FROM gold.dm_lga_summary
    GROUP BY lga_name
    ORDER BY AVG(avg_price) DESC
    LIMIT 3
)
OR lga_name IN (
    SELECT lga_name
    FROM gold.dm_lga_summary
    GROUP BY lga_name
    ORDER BY AVG(avg_price) ASC
    LIMIT 3
)
ORDER BY avg_revenue DESC;
```

**Sample Output:**

| lga_name | avg_revenue | median_age | household_size |
|----------|-------------|------------|----------------|
| mosman | 477.18 | 42.0 | 2.4 |
| hunters_hill | 325.57 | 43.0 | 2.7 |
| woollahra | 321.89 | 35.0 | 2.4 |
| camden | 107.63 | 33.0 | 3.1 |
| cumberland | 102.35 | 32.0 | 3.2 |
| blacktown | 90.30 | 33.0 | 3.2 |

**Business Insight**: Premium markets (Mosman, Hunters Hill) command 4-5x higher average prices than suburban areas (Blacktown, Cumberland), with demographic differences in age and household composition.

### Example 2: Demographic Correlation Analysis

```sql
-- Query: Correlation between median age and revenue
SELECT
    ROUND(CORR(avg_price, avg_median_age)::numeric, 3) AS corr_medianage_revenue
FROM gold.dm_lga_summary;
```

**Result**: `corr_medianage_revenue = 0.759`

**Business Insight**: Strong positive correlation (0.759) indicates that areas with older populations tend to have higher Airbnb listing prices, suggesting targeting mature demographics for premium properties.

### Example 3: Property Type Performance

```sql
-- Query: Best property-room combinations in top neighborhoods
WITH top5_neighbourhoods AS (
    SELECT suburb
    FROM gold.airbnb_census_gold_joined
    WHERE price > 0
    GROUP BY suburb
    ORDER BY AVG(price) DESC
    LIMIT 5
)
SELECT 
    a.suburb,
    a.property_type,
    a.room_type,
    COUNT(*) AS total_listings,
    ROUND(AVG(a.price), 2) AS avg_revenue,
    ROUND(AVG(a.num_reviews), 2) AS avg_reviews
FROM gold.airbnb_census_gold_joined a
JOIN top5_neighbourhoods t ON a.suburb = t.suburb
GROUP BY a.suburb, a.property_type, a.room_type
ORDER BY avg_revenue DESC
LIMIT 10;
```

**Business Insight**: Villas and entire home listings in premium neighborhoods generate the highest revenue, with distinct patterns between property types and room configurations.

---

## 📦 Project Deliverables

### Code Artifacts
- ✅ Airflow DAGs for orchestration
- ✅ dbt models (Bronze → Silver → Gold)
- ✅ Data export scripts
- ✅ SQL analysis queries

### Documentation
- ✅ Technical report (`report/BDA_25217353.pdf`)
- ✅ Submission manifest
- ✅ README with setup instructions

### Data Outputs
- ✅ Processed Gold layer CSVs
- ✅ Analytical datasets ready for visualization

---

## 👤 Credits

**Satyam Palkar (25217353)**  
**Master of IT (Data Analytics)**  
**University of Technology Sydney**

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🔗 Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [dbt Documentation](https://docs.getdbt.com/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Medallion Architecture Overview](https://www.databricks.com/glossary/medallion-architecture)

---

