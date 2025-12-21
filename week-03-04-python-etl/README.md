# Week 03-04: Python ETL Pipeline

## 🎯 Learning Objectives

By the end of this module, you will be able to:

- Build a 3-layer ETL pipeline (Raw → Staging → Production)
- Handle data quality issues systematically
- Implement data validation and quality checks
- Create business metrics and aggregations
- Debug and troubleshoot ETL pipelines

## 📊 ETL Architecture

Parquet Files → RAW Layer → STAGING Layer → PRODUCTION Layer (raw_data/) (Immutable) (Cleaned) (Aggregated)
customers/ raw.customers → staging.customers → prod.customer_metrics products/ raw.products → staging.products orders/ raw.orders → staging.orders → prod.daily_sales order_items/ raw.order_items→ staging.order_items prod.monthly_sales prod.daily_category_metrics prod.daily_product_metrics

**Layer Characteristics:**

**RAW (Bronze):**

- Append-only, immutable
- Contains errors (duplicates, nulls, invalid formats)
- Metadata: \_ingested_at, \_source_file, \_partition_date

**STAGING (Silver):**

- Deduplicated and validated
- Standardized formats
- Referential integrity enforced
- Timestamps: created_at, updated_at

**PRODUCTION (Gold):**

- Aggregated metrics
- Business-ready data
- Optimized for reporting

## 📁 Project Structure

```
week-03-04-python-etl/
├── raw_data/ # Generated Parquet files (gitignored)
│ ├── customers/
│ ├── products/
│ ├── orders/
│ └── order_items/
├── scripts/ # ETL scripts
│ ├── generate_raw_data.py # Generate fake data with errors
│ ├── etl_raw.py # RAW layer ETL
│ ├── etl_stg.py # STAGING layer ETL
│ ├── etl_prod.py # PRODUCTION layer ETL
│ ├── etl_runner.py # Pipeline orchestrator
│ ├── validate_pipeline.py # Data quality validation
│ ├── db_connector.py # Database connection
│ ├── data_cleaner.py # Data cleaning utilities
│ └── validators.py # Data validation utilities
├── notebooks/ # Learning notebooks
│ ├── 00_setup_and_overview.ipynb
│ ├── 01_raw_layer_exploration.ipynb
│ ├── 02_staging_transformation.ipynb
│ ├── 03_production_aggregation.ipynb
│ ├── 04_full_pipeline_demo.ipynb
│ ├── 05_data_quality_checks.ipynb
│ ├── 06_troubleshooting_guide.ipynb
│ └── README.md
├── sql/ # SQL schemas
│ └── 04-create-etl-tables.sql
├── logs/ # Execution logs (gitignored)
└── README.md # This file
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose running
- PostgreSQL container started
- Python environment with required packages

### 1. First Time Setup

```bash
# Complete setup (creates schemas, generates test data, runs pipeline)
make etl-quick-start
```

This will:

- Create database schemas (raw, staging, prod)
- Generate 3 days of test data
- Run RAW layer ETL
- Run STAGING layer ETL
- Run PRODUCTION layer ETL
- Validate the pipeline

### 2. Open Notebooks

```bash
# Start Jupyter Lab
make notebook-start

# Access at: http://localhost:8888 (token: dataengineer)
```

### 3. Validate Pipeline

```bash
# Run comprehensive validation
make etl-validate

# Or check individual layers
make etl-check-raw
make etl-check-stg
make etl-check-prod
```

## 📚 Learning Path

Follow these notebooks in order:

### 1. 00_setup_and_overview.ipynb (Start Here!)

- Understand 3-layer architecture
- Check database connection
- Overview of data flow

### 2. 01_raw_layer_exploration.ipynb

- Read Parquet files
- Ingest into RAW schema
- Analyze data quality issues
- Understand metadata tracking

### 3. 02_staging_transformation.ipynb

- Transform RAW to STAGING
- Step-by-step data cleaning
- Remove duplicates
- Validate formats
- Handle nulls

### 4. 03_production_aggregation.ipynb

- Aggregate STAGING to PRODUCTION
- Create business metrics
- Daily and monthly sales
- Customer lifetime value
- Category performance

### 5. 04_full_pipeline_demo.ipynb

- Run complete pipeline end-to-end
- Monitor progress
- Validate results
- Analyze business metrics

### 6. 05_data_quality_checks.ipynb

- Schema validation
- Data completeness
- Data accuracy
- Referential integrity
- Business rules

### 7. 06_troubleshooting_guide.ipynb

- Common errors and solutions
- Debug techniques
- Recovery procedures
- Health checks

## 🔧 Common Commands

### 1. Data Generation

```bash
# Generate test data (3 days)
make etl-generate-raw-test

# Generate full data (365 days) - takes longer
make etl-generate-raw
```

### 2. Run ETL Layers

```bash
# Run individual layers
make etl-run-raw      # Parquet to RAW
make etl-run-stg      # RAW to STAGING
make etl-run-prod     # STAGING to PRODUCTION

# Run full pipeline
make etl-run-all      # All layers sequentially
```

### 3. Validation and Quality

```bash
# Comprehensive validation
make etl-validate

# Health check all layers
make etl-health-check

# Check individual layers
make etl-check-raw
make etl-check-stg
make etl-check-prod
```

### 4. Debug and Troubleshooting

```bash
# Debug individual layers
make etl-debug-raw
make etl-debug-stg
make etl-debug-prod

# View logs
make etl-logs

# Get help
make etl-help
```

### 5. Cleanup

```bash
# Clear Parquet files only
make etl-clear-raw-data

# Clear all ETL data (WARNING: deletes everything!)
make etl-clear-all
```

## 📊 Data Flow Details

### 1. RAW Layer (Bronze)

**Purpose**: Store raw data exactly as received

**Characteristics**:

- Immutable (append-only)
- Contains data quality issues (intentional)
- Metadata columns: \_ingested_at, \_source_file, \_partition_date
- No transformations applied

**Tables**:

- raw.customers
- raw.products
- raw.orders
- raw.order_items

### 2. STAGING Layer (Silver)

**Purpose**: Clean and standardize data

**Transformations**:

- Remove duplicates (by ID and email)
- Validate formats (email, phone, etc.)
- Standardize text (capitalize names)
- Handle nulls (drop required, fill optional)
- Enforce referential integrity
- Add timestamps (created_at, updated_at)

**Tables**:

- staging.customers
- staging.products
- staging.orders
- staging.order_items

### 3. PRODUCTION Layer (Gold)

**Purpose**: Business-ready metrics and aggregations

**Aggregations**:

- Daily sales metrics
- Monthly sales summary
- Customer lifetime value
- Category performance
- Product performance

**Tables**:

- prod.daily_sales
- prod.monthly_sales
- prod.customer_metrics
- prod.daily_category_metrics
- prod.daily_product_metrics

## 🐛 Troubleshooting

### 1. Pipeline Fails at RAW Layer

```bash
# Check if Parquet files exist
ls -la raw_data/customers/

# If empty, generate data
make etl-generate-raw-test

# Check database connection
make test-connection
```

### 2. Pipeline Fails at STAGING Layer

```bash
# Check RAW layer has data
make etl-check-raw

# Debug RAW layer
make etl-debug-raw

# Check for data quality issues
make etl-validate
```

### 3. Pipeline Fails at PRODUCTION Layer

```bash
# Check STAGING layer has data
make etl-check-stg

# Debug STAGING layer
make etl-debug-stg

# Verify referential integrity
make etl-validate
```

### 4.Data Quality Issues

```bash
# Run comprehensive validation
make etl-validate

# Check specific layer
make etl-check-stg

# View sample data
make shell-postgres
# Then: SELECT * FROM staging.customers LIMIT 10;
```

### ✅ Success Criteria

After completing this module, you should have:

- All schemas created (raw, staging, prod)
- Raw data generated successfully
- ETL pipeline runs without errors
- All data quality checks pass
- Business metrics available in prod layer
- All notebooks runnable
- Understanding of 3-layer architecture
- Ability to debug pipeline issues

### 📖 Key Concepts

**ETL vs ELT**

- ETL: Extract-Transform-Load (transform before loading)
- ELT: Extract-Load-Transform (transform after loading)
  This project uses ETL approach

**Medallion Architecture**

- Bronze (RAW): Raw data as-is
- Silver (STAGING): Cleaned and validated
- Gold (PRODUCTION): Business-ready aggregations

**Data Quality Dimensions**

- Completeness: No missing required data
- Accuracy: Data is correct and valid
- Consistency: No duplicates, standardized formats
- Validity: Follows business rules
- Integrity: Referential relationships maintained

**Best Practices**

- Immutable RAW Layer: Never modify raw data
- Idempotent Transformations: Same input produces same output
- Data Lineage: Track data through pipeline
- Quality Checks: Validate at each layer
- Error Handling: Graceful failures with logging

### 📖 Additional Resources

**Documentation**

- PostgreSQL: https://www.postgresql.org/docs/
- Pandas: https://pandas.pydata.org/docs/
- PyArrow: https://arrow.apache.org/docs/python/

**Learning Materials**

- Data Engineering Zoomcamp: https://github.com/DataTalksClub/data-engineering-zoomcamp
- Medallion Architecture: https://www.databricks.com/glossary/medallion-architecture
- Data Quality: https://www.talend.com/resources/what-is-data-quality/

### 🆘 Getting Help

- Check Notebooks: Start with 06_troubleshooting_guide.ipynb
- Run Validation: `make etl-validate`
- Check Logs: `make etl-logs`
- Debug Layers: `make etl-debug-raw, make etl-debug-stg, make etl-debug-prod`
- Health Check: `make etl-health-check`

### 🎓 Next Steps

**After mastering this module**:

- Week 05-06: Data Modeling and Dimensional Design
- Week 07-08: Workflow Orchestration with Apache Airflow
- Week 09-10: Data Quality and Testing Frameworks
- Week 11-12: Cloud Data Engineering (AWS/GCP)
