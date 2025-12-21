# 📚 Notebooks - ETL Pipeline Learning

Thư mục này chứa các Jupyter notebooks để học và thực hành ETL pipeline.

## 📖 Notebooks

### 00. Setup & Overview

**File:** `00_setup_and_overview.ipynb`

- Tổng quan kiến trúc 3-layer ETL
- Kiểm tra kết nối database
- Hiểu data flow

### 01. Raw Layer Exploration

**File:** `01_raw_layer_exploration.ipynb`

- Đọc Parquet files
- Ingest vào RAW schema
- Phân tích data quality issues
- Metadata tracking

### 02. Staging Transformation

**File:** `02_staging_transformation.ipynb`

- Transform từ RAW → STAGING
- Data cleaning step-by-step
- Validation và constraints
- Compare before/after

### 03. Production Aggregation

**File:** `03_production_aggregation.ipynb`

- Aggregate từ STAGING → PRODUCTION
- Tạo business metrics
- Visualize trends
- Optimize cho reporting

### 04. Full Pipeline Demo

**File:** `04_full_pipeline_demo.ipynb`

- Chạy toàn bộ pipeline end-to-end
- Monitor progress
- Validate kết quả
- Business metrics

### 05. Data Quality Checks

**File:** `05_data_quality_checks.ipynb`

- Schema validation
- Data completeness
- Data accuracy
- Referential integrity
- Business rules

### 06. Troubleshooting Guide

**File:** `06_troubleshooting_guide.ipynb`

- Common errors & solutions
- Diagnostic queries
- Recovery procedures
- Health checks

## 🚀 Cách sử dụng

### 1. Setup môi trường

```bash
# Activate virtual environment
source venv/bin/activate  # macOS/Linux
# hoặc
venv\Scripts\activate     # Windows

# Install Jupyter
pip install jupyter ipykernel

# Add kernel
python -m ipykernel install --user --name=etl-pipeline
```
