# Week 05-06 PySpark Setup Guide

## 🚀 Quick Start

### 1. Build and Start Services

```bash
# From project root
make pyspark-start
```

This will start:

- Spark Master
- 2 Spark Workers
- Spark History Server
- Jupyter Lab with PySpark
- MinIO (S3-compatible storage)

### 2. Verify Setup

```bash
# Check cluster status
make pyspark-status

# Test Spark connection
make pyspark-test
```

### 3. Access Services

- Jupyter Lab: `http://localhost:8889` (token: dataengineer)
- Spark Master UI: `http://localhost:8080`
- Spark Application UI: `http://localhost:4040` (when running jobs)
- MinIO Console: `http://localhost:9001` (minioadmin/minioadmin123)

## 📊 Verify Installation

Open Jupyter Lab and create a new notebook:

```python
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("Test") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Test
spark.range(100).count()
# Expected output: 100

# Check Spark UI
print(f"Spark UI: http://localhost:4040")
```

## 🐛 Troubleshooting

### 1. Workers not connecting to Master

```bash
# Check network
docker network inspect de_network

# Restart workers
docker-compose restart spark-worker-1 spark-worker-2
```

### 2. Jupyter can't connect to Spark

```bash
# Check logs
docker logs de-jupyter-spark

# Test connection
docker exec -it de-jupyter-spark curl http://spark-master:8080
```
