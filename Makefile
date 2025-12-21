# File: data-engineer-course/Makefile

# Force bash shell with ANSI support
SHELL := /bin/bash

.PHONY: help setup start stop restart clean logs test-connection generate-data reset-data shell-postgres shell-jupyter status backup restore etl-setup-schema etl-generate-raw etl-generate-raw-test etl-run-raw etl-run-stg etl-run-prod etl-run-all etl-clear-raw-data etl-clear-all notebook-start notebook-test notebook-clear-output etl-validate etl-check-raw etl-check-stg etl-check-prod etl-health-check etl-quick-start etl-full-demo etl-debug-raw etl-debug-stg etl-debug-prod etl-logs docs etl-help

# Colors for output - using printf for Windows compatibility
define print_blue
	@printf '\033[0;34m%s\033[0m\n' $(1)
endef

define print_green
	@printf '\033[0;32m%s\033[0m\n' $(1)
endef

define print_yellow
	@printf '\033[0;33m%s\033[0m\n' $(1)
endef

define print_red
	@printf '\033[0;31m%s\033[0m\n' $(1)
endef

# =============================================================================
# General Commands
# =============================================================================

help: ## Show this help message
	@printf '\033[0;34m%s\033[0m\n' "Data Engineering Course - Available Commands"
	@echo ""
	@printf '\033[0;33m%s\033[0m\n' "=== General ==="
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -v "===" | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[0;32m%-25s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@printf '\033[0;33m%s\033[0m\n' "For detailed ETL help: make etl-help"

setup: ## Initial setup - create directories and .env file
	@printf '\033[0;34m%s\033[0m\n' "Setting up environment..."
	@mkdir -p week-01-02-sql-python/postgres/data
	@mkdir -p week-01-02-sql-python/exercises
	@mkdir -p week-01-02-sql-python/scripts
	@mkdir -p week-03-04-python-etl/raw_data
	@mkdir -p week-03-04-python-etl/logs
	@mkdir -p shared/datasets
	@mkdir -p backups
	@touch week-01-02-sql-python/postgres/data/.gitkeep
	@if [ ! -f .env ]; then cp .env.example .env; printf '\033[0;32m%s\033[0m\n' "[OK] Created .env file"; fi
	@printf '\033[0;32m%s\033[0m\n' "[OK] Setup completed!"

start: ## Start all services
	@printf '\033[0;34m%s\033[0m\n' "Starting services..."
	@docker-compose up -d
	@printf '\033[0;32m%s\033[0m\n' "[OK] Services started!"
	@echo ""
	@echo "Access points:"
	@echo "  - PostgreSQL: localhost:5432"
	@echo "  - PgAdmin: http://localhost:5050"
	@echo "  - Jupyter Lab: http://localhost:8888 (token: dataengineer)"

stop: ## Stop all services (data is preserved)
	@printf '\033[0;34m%s\033[0m\n' "Stopping services..."
	@docker-compose down
	@printf '\033[0;32m%s\033[0m\n' "[OK] Services stopped!"
	@printf '\033[0;33m%s\033[0m\n' "Note: Database data is preserved in Docker volumes"

restart: ## Restart all services
	@printf '\033[0;34m%s\033[0m\n' "Restarting services..."
	@docker-compose restart
	@printf '\033[0;32m%s\033[0m\n' "[OK] Services restarted!"

clean: ## Stop services and remove volumes (WARNING: deletes all data!)
	@printf '\033[0;31m%s\033[0m\n' "WARNING: This will delete all database data!"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		printf '\033[0;34m%s\033[0m\n' "Cleaning up..."; \
		docker-compose down -v; \
		docker system prune -f; \
		rm -rf week-01-02-sql-python/postgres/data/*; \
		touch week-01-02-sql-python/postgres/data/.gitkeep; \
		printf '\033[0;32m%s\033[0m\n' "[OK] Cleanup completed!"; \
	else \
		printf '\033[0;33m%s\033[0m\n' "Cleanup cancelled"; \
	fi

logs: ## Show logs from all services
	@docker-compose logs -f

logs-postgres: ## Show PostgreSQL logs
	@docker-compose logs -f postgres

logs-jupyter: ## Show Jupyter logs
	@docker-compose logs -f jupyter

logs-pgadmin: ## Show PgAdmin logs
	@docker-compose logs -f pgadmin

status: ## Show status of all services
	@printf '\033[0;34m%s\033[0m\n' "Service Status:"
	@docker-compose ps
	@echo ""
	@printf '\033[0;34m%s\033[0m\n' "Docker Volumes:"
	@docker volume ls | grep de_

shell-postgres: ## Open PostgreSQL shell
	@docker-compose exec postgres psql -U dataengineer -d data_engineer

shell-jupyter: ## Open Jupyter container shell
	@docker-compose exec jupyter bash

backup: ## Backup database to file
	@printf '\033[0;34m%s\033[0m\n' "Creating database backup..."
	@mkdir -p backups
	@docker-compose exec -T postgres pg_dump -U dataengineer data_engineer > backups/backup_$$(date +%Y%m%d_%H%M%S).sql
	@printf '\033[0;32m%s\033[0m\n' "[OK] Backup created in backups/ directory"

restore: ## Restore database from latest backup
	@printf '\033[0;34m%s\033[0m\n' "Restoring database from backup..."
	@LATEST=$$(ls -t backups/*.sql | head -1); \
	if [ -z "$$LATEST" ]; then \
		printf '\033[0;31m%s\033[0m\n' "No backup files found!"; \
		exit 1; \
	fi; \
	echo "Restoring from: $$LATEST"; \
	cat $$LATEST | docker-compose exec -T postgres psql -U dataengineer data_engineer; \
	printf '\033[0;32m%s\033[0m\n' "[OK] Database restored!"

rebuild: ## Rebuild Jupyter image
	@printf '\033[0;34m%s\033[0m\n' "Rebuilding Jupyter image..."
	@docker-compose build --no-cache jupyter
	@docker-compose up -d jupyter
	@printf '\033[0;32m%s\033[0m\n' "[OK] Jupyter rebuilt!"

quick-start: ## Complete setup and start (for first time)
	@printf '\033[0;34m%s\033[0m\n' "==> Quick Start - Complete Setup"
	@echo ""
	@$(MAKE) setup
	@echo ""
	@$(MAKE) start
	@echo ""
	@printf '\033[0;34m%s\033[0m\n' "[...] Waiting 30 seconds for services to initialize..."
	@sleep 30
	@echo ""
	@$(MAKE) test-connection
	@echo ""
	@printf '\033[0;33m%s\033[0m\n' "To generate sample data, run: make generate-data"
	@echo ""
	@printf '\033[0;32m%s\033[0m\n' "[OK] SETUP COMPLETE!"

# =============================================================================
# Week 01-02: SQL & Python Commands
# =============================================================================

test-connection: ## Test database connection
	@docker-compose exec jupyter python /home/jovyan/week-01-02-sql-python/scripts/test_connection.py

generate-data: ## Generate sample data (only if not exists)
	@printf '\033[0;34m%s\033[0m\n' "Generating sample data..."
	@docker-compose exec jupyter python /home/jovyan/week-01-02-sql-python/scripts/generate_data.py

reset-data: ## Reset all data (WARNING: deletes all records!)
	@printf '\033[0;31m%s\033[0m\n' "WARNING: This will delete all data records!"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose exec jupyter python /home/jovyan/week-01-02-sql-python/scripts/reset_data.py; \
	else \
		printf '\033[0;33m%s\033[0m\n' "Reset cancelled"; \
	fi

# =============================================================================
# Week 03-04: ETL Pipeline Commands
# =============================================================================

etl-setup-schema: ## Create ETL schemas and tables (raw, stg, prod)
	@printf '\033[0;34m%s\033[0m\n' "Creating ETL schemas and tables..."
	@MSYS_NO_PATHCONV=1 docker-compose exec jupyter cat /home/jovyan/week-03-04-python-etl/sql/04-create-etl-tables.sql | docker-compose exec -T postgres psql -U dataengineer -d data_engineer
	@printf '\033[0;32m%s\033[0m\n' "[OK] ETL schemas created!"

etl-generate-raw: ## Generate raw parquet data with errors (365 days)
	@printf '\033[0;34m%s\033[0m\n' "Generating raw parquet data (365 days)..."
	@docker-compose exec jupyter python /home/jovyan/week-03-04-python-etl/scripts/generate_raw_data.py
	@printf '\033[0;32m%s\033[0m\n' "[OK] Raw data generated!"

etl-generate-raw-test: ## Generate raw data (test mode - 3 days only)
	@printf '\033[0;34m%s\033[0m\n' "Generating raw data (test mode - 3 days)..."
	@docker-compose exec jupyter python /home/jovyan/week-03-04-python-etl/scripts/generate_raw_data.py --test-mode
	@printf '\033[0;32m%s\033[0m\n' "[OK] Test data generated!"

etl-run-raw: ## Run ETL: raw_data folder -> raw schema
	@printf '\033[0;34m%s\033[0m\n' "Running ETL Raw Layer..."
	@docker-compose exec jupyter python /home/jovyan/week-03-04-python-etl/scripts/etl_raw.py
	@printf '\033[0;32m%s\033[0m\n' "[OK] Raw layer complete!"

etl-run-stg: ## Run ETL: raw schema -> stg schema (clean, dedupe)
	@printf '\033[0;34m%s\033[0m\n' "Running ETL Staging Layer..."
	@docker-compose exec jupyter python /home/jovyan/week-03-04-python-etl/scripts/etl_stg.py
	@printf '\033[0;32m%s\033[0m\n' "[OK] Staging layer complete!"

etl-run-prod: ## Run ETL: stg schema -> prod schema (aggregate)
	@printf '\033[0;34m%s\033[0m\n' "Running ETL Production Layer..."
	@docker-compose exec jupyter python /home/jovyan/week-03-04-python-etl/scripts/etl_prod.py
	@printf '\033[0;32m%s\033[0m\n' "[OK] Production layer complete!"

etl-run-all: ## Run full ETL pipeline (raw -> stg -> prod)
	@printf '\033[0;34m%s\033[0m\n' "Running full ETL pipeline..."
	@docker-compose exec jupyter python /home/jovyan/week-03-04-python-etl/scripts/etl_runner.py --full
	@printf '\033[0;32m%s\033[0m\n' "[OK] Full ETL pipeline complete!"

etl-clear-raw-data: ## Clear raw_data folder (parquet files)
	@printf '\033[0;34m%s\033[0m\n' "Clearing raw_data folder..."
	@rm -rf week-03-04-python-etl/raw_data/*
	@printf '\033[0;32m%s\033[0m\n' "[OK] Raw data cleared!"

etl-clear-all: ## Clear all ETL data (raw, stg, prod schemas)
	@printf '\033[0;31m%s\033[0m\n' "WARNING: This will delete all ETL data!"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose exec -T postgres psql -U dataengineer -d data_engineer -c "TRUNCATE TABLE raw.order_items, raw.orders, raw.products, raw.customers CASCADE;"; \
		docker-compose exec -T postgres psql -U dataengineer -d data_engineer -c "TRUNCATE TABLE staging.order_items, staging.orders, staging.products, staging.customers CASCADE;"; \
		docker-compose exec -T postgres psql -U dataengineer -d data_engineer -c "TRUNCATE TABLE prod.daily_sales, prod.monthly_sales, prod.daily_category_metrics, prod.daily_product_metrics, prod.customer_metrics CASCADE;"; \
		printf '\033[0;32m%s\033[0m\n' "[OK] All ETL data cleared!"; \
	else \
		printf '\033[0;33m%s\033[0m\n' "Clear cancelled"; \
	fi

# =============================================================================
# Notebook Commands
# =============================================================================

notebook-start: ## Start Jupyter and open notebooks
	@printf '\033[0;34m%s\033[0m\n' "Starting Jupyter Lab..."
	@docker-compose up -d jupyter
	@sleep 5
	@printf '\033[0;32m%s\033[0m\n' "[OK] Jupyter Lab started!"
	@echo ""
	@echo "Access: http://localhost:8888 (token: dataengineer)"
	@echo "Notebooks: week-03-04-python-etl/notebooks/"

notebook-clear-output: ## Clear all notebook outputs
	@printf '\033[0;34m%s\033[0m\n' "Clearing notebook outputs..."
	@docker-compose exec jupyter bash -c "cd /home/jovyan/week-03-04-python-etl/notebooks && jupyter nbconvert --clear-output --inplace *.ipynb"
	@printf '\033[0;32m%s\033[0m\n' "[OK] Outputs cleared!"

# =============================================================================
# Validation & Quality Commands
# =============================================================================

etl-validate: ## Validate ETL pipeline data quality
	@printf '\033[0;34m%s\033[0m\n' "Validating ETL pipeline..."
	@docker-compose exec jupyter python /home/jovyan/week-03-04-python-etl/scripts/validate_pipeline.py
	@printf '\033[0;32m%s\033[0m\n' "[OK] Validation complete!"

etl-check-raw: ## Check raw layer data quality
	@printf '\033[0;34m%s\033[0m\n' "Checking raw layer..."
	@docker-compose exec -T postgres psql -U dataengineer -d data_engineer -c "\echo '=== RAW LAYER SUMMARY ==='; SELECT 'customers' as table, COUNT(*) as rows FROM raw.customers UNION ALL SELECT 'products', COUNT(*) FROM raw.products UNION ALL SELECT 'orders', COUNT(*) FROM raw.orders UNION ALL SELECT 'order_items', COUNT(*) FROM raw.order_items;"

etl-check-stg: ## Check staging layer data quality
	@printf '\033[0;34m%s\033[0m\n' "Checking staging layer..."
	@docker-compose exec -T postgres psql -U dataengineer -d data_engineer -c "\echo '=== STAGING LAYER SUMMARY ==='; SELECT 'customers' as table, COUNT(*) as rows FROM staging.customers UNION ALL SELECT 'products', COUNT(*) FROM staging.products UNION ALL SELECT 'orders', COUNT(*) FROM staging.orders UNION ALL SELECT 'order_items', COUNT(*) FROM staging.order_items; \echo ''; \echo '=== DATA QUALITY CHECKS ==='; SELECT 'Duplicate emails' as check, COUNT(*) - COUNT(DISTINCT email) as issues FROM staging.customers UNION ALL SELECT 'NULL emails', COUNT(*) FROM staging.customers WHERE email IS NULL UNION ALL SELECT 'Invalid emails', COUNT(*) FROM staging.customers WHERE email NOT LIKE '%@%.%';"

etl-check-prod: ## Check production layer metrics
	@printf '\033[0;34m%s\033[0m\n' "Checking production layer..."
	@docker-compose exec -T postgres psql -U dataengineer -d data_engineer -c "\echo '=== PRODUCTION LAYER SUMMARY ==='; SELECT 'daily_sales' as table, COUNT(*) as rows FROM prod.daily_sales UNION ALL SELECT 'monthly_sales', COUNT(*) FROM prod.monthly_sales UNION ALL SELECT 'customer_metrics', COUNT(*) FROM prod.customer_metrics UNION ALL SELECT 'daily_category_metrics', COUNT(*) FROM prod.daily_category_metrics UNION ALL SELECT 'daily_product_metrics', COUNT(*) FROM prod.daily_product_metrics;"

etl-health-check: ## Complete health check of ETL pipeline
	@printf '\033[0;34m%s\033[0m\n' "Running health check..."
	@$(MAKE) etl-check-raw
	@echo ""
	@$(MAKE) etl-check-stg
	@echo ""
	@$(MAKE) etl-check-prod
	@printf '\033[0;32m%s\033[0m\n' "[OK] Health check complete!"

# =============================================================================
# Quick Start Commands
# =============================================================================

etl-quick-start: ## Complete ETL setup from scratch (test mode)
	@printf '\033[0;34m%s\033[0m\n' "==> ETL Quick Start (Test Mode)"
	@echo ""
	@printf '\033[0;33m%s\033[0m\n' "Step 1/5: Setup schemas..."
	@$(MAKE) etl-setup-schema
	@echo ""
	@printf '\033[0;33m%s\033[0m\n' "Step 2/5: Generate test data..."
	@$(MAKE) etl-generate-raw-test
	@echo ""
	@printf '\033[0;33m%s\033[0m\n' "Step 3/5: Run raw layer..."
	@$(MAKE) etl-run-raw
	@echo ""
	@printf '\033[0;33m%s\033[0m\n' "Step 4/5: Run staging layer..."
	@$(MAKE) etl-run-stg
	@echo ""
	@printf '\033[0;33m%s\033[0m\n' "Step 5/5: Run production layer..."
	@$(MAKE) etl-run-prod
	@echo ""
	@$(MAKE) etl-health-check
	@echo ""
	@printf '\033[0;32m%s\033[0m\n' "[OK] ETL QUICK START COMPLETE!"
	@echo ""
	@printf '\033[0;33m%s\033[0m\n' "Next steps:"
	@echo "  - Open notebooks: make notebook-start"
	@echo "  - View data: make shell-postgres"
	@echo "  - Check quality: make etl-validate"

etl-full-demo: ## Run full ETL with 365 days of data
	@printf '\033[0;34m%s\033[0m\n' "==> ETL Full Demo (365 days)"
	@echo ""
	@printf '\033[0;31m%s\033[0m\n' "WARNING: This will take several minutes!"
	@read -p "Continue? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		$(MAKE) etl-setup-schema; \
		$(MAKE) etl-generate-raw; \
		$(MAKE) etl-run-all; \
		$(MAKE) etl-health-check; \
		printf '\033[0;32m%s\033[0m\n' "[OK] Full demo complete!"; \
	else \
		printf '\033[0;33m%s\033[0m\n' "Demo cancelled"; \
	fi

# =============================================================================
# Debug Commands
# =============================================================================

etl-debug-raw: ## Debug raw layer (show sample data)
	@docker-compose exec -T postgres psql -U dataengineer -d data_engineer -c "\echo '=== RAW CUSTOMERS (Sample) ==='; SELECT * FROM raw.customers LIMIT 5; \echo ''; \echo '=== RAW METADATA ==='; SELECT _partition_date, COUNT(*) as rows, MIN(_ingested_at), MAX(_ingested_at) FROM raw.customers GROUP BY _partition_date ORDER BY _partition_date LIMIT 5;"

etl-debug-stg: ## Debug staging layer (show issues)
	@docker-compose exec -T postgres psql -U dataengineer -d data_engineer -c "\echo '=== STAGING CUSTOMERS (Sample) ==='; SELECT customer_id, customer_name, email, country FROM staging.customers LIMIT 5; \echo ''; \echo '=== POTENTIAL ISSUES ==='; SELECT 'Duplicate emails' as issue, email, COUNT(*) as count FROM staging.customers GROUP BY email HAVING COUNT(*) > 1 LIMIT 5;"

etl-debug-prod: ## Debug production layer (show metrics)
	@docker-compose exec -T postgres psql -U dataengineer -d data_engineer -c "\echo '=== DAILY SALES (Latest 5 days) ==='; SELECT * FROM prod.daily_sales ORDER BY order_date DESC LIMIT 5; \echo ''; \echo '=== TOP CUSTOMERS ==='; SELECT customer_name, total_orders, total_revenue FROM prod.customer_metrics ORDER BY total_revenue DESC LIMIT 5;"

etl-logs: ## Show ETL execution logs
	@printf '\033[0;34m%s\033[0m\n' "ETL Logs:"
	@if [ -d "week-03-04-python-etl/logs" ]; then \
		ls -lt week-03-04-python-etl/logs/ | head -10; \
	else \
		printf '\033[0;33m%s\033[0m\n' "No logs directory found"; \
	fi

# =============================================================================
# Documentation Commands
# =============================================================================

docs: ## Show project documentation
	@printf '\033[0;34m%s\033[0m\n' "=== PROJECT STRUCTURE ==="
	@tree -L 2 -I '__pycache__|*.pyc|.git|.ipynb_checkpoints' . 2>/dev/null || find . -maxdepth 2 -type d | grep -v -E '__pycache__|\.git|\.ipynb_checkpoints'

etl-help: ## Show detailed ETL help
	@printf '\033[0;34m%s\033[0m\n' "=== ETL PIPELINE HELP ==="
	@echo ""
	@echo "📊 ETL Architecture:"
	@echo "  raw_data/ (Parquet) → raw schema → staging schema → prod schema"
	@echo ""
	@echo "🚀 Quick Start:"
	@echo "  make etl-quick-start     # Test mode (3 days)"
	@echo "  make etl-full-demo       # Full mode (365 days)"
	@echo ""
	@echo "🔧 Step by Step:"
	@echo "  1. make etl-setup-schema       # Create schemas"
	@echo "  2. make etl-generate-raw-test  # Generate data"
	@echo "  3. make etl-run-raw            # Load to raw"
	@echo "  4. make etl-run-stg            # Clean to staging"
	@echo "  5. make etl-run-prod           # Aggregate to prod"
	@echo ""
	@echo "✅ Validation:"
	@echo "  make etl-health-check    # Check all layers"
	@echo "  make etl-validate        # Run quality checks"
	@echo ""
	@echo "📚 Learning:"
	@echo "  make notebook-start      # Open Jupyter notebooks"
	@echo ""
	@echo "🔍 Debug:"
	@echo "  make etl-debug-raw       # Debug raw layer"
	@echo "  make etl-debug-stg       # Debug staging layer"
	@echo "  make etl-debug-prod      # Debug prod layer"
	@echo ""
	@echo "🗑️ Cleanup:"
	@echo "  make etl-clear-raw-data  # Clear parquet files"
	@echo "  make etl-clear-all       # Clear all ETL data"

# ==============================================================================
# WEEK 05-06: PYSPARK BIG DATA
# ==============================================================================

PYSPARK_DIR := week-05-06-pyspark-big-data

.PHONY: pyspark-build
pyspark-build: ## Build PySpark images only
	@echo "🔨 Building Spark images..."
	@echo ""
	@echo "1/2: Building Spark base image..."
	cd week-05-06-pyspark-big-data/spark && docker build --no-cache -t de-spark:3.5.1 .
	@echo ""
	@echo "2/2: Building Jupyter-Spark image..."
	cd week-05-06-pyspark-big-data/jupyter && docker build --no-cache -t de-jupyter-pyspark:latest .
	@echo ""
	@echo "✅ Build completed!"

.PHONY: pyspark-rebuild
pyspark-rebuild: ## Force rebuild all PySpark images
	@echo "🔨 Force rebuilding all images..."
	@docker rmi -f de-spark:3.5.1 de-jupyter-pyspark:latest 2>/dev/null || true
	@$(MAKE) pyspark-build

.PHONY: pyspark-start
pyspark-start: ## Start PySpark cluster (build if needed)
	@echo "🚀 Starting PySpark Big Data environment..."
	@echo ""
	
	@echo "📦 Step 1/7: Checking images..."
	@if ! docker images | grep -q "de-spark.*3.5.1"; then \
		echo "   Building Spark image..."; \
		cd week-05-06-pyspark-big-data/spark && docker build -t de-spark:3.5.1 .; \
	else \
		echo "   ✅ Spark image exists"; \
	fi
	@if ! docker images | grep -q "de-jupyter-pyspark.*latest"; then \
		echo "   Building Jupyter-Spark image..."; \
		cd week-05-06-pyspark-big-data/jupyter && docker build -t de-jupyter-pyspark:latest .; \
	else \
		echo "   ✅ Jupyter-Spark image exists"; \
	fi
	@echo ""
	
	@echo "🗄️  Step 2/7: Starting PostgreSQL and MinIO..."
	@docker-compose up -d postgres minio
	@echo "   ⏳ Waiting 15 seconds..."
	@sleep 15
	@echo ""
	
	@echo "🪣 Step 3/7: Creating MinIO buckets..."
	@docker-compose up -d minio-client
	@echo "   ⏳ Waiting 10 seconds..."
	@sleep 10
	@echo ""
	
	@echo "⚡ Step 4/7: Starting Spark Master..."
	@docker-compose up -d spark-master
	@echo "   ⏳ Waiting for Spark Master to be healthy (max 60s)..."
	@for i in $$(seq 1 30); do \
		if docker inspect de-spark-master 2>/dev/null | grep -q '"Status": "healthy"'; then \
			echo "   ✅ Spark Master is healthy!"; \
			break; \
		fi; \
		echo "   ⏳ Attempt $$i/30..."; \
		sleep 2; \
	done
	@echo ""
	
	@echo "👷 Step 5/7: Starting Spark Workers..."
	@docker-compose up -d spark-worker-1 spark-worker-2
	@echo "   ⏳ Waiting 15 seconds..."
	@sleep 15
	@echo ""
	
	@echo "📜 Step 6/7: Starting Spark History Server..."
	@docker-compose up -d spark-history
	@echo "   ⏳ Waiting 5 seconds..."
	@sleep 5
	@echo ""
	
	@echo "📓 Step 7/7: Starting Jupyter-Spark..."
	@docker-compose up -d jupyter-spark
	@echo "   ⏳ Waiting 10 seconds..."
	@sleep 10
	@echo ""
	
	@echo "✅ =============================================="
	@echo "✅  PYSPARK ENVIRONMENT STARTED SUCCESSFULLY!"
	@echo "✅ =============================================="
	@echo ""
	@docker-compose ps | grep -E "de-spark|de-jupyter-spark|de-minio|de-postgres"
	@echo ""
	@echo "📊 Access points:"
	@echo "   - Jupyter Lab:      http://localhost:8889 (token: dataengineer)"
	@echo "   - Spark Master UI:  http://localhost:8080"
	@echo "   - Spark Job UI:     http://localhost:4041"
	@echo "   - Spark Worker 1:   http://localhost:8081"
	@echo "   - Spark Worker 2:   http://localhost:8082"
	@echo "   - Spark History:    http://localhost:18080"
	@echo "   - MinIO Console:    http://localhost:9001"
	@echo ""
	@echo "🧪 Test connection: make pyspark-test"

.PHONY: pyspark-check
pyspark-check: ## Quick check if PySpark is running
	@echo "🔍 Checking PySpark services..."
	@echo ""
	@echo "📦 Docker Images:"
	@docker images | grep -E "de-spark|de-jupyter-pyspark" || echo "   ❌ No images found"
	@echo ""
	@echo "🐳 Running Containers:"
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "de-spark|de-jupyter-spark" || echo "   ❌ No containers running"
	@echo ""
	@echo "🌐 Service Health:"
	@printf "   Spark Master:    "
	@curl -s http://localhost:8080 > /dev/null && echo "✅ OK" || echo "❌ DOWN"
	@printf "   Spark Worker 1:  "
	@curl -s http://localhost:8081 > /dev/null && echo "✅ OK" || echo "❌ DOWN"
	@printf "   Spark Worker 2:  "
	@curl -s http://localhost:8082 > /dev/null && echo "✅ OK" || echo "❌ DOWN"
	@printf "   Jupyter-Spark:   "
	@curl -s http://localhost:8889 > /dev/null && echo "✅ OK" || echo "❌ DOWN"
	@printf "   MinIO:           "
	@curl -s http://localhost:9001 > /dev/null && echo "✅ OK" || echo "❌ DOWN"

# Start with streaming (Kafka)
.PHONY: pyspark-start-streaming
pyspark-start-streaming:
	@echo "🚀 Starting PySpark with Kafka..."
	docker-compose --profile streaming up -d
	@echo "✅ All services started (including Kafka)!"
	@echo "  - Kafka UI: http://localhost:8090"

# Stop PySpark services
.PHONY: pyspark-stop
pyspark-stop:
	@echo "🛑 Stopping PySpark services..."
	docker-compose stop spark-master spark-worker-1 spark-worker-2 spark-history jupyter-spark minio

# Restart PySpark services
.PHONY: pyspark-restart
pyspark-restart: pyspark-stop pyspark-start

# View logs
.PHONY: pyspark-logs
pyspark-logs:
	docker-compose logs -f spark-master spark-worker-1 spark-worker-2 jupyter-spark

# Test Spark connection
.PHONY: pyspark-test
pyspark-test:
	@echo "🧪 Testing Spark connection..."
	docker exec -it de-jupyter-spark python /home/jovyan/apps/test_spark_connection.py

# Shell into Jupyter-Spark
.PHONY: pyspark-shell
pyspark-shell:
	docker exec -it de-jupyter-spark bash

# PySpark shell (interactive)
.PHONY: pyspark-repl
pyspark-repl:
	docker exec -it de-jupyter-spark pyspark --master spark://spark-master:7077

# Check Spark cluster status
.PHONY: pyspark-status
pyspark-status:
	@echo "📊 Spark Cluster Status:"
	@echo ""
	@echo "Master:"
	@curl -s http://localhost:8080 > /dev/null && echo "  ✅ Running" || echo "  ❌ Not running"
	@echo ""
	@echo "Workers:"
	@curl -s http://localhost:8081 > /dev/null && echo "  ✅ Worker 1 Running" || echo "  ❌ Worker 1 Not running"
	@curl -s http://localhost:8082 > /dev/null && echo "  ✅ Worker 2 Running" || echo "  ❌ Worker 2 Not running"
	@echo ""
	@echo "Jupyter-Spark:"
	@curl -s http://localhost:8889 > /dev/null && echo "  ✅ Running" || echo "  ❌ Not running"

# Generate sample data
.PHONY: pyspark-generate-data
pyspark-generate-data:
	@echo "📊 Generating sample data..."
	docker exec -it de-jupyter-spark python /home/jovyan/apps/generate_sample_data.py

# Clean PySpark data
.PHONY: pyspark-clean-data
pyspark-clean-data:
	@echo "🗑️  Cleaning PySpark data..."
	docker exec -it de-jupyter-spark rm -rf /home/jovyan/data/raw/* /home/jovyan/data/staging/* /home/jovyan/data/production/*
	@echo "✅ Data cleaned!"

# PySpark help
.PHONY: pyspark-help
pyspark-help:
	@echo "📚 PySpark Commands:"
	@echo ""
	@echo "  make pyspark-start              Start PySpark services"
	@echo "  make pyspark-start-streaming    Start with Kafka"
	@echo "  make pyspark-stop               Stop PySpark services"
	@echo "  make pyspark-restart            Restart PySpark services"
	@echo "  make pyspark-logs               View logs"
	@echo "  make pyspark-test               Test Spark connection"
	@echo "  make pyspark-shell              Shell into Jupyter-Spark"
	@echo "  make pyspark-repl               PySpark interactive shell"
	@echo "  make pyspark-status             Check cluster status"
	@echo "  make pyspark-generate-data      Generate sample data"
	@echo "  make pyspark-clean-data         Clean data directories"
	@echo "  make pyspark-help               Show this help"