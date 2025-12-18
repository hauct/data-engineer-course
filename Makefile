# File: data-engineer-course/Makefile

# Force bash shell with ANSI support
SHELL := /bin/bash

.PHONY: help setup start stop restart clean logs test-connection generate-data reset-data shell-postgres shell-jupyter status backup restore etl-setup-schema etl-generate-raw etl-generate-raw-test etl-run-raw etl-run-stg etl-run-prod etl-run-all etl-clear-raw-data etl-clear-all

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

help: ## Show this help message
	@printf '\033[0;34m%s\033[0m\n' "Data Engineering Course - Available Commands"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[0;32m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""

setup: ## Initial setup - create directories and .env file
	@printf '\033[0;34m%s\033[0m\n' "Setting up environment..."
	@mkdir -p week-01-02-sql-python/postgres/data
	@mkdir -p week-01-02-sql-python/exercises
	@mkdir -p week-01-02-sql-python/scripts
	@mkdir -p shared/datasets
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

rebuild: ## Rebuild Jupyter image
	@printf '\033[0;34m%s\033[0m\n' "Rebuilding Jupyter image..."
	@docker-compose build --no-cache jupyter
	@docker-compose up -d jupyter
	@printf '\033[0;32m%s\033[0m\n' "[OK] Jupyter rebuilt!"

# =============================================================================
# ETL Pipeline Commands (Week 03-04)
# =============================================================================

etl-setup-schema: ## Create ETL schemas and tables (raw, stg, prod)
	@printf '\033[0;34m%s\033[0m\n' "Creating ETL schemas and tables..."
	@MSYS_NO_PATHCONV=1 docker-compose exec jupyter cat /home/jovyan/week-03-04-python-etl/sql/04-create-etl-tables.sql | docker-compose exec -T postgres psql -U dataengineer -d data_engineer
	@printf '\033[0;32m%s\033[0m\n' "[OK] ETL schemas created!"

etl-generate-raw: ## Generate raw parquet data with errors (365 days)
	@printf '\033[0;34m%s\033[0m\n' "Generating raw parquet data..."
	@docker-compose exec jupyter python /home/jovyan/week-03-04-python-etl/scripts/generate_raw_data.py
	@printf '\033[0;32m%s\033[0m\n' "[OK] Raw data generated!"

etl-generate-raw-test: ## Generate raw data (test mode - 3 days only)
	@printf '\033[0;34m%s\033[0m\n' "Generating raw data (test mode)..."
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