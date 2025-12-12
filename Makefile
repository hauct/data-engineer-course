# File: data-engineer-course/Makefile

.PHONY: help setup start stop restart clean logs test-connection generate-data reset-data shell-postgres shell-jupyter status backup restore

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

help: ## Show this help message
	@echo "$(BLUE)Data Engineering Course - Available Commands$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""

setup: ## Initial setup - create directories and .env file
	@echo "$(BLUE)Setting up environment...$(NC)"
	@mkdir -p week-01-02-sql-python/postgres/data
	@mkdir -p week-01-02-sql-python/exercises
	@mkdir -p week-01-02-sql-python/scripts
	@mkdir -p shared/datasets
	@touch week-01-02-sql-python/postgres/data/.gitkeep
	@if [ ! -f .env ]; then cp .env.example .env; echo "$(GREEN)✓ Created .env file$(NC)"; fi
	@echo "$(GREEN)✓ Setup completed!$(NC)"

start: ## Start all services
	@echo "$(BLUE)Starting services...$(NC)"
	@docker-compose up -d
	@echo "$(GREEN)✓ Services started!$(NC)"
	@echo ""
	@echo "Access points:"
	@echo "  - PostgreSQL: localhost:5432"
	@echo "  - PgAdmin: http://localhost:5050"
	@echo "  - Jupyter Lab: http://localhost:8888 (token: dataengineer)"

stop: ## Stop all services (data is preserved)
	@echo "$(BLUE)Stopping services...$(NC)"
	@docker-compose down
	@echo "$(GREEN)✓ Services stopped!$(NC)"
	@echo "$(YELLOW)Note: Database data is preserved in Docker volumes$(NC)"

restart: ## Restart all services
	@echo "$(BLUE)Restarting services...$(NC)"
	@docker-compose restart
	@echo "$(GREEN)✓ Services restarted!$(NC)"

clean: ## Stop services and remove volumes (WARNING: deletes all data!)
	@echo "$(RED)WARNING: This will delete all database data!$(NC)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		echo "$(BLUE)Cleaning up...$(NC)"; \
		docker-compose down -v; \
		docker system prune -f; \
		rm -rf week-01-02-sql-python/postgres/data/*; \
		touch week-01-02-sql-python/postgres/data/.gitkeep; \
		echo "$(GREEN)✓ Cleanup completed!$(NC)"; \
	else \
		echo "$(YELLOW)Cleanup cancelled$(NC)"; \
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
	@echo "$(BLUE)Service Status:$(NC)"
	@docker-compose ps
	@echo ""
	@echo "$(BLUE)Docker Volumes:$(NC)"
	@docker volume ls | grep de_

test-connection: ## Test database connection
	@docker-compose exec jupyter python /home/jovyan/scripts/test_connection.py

generate-data: ## Generate sample data (only if not exists)
	@echo "$(BLUE)Generating sample data...$(NC)"
	@docker-compose exec jupyter python /home/jovyan/scripts/generate_data.py

reset-data: ## Reset all data (WARNING: deletes all records!)
	@echo "$(RED)WARNING: This will delete all data records!$(NC)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose exec jupyter python /home/jovyan/scripts/reset_data.py; \
	else \
		echo "$(YELLOW)Reset cancelled$(NC)"; \
	fi

shell-postgres: ## Open PostgreSQL shell
	@docker-compose exec postgres psql -U dataengineer -d data_engineer

shell-jupyter: ## Open Jupyter container shell
	@docker-compose exec jupyter bash

backup: ## Backup database to file
	@echo "$(BLUE)Creating database backup...$(NC)"
	@mkdir -p backups
	@docker-compose exec -T postgres pg_dump -U dataengineer data_engineer > backups/backup_$$(date +%Y%m%d_%H%M%S).sql
	@echo "$(GREEN)✓ Backup created in backups/ directory$(NC)"

restore: ## Restore database from latest backup
	@echo "$(BLUE)Restoring database from backup...$(NC)"
	@LATEST=$$(ls -t backups/*.sql | head -1); \
	if [ -z "$$LATEST" ]; then \
		echo "$(RED)No backup files found!$(NC)"; \
		exit 1; \
	fi; \
	echo "Restoring from: $$LATEST"; \
	cat $$LATEST | docker-compose exec -T postgres psql -U dataengineer data_engineer; \
	echo "$(GREEN)✓ Database restored!$(NC)"

quick-start: ## Complete setup and start (for first time)
	@echo "$(BLUE)🚀 Quick Start - Complete Setup$(NC)"
	@echo ""
	@make setup
	@echo ""
	@make start
	@echo ""
	@echo "$(BLUE)⏳ Waiting 30 seconds for services to initialize...$(NC)"
	@sleep 30
	@echo ""
	@make test-connection
	@echo ""
	@echo "$(YELLOW)To generate sample data, run: make generate-data$(NC)"
	@echo ""
	@echo "$(GREEN)✅ SETUP COMPLETE!$(NC)"

rebuild: ## Rebuild Jupyter image
	@echo "$(BLUE)Rebuilding Jupyter image...$(NC)"
	@docker-compose build --no-cache jupyter
	@docker-compose up -d jupyter
	@echo "$(GREEN)✓ Jupyter rebuilt!$(NC)"