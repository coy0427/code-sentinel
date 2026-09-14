# ==============================================================================
# Secure Edge IoT & Telemetry Ingestion Engine - Makefile
# ==============================================================================

SHELL := /bin/bash
.DEFAULT_GOAL := help

.PHONY: help certs build up down restart logs test lint clean run launch

help: ## Show this help message
	@echo "Secure Edge IoT System - Available Commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

run: ## Launch the complete system (DB, Gateway, PKI, and Edge Agent) via single command
	@chmod +x launch.sh
	@./launch.sh || true

launch: run ## Alias for 'run'

certs: ## Generate PKI Root CA, Server, and Client mTLS Certificates
	@echo "==> Generating mTLS certificates..."
	@chmod +x certs/generate_certs.sh
	@./certs/generate_certs.sh

build: ## Build Gateway container images via Docker Compose
	@echo "==> Building Docker containers..."
	@docker compose build

up: certs ## Start Gateway and TimescaleDB services in the background
	@echo "==> Starting containers..."
	@docker compose up -d
	@echo "==> Gateway running at https://localhost:8443"

down: ## Stop all services and tear down containers and networks
	@echo "==> Stopping containers..."
	@docker compose down -v

restart: down up ## Restart all services

logs: ## Follow Docker Compose service logs
	@docker compose logs -f

test: ## Execute unit and integration test suite
	@echo "==> Running pytest test suite..."
	@pytest -v tests/

lint: ## Run Ruff linter and static code analysis
	@echo "==> Running Ruff linter..."
	@ruff check .

clean: ## Clean generated caches, bytecode, and temporary databases
	@echo "==> Cleaning cache artifacts..."
	@find . -type d -name "__pycache__" -exec rm -rf {} +
	@find . -type d -name ".pytest_cache" -exec rm -rf {} +
	@find . -type f -name "*.pyc" -delete
	@find . -type f -name "*.pyo" -delete
	@rm -f *.db *.sqlite *.sqlite3

