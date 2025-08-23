.PHONY: help install install-dev test lint format type-check security clean build docker-build docker-run pre-commit setup

# Default target
help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Installation
install: ## Install the package
	pip install -e .

install-dev: ## Install development dependencies
	pip install -e ".[dev,test]"

# Development
setup: ## Setup development environment
	pip install -e ".[dev,test]"
	pre-commit install
	pre-commit install --hook-type commit-msg

# Code quality
lint: ## Run linting with Ruff
	ruff check .

lint-fix: ## Run linting with Ruff and fix issues
	ruff check . --fix

format: ## Format code with Ruff
	ruff format .

format-check: ## Check code formatting
	ruff format --check .

type-check: ## Run type checking with MyPy
	mypy src/

security: ## Run security checks
	bandit -r src/
	safety check
	pip-audit

# Testing
test: ## Run tests
	pytest

test-cov: ## Run tests with coverage
	pytest --cov=src --cov-report=term-missing --cov-report=html

test-cov-xml: ## Run tests with XML coverage report
	pytest --cov=src --cov-report=xml

# Pre-commit
pre-commit: ## Run pre-commit hooks on all files
	pre-commit run --all-files

pre-commit-update: ## Update pre-commit hooks
	pre-commit autoupdate

# Quality checks (run all)
check: lint format-check type-check security test ## Run all quality checks

# Build
clean: ## Clean build artifacts
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	rm -rf htmlcov/
	find . -type d -name __pycache__ -delete
	find . -type f -name "*.pyc" -delete

build: clean ## Build the package
	python -m build

upload-test: build ## Upload to Test PyPI
	twine upload --repository testpypi dist/*

upload: build ## Upload to PyPI
	twine upload dist/*

# Docker
docker-build: ## Build Docker image
	docker build -t stock-tracker .

docker-run: ## Run Docker container
	docker run -it --rm stock-tracker

docker-compose-up: ## Start services with docker-compose
	docker-compose up -d

docker-compose-down: ## Stop services with docker-compose
	docker-compose down

# Development server
dev: ## Run in development mode
	python main.py

# Documentation (if using mkdocs)
docs-serve: ## Serve documentation locally
	mkdocs serve

docs-build: ## Build documentation
	mkdocs build

docs-deploy: ## Deploy documentation
	mkdocs gh-deploy

# Environment
env-create: ## Create virtual environment
	python -m venv venv

env-activate: ## Show command to activate virtual environment
	@echo "Run: source venv/bin/activate (Linux/Mac) or venv\\Scripts\\activate (Windows)"

# Database (if applicable)
db-migrate: ## Run database migrations
	@echo "Add your database migration commands here"

db-reset: ## Reset database
	@echo "Add your database reset commands here"

# Logs
logs-clean: ## Clean log files
	rm -rf logs/*.log

# All-in-one targets
init: env-create install-dev setup ## Initialize project for development
	@echo "Development environment setup complete!"
	@echo "Activate virtual environment with: source venv/bin/activate"

ci: check ## Run all CI checks locally
	@echo "All checks passed!"
