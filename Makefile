# Local entry points; CI (.github/workflows/ci.yml) runs the same targets.

SHELL  := /bin/bash
PYTHON ?= python3
VENV   ?= .venv
TARGET ?= dev

.DEFAULT_GOAL := help

.PHONY: help setup test lint format validate sample-data clean

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

setup: $(VENV)/.dev ## Create .venv with pyspark, pytest, pyyaml and ruff

$(VENV)/bin/python:
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/python -m pip install --quiet --upgrade "pip>=25.1"

$(VENV)/.dev: pyproject.toml | $(VENV)/bin/python
	$(VENV)/bin/python -m pip install --quiet --group dev
	@touch $@

test: $(VENV)/.dev ## pytest: transforms and expectation rules on local Spark, plus bundle config checks
	$(VENV)/bin/pytest

lint: $(VENV)/.dev ## ruff lint + format check
	$(VENV)/bin/ruff check .
	$(VENV)/bin/ruff format --check .

format: $(VENV)/.dev ## Apply ruff fixes and formatting
	$(VENV)/bin/ruff check --fix .
	$(VENV)/bin/ruff format .

# Runs only when the Databricks CLI is installed AND a workspace is configured
# (DATABRICKS_HOST or a ~/.databrickscfg profile); otherwise it explains why it
# skipped and exits 0, so `make validate` is safe on any laptop and in CI.
validate: ## databricks bundle validate -t $(TARGET) (skipped without CLI + host)
	@if ! command -v databricks > /dev/null 2>&1; then \
		echo "SKIP: Databricks CLI not installed (https://docs.databricks.com/dev-tools/cli/install.html)"; \
	elif [ -z "$$DATABRICKS_HOST" ] && [ -z "$$DATABRICKS_CONFIG_PROFILE" ] && [ ! -f "$$HOME/.databrickscfg" ]; then \
		echo "SKIP: no Databricks workspace configured (set DATABRICKS_HOST or run 'databricks configure')"; \
	else \
		databricks bundle validate -t $(TARGET); \
	fi

sample-data: ## Write synthetic shipment events to data/shipments.json
	$(PYTHON) scripts/generate_shipments.py --shipments 500 --out data/shipments.json

clean: ## Remove local Python and Spark artefacts
	find . -type d \( -name __pycache__ -o -name .pytest_cache -o -name .ruff_cache -o -name spark-warehouse \) -prune -exec rm -rf {} +
	rm -rf $(VENV) data
