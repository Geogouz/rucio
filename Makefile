# Rucio Development Makefile
# Copyright European Organization for Nuclear Research (CERN) since 2012
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

.PHONY: help install test clean check

# Default profiles for external services
RUCIO_DEV_PROFILES ?= storage,externalmetadata

# Virtual environment path (used only by install target)
VENV := .venv

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[1;33m
RED := \033[0;31m
NC := \033[0m

##@ General

help: ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\n$(BLUE)Rucio Development Makefile$(NC)\n\nUsage:\n  make $(YELLOW)<target>$(NC)\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  $(YELLOW)%-25s$(NC) %s\n", $$1, $$2 } /^##@/ { printf "\n$(BLUE)%s$(NC)\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
	@echo ""
	@echo "$(GREEN)Quick Start (Local Development):$(NC)"
	@echo "  1. make install                  # Set up venv + dependencies + hooks"
	@echo "  2. source .venv/bin/activate     # Activate virtual environment"
	@echo "  3. make check                    # Run fast code quality checks"
	@echo "  4. make test                     # Run unit tests"
	@echo ""
	@echo "$(GREEN)CI Usage:$(NC)"
	@echo "  make install-deps                # Install dependencies (no venv)"
	@echo "  make check                       # Run quality checks"
	@echo "  make test                        # Run unit tests"
	@echo ""
	@echo "$(GREEN)Docker Dev Services:$(NC)"
	@echo "  make services-start                                 # Default: storage,externalmetadata"
	@echo "  make services-start RUCIO_DEV_PROFILES=storage,iam  # Custom profiles"
	@echo ""
	@echo "$(YELLOW)Tip: All 'check' targets use pre-commit hooks - same as CI$(NC)"
	@echo ""

##@ Installation

install: ## Install Rucio in venv and setup pre-commit (for local development)
	@echo "$(BLUE)Creating virtual environment...$(NC)"
	@python3 -m venv $(VENV)
	@echo "$(BLUE)Installing dependencies...$(NC)"
	@$(VENV)/bin/pip install --upgrade pip setuptools wheel
	@$(VENV)/bin/pip install -r requirements/requirements.server.txt
	@$(VENV)/bin/pip install -r requirements/requirements.dev.txt
	@$(VENV)/bin/pip install pre-commit
	@echo "$(BLUE)Setting up pre-commit hooks...$(NC)"
	@$(VENV)/bin/pre-commit install --install-hooks -t pre-commit
	@if ! $(VENV)/bin/python3 -c "from magic import Magic" 2>/dev/null; then \
		echo "$(YELLOW)⚠ Warning: libmagic not found$(NC)"; \
		echo "$(YELLOW)  Some tests (dumper) will fail without it.$(NC)"; \
		if command -v brew >/dev/null 2>&1; then \
			echo "$(YELLOW)  Install with: brew install libmagic$(NC)"; \
		elif command -v apt-get >/dev/null 2>&1; then \
			echo "$(YELLOW)  Install with: sudo apt-get install libmagic1$(NC)"; \
		else \
			echo "$(YELLOW)  Install libmagic using your system package manager$(NC)"; \
		fi; \
	else \
		echo "$(GREEN) libmagic found$(NC)"; \
	fi
	@echo "$(GREEN) Development environment ready$(NC)"
	@echo "$(YELLOW) Run 'source .venv/bin/activate' to activate the virtual environment$(NC)"

install-hooks: ## Install/reinstall pre-commit hooks
	@pre-commit install --install-hooks -t pre-commit
	@echo "$(GREEN) Pre-commit hooks installed$(NC)"

##@ Code Quality (via pre-commit)

check: ## Run fast code quality checks (ruff, file hygiene)
	@echo "$(BLUE)Running fast quality checks...$(NC)"
	@pre-commit run --all-files --hook-stage pre-commit --show-diff-on-failure
	@echo "$(GREEN) Fast checks passed$(NC)"

check-all: ## Run ALL checks including slow ones (pyright, bandit)
	@echo "$(BLUE)Running all quality checks (this may take a while)...$(NC)"
	@pre-commit run --all-files --hook-stage pre-commit --show-diff-on-failure
	@pre-commit run --all-files --hook-stage manual --show-diff-on-failure
	@echo "$(GREEN) All checks passed$(NC)"

check-modified: ## Run checks only on modified files (fast)
	@echo "$(BLUE)Running checks on modified files...$(NC)"
	@pre-commit run --show-diff-on-failure
	@echo "$(GREEN) Modified files checked$(NC)"

# Individual check targets for convenience
lint: ## Run ruff linting only
	@pre-commit run ruff --all-files --show-diff-on-failure

format: ## Run ruff formatting only (currently disabled - only linting enabled)
	@echo "$(YELLOW)Note: ruff-format is not currently enabled in pre-commit config$(NC)"
	@echo "$(YELLOW)To format code: ruff format .$(NC)"

headers: ## Check license headers only
	@pre-commit run add-header --all-files --hook-stage pre-commit --show-diff-on-failure

type-check: ## Run pyright type checking (slow)
	@pre-commit run pyright --all-files --hook-stage manual --show-diff-on-failure

security: ## Run security scanning (bandit)
	@pre-commit run bandit --all-files --hook-stage manual --show-diff-on-failure

##@ Testing

test: ## Run unit tests
	@echo "$(BLUE)Running unit tests...$(NC)"
	@python3 -m pytest tests/rucio

test-cov: ## Run unit tests with coverage
	@echo "$(BLUE)Running unit tests with coverage...$(NC)"
	@python3 -m pytest tests/rucio \
		--cov=lib/rucio \
		--cov-report=term-missing \
		--cov-report=html
	@echo "$(GREEN) Coverage report generated in htmlcov/$(NC)"

test-verbose: ## Run unit tests with verbose output
	@python3 -m pytest tests/rucio -v

test-integration: ## Run integration tests (requires services)
	@if ! docker exec dev-rucio-1 true 2>/dev/null; then \
		echo "$(RED)✗ dev-rucio-1 container not running$(NC)"; \
		echo "$(YELLOW) Start services with: make services-start$(NC)"; \
		exit 1; \
	fi
	@echo "$(BLUE)Running integration tests...$(NC)"
	@docker exec -t dev-rucio-1 tools/run_tests.sh -ir
	@echo "$(GREEN) Integration tests passed$(NC)"

##@ Docker Services

services-start: ## Start external services (storage, metadata, etc)
	@echo "$(BLUE)Starting services with profiles: $(RUCIO_DEV_PROFILES)$(NC)"
	@PROFILE_FLAGS=$$(echo "$(RUCIO_DEV_PROFILES)" | sed 's/,/ --profile /g' | sed 's/^/--profile /'); \
	docker compose -f etc/docker/dev/docker-compose.yml $$PROFILE_FLAGS up -d
	@echo "$(GREEN) Services started$(NC)"
	@echo "$(YELLOW) Run 'make test-integration' to run integration tests$(NC)"

services-stop: ## Stop all dev services and cleanup
	@echo "$(YELLOW)Stopping all dev services...$(NC)"
	@docker ps -q --filter "name=^dev-" | xargs -r docker stop 2>/dev/null || true
	@docker ps -aq --filter "name=^dev-" | xargs -r docker rm 2>/dev/null || true
	@echo "$(YELLOW)Removing all dev volumes...$(NC)"
	@docker volume ls -q --filter "name=^dev_" | xargs -r docker volume rm 2>/dev/null || true
	@echo "$(YELLOW)Removing dev network...$(NC)"
	@docker network rm ruciodevnetwork 2>/dev/null || true
	@echo "$(GREEN) Full cleanup complete$(NC)"

services-logs: ## Show logs from all dev services
	@docker compose -f etc/docker/dev/docker-compose.yml logs -f

services-status: ## Show status of dev services
	@echo "$(BLUE)Dev services status:$(NC)"
	@docker ps --filter "name=^dev-" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

##@ Cleanup

clean: ## Clean temporary files and caches
	@echo "$(YELLOW)Cleaning temporary files...$(NC)"
	@find . -type f -name '*.pyc' -delete
	@find . -type d -name '__pycache__' -delete
	@find . -type d -name '*.egg-info' -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name '.pytest_cache' -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name '.ruff_cache' -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name 'htmlcov' -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name '.coverage' -delete
	@echo "$(GREEN) Cleanup complete$(NC)"

clean-venv: clean ## Remove virtual environment
	@echo "$(YELLOW)Removing virtual environment...$(NC)"
	@rm -rf $(VENV)
	@echo "$(GREEN) Virtual environment removed$(NC)"
	@echo "$(YELLOW) Run 'make install' to recreate$(NC)"

##@ Development

dev-shell: ## Open a shell in the dev container
	@if ! docker exec dev-rucio-1 true 2>/dev/null; then \
		echo "$(RED)✗ dev-rucio-1 container not running$(NC)"; \
		echo "$(YELLOW) Start services with: make services-start$(NC)"; \
		exit 1; \
	fi
	@docker exec -it dev-rucio-1 /bin/bash

pre-commit-update: ## Update pre-commit hooks to latest versions
	@pre-commit autoupdate
	@echo "$(GREEN) Pre-commit hooks updated$(NC)"

pre-commit-clean: ## Clean pre-commit cache
	@pre-commit clean
	@echo "$(GREEN) Pre-commit cache cleaned$(NC)"

##@ Information

info: ## Show development environment info
	@echo "$(BLUE)Development Environment Information$(NC)"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "Python version:    $$(python3 --version 2>/dev/null || echo 'not installed')"
	@echo "Virtual env:       $(VENV)"
	@echo "Pre-commit:        $$(pre-commit --version 2>/dev/null || echo 'not installed')"
	@echo "Docker:            $$(docker --version 2>/dev/null || echo 'not installed')"
	@echo ""
	@echo "$(BLUE)Quick Commands$(NC)"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "make check         # Fast quality checks (~5s)"
	@echo "make test          # Run unit tests"
