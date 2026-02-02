SHELL := /bin/bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c

# =========================================================
# Config
# =========================================================
PYTHON ?= python
UVICORN ?= uvicorn
ALEMBIC ?= alembic
BACKEND_HOST ?= 0.0.0.0
BACKEND_PORT ?= 8000

# =========================================================
# Help
# =========================================================
.PHONY: help
help:
	@echo ""
	@echo "=== AskTony Dev Makefile ============================="
	@echo ""
	@echo "Git helpers:"
	@echo "  make git-sync-main"
	@echo "  make git-new BR=feature/xxx"
	@echo "  make git-pushandpr MSG='commit message'"
	@echo "  make git-push MSG='commit message'"
	@echo "  make git-clean BR=feature/xxx"
	@echo ""
	@echo "====================================================="
	@echo ""

# =========================================================
# 1) Git helpers
# =========================================================
.PHONY: git-sync-main git-new git-push git-clean

git-sync-main:
	git checkout main
	git pull

git-new: git-sync-main
	@if [ -z "$(BR)" ]; then echo "ERR: BR is required, e.g. BR=feature/my-thing"; exit 1; fi
	git checkout -b "$(BR)"

git-pushandpr:
	@if [ -z "$(MSG)" ]; then echo "ERR: MSG is required, e.g. MSG='Add xxx'"; exit 1; fi
	git add .
	git commit -m "$(MSG)"
	git push -u origin "$$(git rev-parse --abbrev-ref HEAD)"

git-push:
	@if [ -z "$(MSG)" ]; then echo "ERR: MSG is required, e.g. MSG='Add xxx'"; exit 1; fi
	git add .
	git commit -m "$(MSG)"
	git push

git-clean: git-sync-main
	@if [ -z "$(BR)" ]; then echo "ERR: BR is required, e.g. BR=feature/my-thing"; exit 1; fi
	git branch -d "$(BR)"
