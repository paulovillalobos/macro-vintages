# Everything a reviewer needs, from a clean clone:
#   make db-up      local PostgreSQL on :5441 (Homebrew binaries, data in .local/)
#   make test       lint + types + tests with coverage
#   make demo       migrate, ingest the live ECB/Eurostat series, start the API on :8000
#
# Set MV_TEST_DATABASE_URL / MV_DATABASE_URL to use another PostgreSQL (CI does).

UV       ?= uv
RUN      := $(UV) run --frozen
PORT     ?= 8000
export MV_DATABASE_URL ?= postgresql://postgres@127.0.0.1:5441/macro_vintages

.PHONY: install db-up db-down lint typecheck unit test ci migrate sync ingest worker api key \
        demo loadtest explain probe mutation-check docker-build

install:
	$(UV) sync --frozen

db-up:
	@scripts/dev-db.sh up

db-down:
	@scripts/dev-db.sh down

lint: install
	$(RUN) ruff check .
	$(RUN) ruff format --check .

typecheck: install
	$(RUN) mypy

unit: install
	$(RUN) pytest tests/unit -q

# The integration and race tests need PostgreSQL; start the local one unless
# the caller points MV_TEST_DATABASE_URL elsewhere.
test: install lint typecheck
	@if [ -z "$$MV_TEST_DATABASE_URL" ]; then scripts/dev-db.sh up; fi
	$(RUN) pytest --cov --cov-report=term --cov-report=xml -q

ci: test

migrate: install
	$(RUN) macro-vintages migrate

sync: migrate
	$(RUN) macro-vintages sync-catalogue

ingest: sync
	$(RUN) macro-vintages worker --drain

worker: sync
	$(RUN) macro-vintages worker

api: migrate
	$(RUN) macro-vintages api --port $(PORT)

key: migrate
	@$(RUN) macro-vintages create-key --name $${NAME:-local}

demo: db-up ingest
	@echo "API key (shown once):"; $(RUN) macro-vintages create-key --name demo
	$(RUN) macro-vintages api --port $(PORT)

loadtest: install
	$(RUN) python scripts/loadtest.py

explain: install
	$(RUN) python scripts/explain.py

probe: install
	$(RUN) python scripts/probe_sources.py

mutation-check: install
	scripts/mutation_check.sh

docker-build:
	docker build -t macro-vintages:local .
