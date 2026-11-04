CITUS_DIR := infra/citus
COMPOSE   := docker compose --project-directory $(CITUS_DIR) -f $(CITUS_DIR)/docker-compose.yml
# Optional compose profiles: scale (worker-4), replica (worker-1-replica). e.g. make up PROFILES="scale replica"
PROFILES  ?=
PROFILE_FLAGS := $(foreach p,$(PROFILES),--profile $(p))

SIM      := cd sim && uv run shardsim
SCALE    ?= small
LAYOUT   ?= tenant
SCENARIO ?= steady
RUN      ?= latest
ARGS     ?=

.DEFAULT_GOAL := help
.PHONY: help build up init down reset psql smoke status logs sim-setup sim-layout sim-run sim-report sim-scenarios sim-test sim-test-cluster
.PHONY: engine-build engine-deploy engine-test-c engine-test-sql collector-test collector-run

help: ## List targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-z-]+:.*## / {printf "  %-16s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build the arm64/amd64 Citus image from source
	$(COMPOSE) build

up: ## Start the cluster and register workers
	$(COMPOSE) $(PROFILE_FLAGS) up -d --wait
	$(CITUS_DIR)/scripts/init.sh

init: ## (Re)register running workers with the coordinator
	$(CITUS_DIR)/scripts/init.sh

down: ## Stop the cluster, keep data
	$(COMPOSE) --profile scale --profile replica down

reset: ## Stop the cluster and delete all data volumes
	$(COMPOSE) --profile scale --profile replica down -v

psql: ## Open psql on the coordinator
	$(COMPOSE) exec coordinator psql -U postgres

smoke: ## Verify routing, moves, splits, isolation, rebalance, stats
	$(CITUS_DIR)/scripts/smoke.sh

status: ## Nodes, shard counts and sizes per node
	@$(COMPOSE) exec -T coordinator psql -U postgres -X -c \
	  "SELECT nodeid, nodename, noderole, isactive, shouldhaveshards FROM pg_dist_node ORDER BY nodeid;" -c \
	  "SELECT nodename, count(*) AS shards, pg_size_pretty(sum(shard_size)) AS size FROM citus_shards GROUP BY 1 ORDER BY 1;"

logs: ## Tail cluster logs
	$(COMPOSE) $(PROFILE_FLAGS) logs -f --tail=100

sim-setup: ## Schema + layout + data (SCALE=tiny|small|medium LAYOUT=tenant|naive|unsharded)
	$(SIM) setup --scale $(SCALE) --layout $(LAYOUT)

sim-layout: ## Re-shard loaded data (LAYOUT=...)
	$(SIM) layout apply $(LAYOUT)

sim-run: ## Run a scenario (SCENARIO=steady ARGS="--compress 1440")
	$(SIM) run $(SCENARIO) $(ARGS)

sim-report: ## Report on a run (RUN=latest|<run-id>)
	$(SIM) report $(RUN)

sim-scenarios: ## List bundled scenarios
	$(SIM) scenarios

sim-test: ## Simulator unit tests
	cd sim && uv run pytest -q -m "not cluster"

sim-test-cluster: ## Simulator end-to-end tests against the running cluster (reloads tiny data)
	cd sim && uv run pytest -q -m cluster

engine-build: ## Incremental build of the Citus fork in a toolchain container (ccache)
	docker build -q --target toolchain -t smart-sharding/citus-toolchain -f $(CITUS_DIR)/Dockerfile . >/dev/null
	docker run --rm -v "$(CURDIR)/engine/citus:/src/citus" -v smart-sharding-ccache:/ccache -w /src/citus \
	  smart-sharding/citus-toolchain bash -c 'test -f config.status || ./configure >/dev/null; make -j"$$(nproc)" -s'

engine-deploy: engine-build build ## Rebuild the image from the fork and restart the cluster on it
	$(COMPOSE) $(PROFILE_FLAGS) up -d --wait --force-recreate
	$(CITUS_DIR)/scripts/init.sh

engine-test-c: ## Unit tests for the observer's sketch and event ring (plain C)
	$(MAKE) -C engine/tests/c test

engine-test-sql: ## citus_observer integration tests against the running cluster
	cd engine/tests/sql && uv run pytest -q -m cluster

collector-test: ## engine-collector unit tests
	cd engine/collector && uv run pytest -q -m "not cluster"

collector-run: ## Poll engine telemetry into runs/engine-<time>.duckdb (ARGS="--duration 60")
	cd engine/collector && uv run engine-collector run --out ../../runs/engine-$$(date +%Y%m%d-%H%M%S).duckdb $(ARGS)
