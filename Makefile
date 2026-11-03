CITUS_DIR := infra/citus
COMPOSE   := docker compose --project-directory $(CITUS_DIR) -f $(CITUS_DIR)/docker-compose.yml
# Optional compose profiles: scale (worker-4), replica (worker-1-replica). e.g. make up PROFILES="scale replica"
PROFILES  ?=
PROFILE_FLAGS := $(foreach p,$(PROFILES),--profile $(p))

.DEFAULT_GOAL := help
.PHONY: help build up init down reset psql smoke status logs

help: ## List targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-z-]+:.*## / {printf "  %-8s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

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
