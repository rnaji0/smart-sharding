#!/bin/bash
# Runs once per node on first initdb (primaries only; replicas are cloned).
set -euo pipefail

# Nodes connect to each other (and replicas stream) over the compose network.
echo "host replication all all trust" >> "$PGDATA/pg_hba.conf"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-'SQL'
	CREATE EXTENSION IF NOT EXISTS citus;
	CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
	CREATE EXTENSION IF NOT EXISTS citus_observer;
SQL
