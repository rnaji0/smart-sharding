# Shared helpers for the host-side cluster scripts.

COMPOSE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

compose() {
  docker compose --project-directory "$COMPOSE_DIR" -f "$COMPOSE_DIR/docker-compose.yml" "$@"
}

# psql on the coordinator; extra args are passed through (e.g. -c, -f -).
cpsql() {
  compose exec -T coordinator psql -U postgres -d postgres -v ON_ERROR_STOP=1 -X -q "$@"
}

# Single scalar value from a query.
cval() {
  cpsql -At -c "$1"
}

running_services() {
  compose ps --status running --services
}
