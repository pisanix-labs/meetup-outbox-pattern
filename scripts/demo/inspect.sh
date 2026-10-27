#!/usr/bin/env bash
# =====================================================================
# Consultas SQL úteis para acompanhar a demonstração.
# Uso: ./scripts/demo/inspect.sh
# =====================================================================
set -euo pipefail

echo "==================== ORDERS ===================="
docker compose exec -T postgres psql -U postgres -d outbox -c \
  "SELECT id, customer_name, amount, status, created_at
     FROM orders ORDER BY created_at DESC LIMIT 10;"

echo "================= OUTBOX_EVENTS ================="
docker compose exec -T postgres psql -U postgres -d outbox -c \
  "SELECT id, event_type, status, attempts, next_attempt_at, locked_until, last_error
     FROM outbox_events ORDER BY created_at DESC LIMIT 10;"

echo "=============== PROCESSED_EVENTS ==============="
docker compose exec -T postgres psql -U postgres -d outbox -c \
  "SELECT event_id, processed_at
     FROM processed_events ORDER BY processed_at DESC LIMIT 10;"

echo "============= RESUMO POR STATUS ================"
docker compose exec -T postgres psql -U postgres -d outbox -c \
  "SELECT status, count(*) FROM outbox_events GROUP BY status ORDER BY status;"
