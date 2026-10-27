#!/usr/bin/env bash
# =====================================================================
# Reprocessamento manual de eventos FAILED.
#
# Reseta eventos em status FAILED de volta para PENDING, zerando o próximo
# horário elegível para que o worker os publique imediatamente. O attempts
# é preservado para manter o histórico; ajuste se quiser reiniciar a contagem.
#
# Uso:
#   ./scripts/demo/reprocess-failed.sh            # reprocessa todos os FAILED
#   ./scripts/demo/reprocess-failed.sh <event_id> # reprocessa um específico
# =====================================================================
set -euo pipefail

EVENT_ID="${1:-}"

if [[ -n "${EVENT_ID}" ]]; then
  WHERE="status = 'FAILED' AND id = '${EVENT_ID}'"
else
  WHERE="status = 'FAILED'"
fi

docker compose exec -T postgres psql -U postgres -d outbox -c \
  "UPDATE outbox_events
      SET status = 'PENDING',
          next_attempt_at = now(),
          locked_until = NULL,
          last_error = NULL
    WHERE ${WHERE}
  RETURNING id, status, attempts;"

echo "Eventos reenfileirados. Acompanhe: docker compose logs -f worker"
