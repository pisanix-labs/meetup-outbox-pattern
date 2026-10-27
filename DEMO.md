# Roteiro de Demonstração — Transactional Outbox Pattern

Roteiro para apresentação **ao vivo** em meetup. Os comandos funcionam por
cópia e cola em Bash. Cada passo indica o **resultado esperado**.

> Dica: deixe dois terminais abertos — um para os comandos, outro rodando
> `docker compose logs -f api worker consumer` para acompanhar os logs.

Sumário:

1. [Parte 1: Arquitetura](#parte-1-arquitetura)
2. [Parte 2: Code review](#parte-2-code-review)
3. [Parte 3: Happy Path](#parte-3-happy-path)
4. [Parte 4: RabbitMQ offline](#parte-4-rabbitmq-offline)
5. [Parte 5: Falha de transação (ROLLBACK)](#parte-5-falha-de-transação-rollback)
6. [Parte 6: Mensagem duplicada (idempotência)](#parte-6-mensagem-duplicada-idempotência)
7. [Parte 7: Worker interrompido (lease)](#parte-7-worker-interrompido-lease)
8. [Parte 8: Conclusão](#parte-8-conclusão)

---

## Preparação

```bash
# Sobe toda a stack (postgres, rabbitmq, api, worker, consumer)
docker compose up --build -d

# Confirma que tudo está de pé
docker compose ps

# Em outro terminal, acompanhe os logs estruturados
docker compose logs -f api worker consumer
```

**Esperado:** 5 contêineres `Up` (postgres e rabbitmq como `healthy`).

Atalho para inspecionar o banco a qualquer momento:

```bash
./scripts/demo/inspect.sh
```

---

## Parte 1: Arquitetura

Fale sobre o **problema do dual write** e a **solução Outbox** (ver README,
seções "O problema: Dual Write" e "A solução: Transactional Outbox").

Pontos a destacar:

- Gravar no banco **e** publicar no broker não é atômico.
- A Outbox resolve isso gravando o evento **na mesma transação** do pedido.
- Um worker independente publica depois, com confirmação e retry.
- Mostre o diagrama de arquitetura e o diagrama de estados do README.

Os três serviços e suas responsabilidades:

- **API** — recebe HTTP, executa a transação (orders + outbox). **Não** publica.
- **Worker** — lê a Outbox, publica no RabbitMQ com Publisher Confirms.
- **Consumer** — consome a fila, processa de forma idempotente, ACK manual.

---

## Parte 2: Code review

Apresente os arquivos **nesta ordem**:

1. **`apps/api/src/orders/orders.controller.ts`** — `OrdersController`
   recebe o `POST /orders` e delega ao serviço.
2. **`apps/api/src/orders/orders.service.ts`** — `OrdersService.createOrder`:
   3. a **transação PostgreSQL** explícita (`BEGIN` ... `COMMIT` / `ROLLBACK`);
   4. a **inserção na Outbox** na mesma transação (o coração do padrão).
5. **`apps/worker/src/outbox/outbox.processor.ts`** — `OutboxProcessor`:
   o ciclo de polling, a recuperação de leases e o tratamento de retry/FAILED.
6. **`apps/worker/src/outbox/outbox.repository.ts`** — a query com
   **`FOR UPDATE SKIP LOCKED`** (`reserveBatch`).
7. **`libs/common/src/messaging/rabbitmq-publisher.service.ts`** — o
   **publisher** com `ConfirmChannel` e a **confirmação de publicação**
   (`publishWithConfirm`).
8. De volta ao processor: a **atualização do status** para `PUBLISHED`
   (`markPublished`), feita **só após** o confirm.
9. **`apps/consumer/src/consumer/order.consumer.ts`** e
   **`processed-events.service.ts`** — o **consumer** e a **idempotência**
   (`INSERT ... ON CONFLICT DO NOTHING`).

---

## Parte 3: Happy Path

Crie um pedido:

```bash
curl -s -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_name":"Joao","amount":150.00}'
```

**Esperado:** `201` com `{"id":"<uuid>","status":"CREATED"}`.

Nos logs, observe a sequência (campo `operation`):

- **api**: `order.request_received` → `order.transaction_started` →
  `order.persisted` → `outbox.event_created` → `order.transaction_committed`.
- **worker**: `outbox.event_claimed` → `outbox.publish_started` →
  `outbox.publish_confirmed` → `outbox.event_published`.
- **consumer**: `consumer.event_received` → `consumer.event_processed` →
  `consumer.message_acknowledged`.

Exemplo real de log do worker (JSON estruturado):

```json
{"level":30,"service":"worker","component":"OutboxProcessor","operation":"outbox.event_published","event_id":"513a9404-...","order_id":"c94ce1d6-...","attempt":1,"status":"PUBLISHED","duration_ms":7,"msg":"Evento publicado com sucesso"}
```

Confira o banco:

```bash
./scripts/demo/inspect.sh
```

**Esperado:**

- `orders`: 1 pedido `CREATED`.
- `outbox_events`: 1 evento `PUBLISHED` (`attempts = 1`, `published` preenchido).
- `processed_events`: 1 registro (mesmo `event_id` do evento) → o consumer
  processou.

---

## Parte 4: RabbitMQ offline

Demonstra que **a API não depende do broker**.

```bash
# Derruba o broker
docker compose stop rabbitmq

# Cria um pedido mesmo com o RabbitMQ fora
curl -s -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_name":"Maria","amount":99.90}'
```

**Esperado:** ainda retorna `201` — o pedido é persistido normalmente.

Veja o evento preso aguardando publicação:

```bash
docker compose exec postgres psql -U postgres -d outbox -c \
  "SELECT status, attempts, last_error IS NOT NULL AS has_error
     FROM outbox_events WHERE status <> 'PUBLISHED';"
```

**Esperado:** o evento fica `PENDING`, com `attempts` crescendo e
`has_error = t`. Nos logs do **worker** aparecem `outbox.publish_failed` e
`outbox.retry_scheduled` (backoff).

Agora restaure o broker:

```bash
docker compose start rabbitmq
```

**Esperado:** após o broker ficar `healthy` e o backoff expirar, o worker
reconecta sozinho (`rabbitmq.connected`) e publica o evento. Em alguns segundos:

```bash
docker compose exec postgres psql -U postgres -d outbox -c \
  "SELECT status, count(*) FROM outbox_events GROUP BY status;"
```

**Esperado:** todos os eventos em `PUBLISHED`. Nenhum pedido ficou sem evento.

---

## Parte 5: Falha de transação (ROLLBACK)

Mostra que um erro no meio da transação **não deixa dados isolados**.

```bash
# Habilita a falha simulada só na API e recria o serviço
SIMULATE_API_FAILURE=true docker compose up -d --no-deps api
```

Anote as contagens atuais e tente criar um pedido:

```bash
docker compose exec postgres psql -U postgres -d outbox -c \
  "SELECT (SELECT count(*) FROM orders) AS orders,
          (SELECT count(*) FROM outbox_events) AS outbox;"

curl -s -o /dev/null -w "HTTP %{http_code}\n" -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_name":"Rollback","amount":55.00}'
```

**Esperado:** `HTTP 500`. Nos logs da **api**: `order.persisted` seguido de
`order.transaction_rolled_back` (o evento Outbox **nunca** é criado).

Confira que **nada** foi persistido:

```bash
docker compose exec postgres psql -U postgres -d outbox -c \
  "SELECT (SELECT count(*) FROM orders) AS orders,
          (SELECT count(*) FROM outbox_events) AS outbox;"
```

**Esperado:** as contagens **não mudaram**. O `ROLLBACK` desfez a inserção do
pedido — provando a atomicidade.

Desative a simulação:

```bash
docker compose up -d --no-deps api
```

---

## Parte 6: Mensagem duplicada (idempotência)

Simula um crash do worker **após** o Publisher Confirm, mas **antes** de marcar
`PUBLISHED`. O evento volta a ser elegível e é **republicado**, chegando duas
vezes ao consumer — onde a idempotência entra em ação.

```bash
# Crash após o confirm + lease curto (8s) para a demo ser rápida
SIMULATE_CRASH_AFTER_CONFIRM=true OUTBOX_LEASE_DURATION_MS=8000 \
  docker compose up -d --no-deps worker

# Cria o pedido
curl -s -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_name":"Dup","amount":12.34}'
```

O que acontece (acompanhe os logs):

1. O worker publica, recebe o confirm, e **cai** (`outbox.simulated_crash` →
   processo encerra; o Docker reinicia o contêiner).
2. A mensagem **já chegou** ao consumer uma vez (`consumer.event_processed`).
3. Após ~8s o lease expira; o worker recupera o evento e **republica**.
4. A mensagem chega **de novo** ao consumer → `consumer.duplicate_detected`.

Verifique a idempotência:

```bash
docker compose exec postgres psql -U postgres -d outbox -c \
  "SELECT count(*) AS total, count(DISTINCT event_id) AS distinct_ids
     FROM processed_events;"
```

**Esperado:** `total == distinct_ids`. Mesmo tendo sido **entregue mais de uma
vez**, cada `event_id` foi processado **uma única vez**. Nos logs do consumer há
`consumer.duplicate_detected` para o evento republicado.

Desative a simulação:

```bash
docker compose up -d --no-deps worker
```

**Esperado:** o worker volta ao normal e o evento termina em `PUBLISHED`.

---

## Parte 7: Worker interrompido (lease)

Mostra que um worker interrompido no meio do processamento **não prende** o
evento: o lease expira e outro ciclo (ou outra instância) recupera.

```bash
# Interrompe o worker
docker compose stop worker

# Cria um pedido — o evento ficará PENDING (ninguém para publicar)
curl -s -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_name":"Lease","amount":33.00}'

# Veja o evento aguardando
docker compose exec postgres psql -U postgres -d outbox -c \
  "SELECT status, attempts FROM outbox_events WHERE status <> 'PUBLISHED';"
```

**Esperado:** evento `PENDING` (ou `PROCESSING`, se tiver sido reservado antes
da parada). Reinicie o worker:

```bash
docker compose start worker
```

**Esperado:** o worker recupera reservas expiradas (`outbox.leases_recovered`
nos logs) e publica o evento, que vai a `PUBLISHED`. Verifique:

```bash
docker compose exec postgres psql -U postgres -d outbox -c \
  "SELECT status, count(*) FROM outbox_events GROUP BY status;"
```

### Reprocessamento manual de FAILED

Se um evento exceder `OUTBOX_MAX_ATTEMPTS`, ele vira `FAILED`. Para reenfileirar:

```bash
./scripts/demo/reprocess-failed.sh            # todos os FAILED
./scripts/demo/reprocess-failed.sh <event_id> # um específico
```

**Esperado:** os eventos voltam para `PENDING` e o worker os publica no próximo
ciclo.

---

## Parte 8: Conclusão

**O que o padrão garante:**

- **Atomicidade** entre dado de negócio e evento (mesmo COMMIT). Sem dual write.
- **Nenhum evento perdido**: se o pedido existe, o evento existe e será
  publicado — mesmo que o broker caia.
- **Entrega pelo menos uma vez** (at-least-once) ao consumer.

**O que continua sendo responsabilidade da aplicação:**

- **Idempotência no consumer** — duplicatas acontecem (Parte 6).
- **Ordenação** — esta demo não garante ordem global.
- **Manutenção da Outbox** — arquivar/limpar eventos `PUBLISHED` antigos.
- **Monitorar `FAILED`** — e reprocessar quando apropriado.

Encerre reforçando a mensagem central: foi possível acompanhar, **pelo código,
pelo banco e pelos logs**, a jornada completa de um evento — criação,
publicação, processamento — e como o padrão se comporta sob falhas.

---

## Limpeza

```bash
docker compose down -v   # derruba tudo e zera os volumes
```
