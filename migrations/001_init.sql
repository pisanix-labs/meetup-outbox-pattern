-- =====================================================================
-- Migração inicial do Transactional Outbox Pattern
--
-- Executada automaticamente pelo PostgreSQL na primeira inicialização
-- (montada em /docker-entrypoint-initdb.d via docker-compose).
--
-- Três tabelas:
--   orders           -> dados de negócio (pedidos)
--   outbox_events    -> eventos a publicar (a "caixa de saída")
--   processed_events -> IDs já processados pelo consumer (idempotência)
-- =====================================================================

-- Extensão para gerar UUIDs no próprio banco (usada só por conveniência).
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ---------------------------------------------------------------------
-- Tabela de pedidos (dado de negócio).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders (
  id            UUID PRIMARY KEY,
  customer_name VARCHAR(255)   NOT NULL,
  amount        NUMERIC(12, 2) NOT NULL,
  status        VARCHAR(32)    NOT NULL,
  created_at    TIMESTAMPTZ    NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- Tabela Outbox: o coração do padrão.
--
-- O pedido e o evento são gravados na MESMA transação. Enquanto o evento
-- não é publicado no broker, ele vive aqui com status PENDING/PROCESSING.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS outbox_events (
  id              UUID PRIMARY KEY,
  aggregate_id    UUID        NOT NULL,          -- id do pedido relacionado
  event_type      VARCHAR(64) NOT NULL,          -- ex.: 'OrderCreated'
  payload         JSONB       NOT NULL,          -- envelope completo do evento
  status          VARCHAR(16) NOT NULL DEFAULT 'PENDING',
  attempts        INTEGER     NOT NULL DEFAULT 0,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at    TIMESTAMPTZ,                   -- preenchido ao publicar
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- quando fica elegível
  locked_until    TIMESTAMPTZ,                   -- expiração do lease (reserva)
  last_error      TEXT                           -- último erro de publicação
);

-- Índice para a seleção de eventos elegíveis pelo worker.
-- O worker busca eventos PENDING cujo next_attempt_at já chegou, ordenados
-- por created_at. Este índice parcial cobre exatamente esse acesso.
CREATE INDEX IF NOT EXISTS idx_outbox_pending_ready
  ON outbox_events (next_attempt_at, created_at)
  WHERE status = 'PENDING';

-- Índice para recuperar reservas expiradas: eventos PROCESSING cujo
-- locked_until já passou (worker que caiu durante o processamento).
CREATE INDEX IF NOT EXISTS idx_outbox_processing_expired
  ON outbox_events (locked_until)
  WHERE status = 'PROCESSING';

-- ---------------------------------------------------------------------
-- Tabela de idempotência do consumer.
--
-- A PRIMARY KEY em event_id garante, via restrição UNIQUE, que o mesmo
-- evento não seja processado duas vezes (entrega at-least-once do broker).
-- O registro aqui é o "efeito de negócio" da demonstração.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS processed_events (
  event_id     UUID PRIMARY KEY,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
