/**
 * Tipos e constantes compartilhadas do domínio Outbox.
 *
 * Mantemos tudo explícito e simples: sem interfaces genéricas ou abstrações
 * que não ajudem a entender o padrão.
 */

/** Estados possíveis de um evento na tabela outbox_events. */
export enum OutboxStatus {
  /** Recém-criado pela API, aguardando o worker. */
  PENDING = 'PENDING',
  /** Reservado por um worker (lease ativo), em processamento. */
  PROCESSING = 'PROCESSING',
  /** Publicado com sucesso e confirmado pelo RabbitMQ. */
  PUBLISHED = 'PUBLISHED',
  /** Excedeu o número máximo de tentativas. */
  FAILED = 'FAILED',
}

/** Status de negócio de um pedido. */
export enum OrderStatus {
  CREATED = 'CREATED',
}

/** Tipo do evento emitido quando um pedido é criado. */
export const EVENT_ORDER_CREATED = 'OrderCreated';

/**
 * Envelope padrão de evento trafegado no RabbitMQ.
 *
 * Formato combinado com o consumer. O `event_id` é imutável entre tentativas
 * e é usado também como `messageId` no broker, garantindo idempotência.
 */
export interface EventEnvelope<T = unknown> {
  event_id: string;
  event_type: string;
  occurred_at: string;
  data: T;
}

/** Payload de dados do evento OrderCreated. */
export interface OrderCreatedData {
  order_id: string;
  customer_name: string;
  amount: number;
}
