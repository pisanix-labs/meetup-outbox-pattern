import { Injectable, NotFoundException } from '@nestjs/common';
import { InjectPinoLogger, PinoLogger } from 'nestjs-pino';
import { randomUUID } from 'crypto';
import {
  AppConfigService,
  DatabaseService,
  EVENT_ORDER_CREATED,
  EventEnvelope,
  OrderCreatedData,
  OrderStatus,
} from '@app/common';
import { CreateOrderDto } from './dto/create-order.dto';

/** Representação de um pedido lido do banco. */
export interface OrderRow {
  id: string;
  customer_name: string;
  amount: string; // NUMERIC chega como string na lib `pg`
  status: string;
  created_at: Date;
}

/**
 * Serviço de pedidos.
 *
 * Este é o CORAÇÃO do Transactional Outbox Pattern na API:
 * o pedido e o evento Outbox são gravados na MESMA transação PostgreSQL.
 * Ou ambos são confirmados (COMMIT), ou nenhum é (ROLLBACK).
 *
 * A API NÃO publica no RabbitMQ — isso é responsabilidade do worker.
 * Assim, criar pedidos não depende da disponibilidade do broker.
 */
@Injectable()
export class OrdersService {
  constructor(
    private readonly db: DatabaseService,
    private readonly config: AppConfigService,
    @InjectPinoLogger(OrdersService.name)
    private readonly logger: PinoLogger,
  ) {}

  /**
   * Cria um pedido e seu evento OrderCreated de forma atômica.
   *
   * O "dual write" (gravar no banco E publicar no broker) é justamente o
   * problema que o padrão resolve: aqui NÃO publicamos nada, apenas gravamos
   * o evento na mesma transação do pedido.
   */
  async createOrder(dto: CreateOrderDto): Promise<{ id: string; status: string }> {
    const start = Date.now();
    const orderId = randomUUID();
    const eventId = randomUUID();

    this.logger.info(
      {
        component: 'OrdersService',
        operation: 'order.request_received',
        order_id: orderId,
      },
      'Requisição de criação de pedido recebida',
    );

    // Obtemos um client dedicado do pool para controlar a transação.
    const client = await this.db.getClient();

    try {
      // ---- INÍCIO DA TRANSAÇÃO ----
      await client.query('BEGIN');
      this.logger.info(
        {
          component: 'OrdersService',
          operation: 'order.transaction_started',
          order_id: orderId,
        },
        'Transação iniciada',
      );

      // 1) Persiste o pedido.
      await client.query(
        `INSERT INTO orders (id, customer_name, amount, status, created_at)
         VALUES ($1, $2, $3, $4, now())`,
        [orderId, dto.customer_name, dto.amount, OrderStatus.CREATED],
      );
      this.logger.info(
        {
          component: 'OrdersService',
          operation: 'order.persisted',
          order_id: orderId,
        },
        'Pedido persistido',
      );

      // --- Ponto de simulação de falha (Cenário 3) ---
      // Habilitado via SIMULATE_API_FAILURE=true apenas para demonstração.
      // Lança erro DEPOIS de inserir o pedido e ANTES de inserir o evento,
      // provando que o ROLLBACK desfaz também a inserção do pedido.
      if (this.config.simulateApiFailure) {
        throw new Error(
          'Falha simulada após persistir o pedido (SIMULATE_API_FAILURE=true)',
        );
      }

      // 2) Monta o envelope do evento e o insere na Outbox.
      const data: OrderCreatedData = {
        order_id: orderId,
        customer_name: dto.customer_name,
        amount: dto.amount,
      };
      const envelope: EventEnvelope<OrderCreatedData> = {
        event_id: eventId,
        event_type: EVENT_ORDER_CREATED,
        occurred_at: new Date().toISOString(),
        data,
      };

      await client.query(
        `INSERT INTO outbox_events
           (id, aggregate_id, event_type, payload, status, attempts, created_at, next_attempt_at)
         VALUES ($1, $2, $3, $4, 'PENDING', 0, now(), now())`,
        [eventId, orderId, EVENT_ORDER_CREATED, JSON.stringify(envelope)],
      );
      this.logger.info(
        {
          component: 'OrdersService',
          operation: 'outbox.event_created',
          order_id: orderId,
          event_id: eventId,
        },
        'Evento Outbox registrado na mesma transação',
      );

      // ---- COMMIT: confirma pedido + evento atomicamente ----
      await client.query('COMMIT');
      this.logger.info(
        {
          component: 'OrdersService',
          operation: 'order.transaction_committed',
          order_id: orderId,
          event_id: eventId,
          status: OrderStatus.CREATED,
          duration_ms: Date.now() - start,
        },
        'Transação confirmada (COMMIT)',
      );

      return { id: orderId, status: OrderStatus.CREATED };
    } catch (error) {
      // ---- ROLLBACK: desfaz tudo em caso de erro ----
      await client.query('ROLLBACK');
      this.logger.error(
        {
          component: 'OrdersService',
          operation: 'order.transaction_rolled_back',
          order_id: orderId,
          duration_ms: Date.now() - start,
          error: error instanceof Error ? error.message : String(error),
        },
        'Transação revertida (ROLLBACK) — nada foi persistido',
      );
      throw error;
    } finally {
      // Sempre devolvemos o client ao pool.
      client.release();
    }
  }

  /** Busca um pedido pelo id. */
  async findOne(id: string): Promise<OrderRow> {
    const result = await this.db.query<OrderRow>(
      `SELECT id, customer_name, amount, status, created_at
       FROM orders WHERE id = $1`,
      [id],
    );
    const order = result.rows[0];
    if (!order) {
      throw new NotFoundException(`Pedido ${id} não encontrado`);
    }
    return order;
  }
}
