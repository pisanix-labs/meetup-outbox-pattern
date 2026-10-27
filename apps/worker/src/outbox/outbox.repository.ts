import { Injectable } from '@nestjs/common';
import { DatabaseService } from '@app/common';

/** Linha da outbox reservada para processamento. */
export interface ReservedOutboxEvent {
  id: string;
  aggregate_id: string;
  event_type: string;
  payload: unknown; // JSONB já desserializado pela lib `pg`
  attempts: number;
}

/**
 * Repositório de acesso à tabela outbox_events para o worker.
 *
 * Concentra o SQL do padrão: reserva com FOR UPDATE SKIP LOCKED,
 * marcação de PUBLISHED, agendamento de retry e recuperação de leases
 * expirados. Transações são CURTAS e nunca envolvem o RabbitMQ.
 */
@Injectable()
export class OutboxRepository {
  constructor(private readonly db: DatabaseService) {}

  /**
   * Reserva até `batchSize` eventos elegíveis para publicação.
   *
   * Em UMA transação curta:
   *   1. SELECT dos eventos PENDING prontos (next_attempt_at <= now),
   *      com FOR UPDATE SKIP LOCKED para não brigar com outros workers.
   *   2. UPDATE marcando-os como PROCESSING, incrementando attempts e
   *      definindo locked_until = now + lease.
   *
   * O SKIP LOCKED permite escalar para múltiplos workers sem contenção:
   * cada um pega um conjunto disjunto de linhas.
   */
  async reserveBatch(
    batchSize: number,
    leaseMs: number,
  ): Promise<ReservedOutboxEvent[]> {
    const client = await this.db.getClient();
    try {
      await client.query('BEGIN');

      const selected = await client.query<{ id: string }>(
        `SELECT id
           FROM outbox_events
          WHERE status = 'PENDING'
            AND next_attempt_at <= now()
          ORDER BY created_at
          FOR UPDATE SKIP LOCKED
          LIMIT $1`,
        [batchSize],
      );

      const ids = selected.rows.map((r) => r.id);
      if (ids.length === 0) {
        await client.query('COMMIT');
        return [];
      }

      const reserved = await client.query<ReservedOutboxEvent>(
        `UPDATE outbox_events
            SET status = 'PROCESSING',
                attempts = attempts + 1,
                locked_until = now() + ($2::int * interval '1 millisecond')
          WHERE id = ANY($1::uuid[])
        RETURNING id, aggregate_id, event_type, payload, attempts`,
        [ids, leaseMs],
      );

      await client.query('COMMIT');
      return reserved.rows;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Marca um evento como PUBLISHED após a confirmação do broker.
   * Transação curta de comando único.
   */
  async markPublished(eventId: string): Promise<void> {
    await this.db.query(
      `UPDATE outbox_events
          SET status = 'PUBLISHED',
              published_at = now(),
              locked_until = NULL,
              last_error = NULL
        WHERE id = $1`,
      [eventId],
    );
  }

  /**
   * Reagenda um evento para nova tentativa (volta a PENDING).
   * Registra o último erro e o próximo horário elegível (backoff).
   */
  async scheduleRetry(
    eventId: string,
    nextDelayMs: number,
    lastError: string,
  ): Promise<void> {
    await this.db.query(
      `UPDATE outbox_events
          SET status = 'PENDING',
              locked_until = NULL,
              last_error = $2,
              next_attempt_at = now() + ($3::int * interval '1 millisecond')
        WHERE id = $1`,
      [eventId, lastError, nextDelayMs],
    );
  }

  /**
   * Marca um evento como FAILED após exceder o número máximo de tentativas.
   */
  async markFailed(eventId: string, lastError: string): Promise<void> {
    await this.db.query(
      `UPDATE outbox_events
          SET status = 'FAILED',
              locked_until = NULL,
              last_error = $2
        WHERE id = $1`,
      [eventId, lastError],
    );
  }

  /**
   * Recupera reservas expiradas (lease recovery).
   *
   * Eventos em PROCESSING cujo locked_until já passou pertencem a workers
   * que caíram no meio do processamento. Voltam para PENDING e ficam
   * elegíveis imediatamente. Retorna quantos foram recuperados.
   */
  async recoverExpiredLeases(): Promise<number> {
    const result = await this.db.query(
      `UPDATE outbox_events
          SET status = 'PENDING',
              locked_until = NULL,
              next_attempt_at = now()
        WHERE status = 'PROCESSING'
          AND locked_until IS NOT NULL
          AND locked_until <= now()`,
    );
    return result.rowCount ?? 0;
  }
}
