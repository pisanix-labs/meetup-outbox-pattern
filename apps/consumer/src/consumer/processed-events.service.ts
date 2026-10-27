import { Injectable } from '@nestjs/common';
import { DatabaseService } from '@app/common';

/**
 * Serviço de idempotência do consumer.
 *
 * Registra os IDs de eventos já processados em processed_events.
 * A PRIMARY KEY em event_id garante unicidade: se o mesmo evento chegar
 * de novo (entrega at-least-once do broker), a inserção é ignorada.
 */
@Injectable()
export class ProcessedEventsService {
  constructor(private readonly db: DatabaseService) {}

  /**
   * Tenta registrar um evento como processado.
   *
   * Usa INSERT ... ON CONFLICT DO NOTHING: operação atômica que evita
   * condição de corrida. Retorna:
   *   - true  -> inserido agora (primeira vez; deve processar o efeito)
   *   - false -> já existia (duplicado; não reprocessar)
   */
  async tryMarkProcessed(eventId: string): Promise<boolean> {
    const result = await this.db.query(
      `INSERT INTO processed_events (event_id, processed_at)
       VALUES ($1, now())
       ON CONFLICT (event_id) DO NOTHING`,
      [eventId],
    );
    // rowCount = 1 quando inseriu; 0 quando o ON CONFLICT ignorou.
    return (result.rowCount ?? 0) > 0;
  }
}
