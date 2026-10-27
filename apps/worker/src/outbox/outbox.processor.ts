import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { SchedulerRegistry } from '@nestjs/schedule';
import { InjectPinoLogger, PinoLogger } from 'nestjs-pino';
import {
  AppConfigService,
  EventEnvelope,
  RabbitmqPublisher,
} from '@app/common';
import { OutboxRepository, ReservedOutboxEvent } from './outbox.repository';

/**
 * Processador da Outbox (o worker).
 *
 * A cada intervalo de polling:
 *   1. Recupera leases expirados (eventos de workers que caíram).
 *   2. Reserva um lote de eventos PENDING (SKIP LOCKED, transação curta).
 *   3. Para cada evento: publica no RabbitMQ e AGUARDA o Publisher Confirm.
 *   4. Em sucesso -> marca PUBLISHED. Em falha -> retry com backoff ou FAILED.
 *
 * Regras importantes:
 *   - Nunca mantemos transação PostgreSQL aberta durante a publicação.
 *   - Evitamos execuções sobrepostas na mesma instância (flag `running`).
 *   - Um evento só é considerado publicado APÓS a confirmação do broker.
 */
@Injectable()
export class OutboxProcessor implements OnModuleInit, OnModuleDestroy {
  // Impede que dois ciclos de polling rodem ao mesmo tempo nesta instância.
  private running = false;
  private static readonly INTERVAL_NAME = 'outbox-polling';

  constructor(
    private readonly repo: OutboxRepository,
    private readonly publisher: RabbitmqPublisher,
    private readonly config: AppConfigService,
    private readonly scheduler: SchedulerRegistry,
    @InjectPinoLogger(OutboxProcessor.name)
    private readonly logger: PinoLogger,
  ) {}

  /**
   * Registra o intervalo de polling dinamicamente, usando o valor
   * configurável OUTBOX_POLLING_INTERVAL_MS (padrão: 2000ms).
   * O @nestjs/schedule não aceita intervalo dinâmico via decorator,
   * então o criamos aqui, no onModuleInit.
   */
  onModuleInit(): void {
    const intervalMs = this.config.pollingIntervalMs;
    const handle = setInterval(() => {
      void this.handlePolling();
    }, intervalMs);
    this.scheduler.addInterval(OutboxProcessor.INTERVAL_NAME, handle);
    this.logger.info(
      {
        component: 'OutboxProcessor',
        operation: 'worker.started',
        polling_interval_ms: intervalMs,
      },
      `Worker iniciado (polling a cada ${intervalMs}ms)`,
    );
  }

  /**
   * Gatilho periódico do polling.
   */
  async handlePolling(): Promise<void> {
    if (this.running) {
      // Ciclo anterior ainda em andamento: pulamos este tick.
      return;
    }
    this.running = true;
    const start = Date.now();

    try {
      // 1) Recupera reservas expiradas antes de reservar novos eventos.
      const recovered = await this.repo.recoverExpiredLeases();
      if (recovered > 0) {
        this.logger.warn(
          {
            component: 'OutboxProcessor',
            operation: 'outbox.leases_recovered',
            recovered,
          },
          'Reservas expiradas recuperadas (eventos voltaram para PENDING)',
        );
      }

      // 2) Reserva um lote de eventos elegíveis.
      const batch = await this.repo.reserveBatch(
        this.config.outboxBatchSize,
        this.config.leaseDurationMs,
      );

      if (batch.length === 0) {
        return;
      }

      this.logger.info(
        {
          component: 'OutboxProcessor',
          operation: 'outbox.polling_started',
          reserved: batch.length,
        },
        `Lote reservado: ${batch.length} evento(s)`,
      );

      // 3) Processa cada evento sequencialmente (simples e previsível).
      for (const event of batch) {
        await this.processEvent(event);
      }
    } catch (error) {
      this.logger.error(
        {
          component: 'OutboxProcessor',
          operation: 'outbox.polling_error',
          error: error instanceof Error ? error.message : String(error),
        },
        'Erro inesperado no ciclo de polling',
      );
    } finally {
      this.logger.debug(
        {
          component: 'OutboxProcessor',
          operation: 'outbox.polling_finished',
          duration_ms: Date.now() - start,
        },
        'Ciclo de polling finalizado',
      );
      this.running = false;
    }
  }

  /**
   * Publica um único evento e trata o resultado.
   * Exposto para facilitar testes unitários.
   */
  async processEvent(event: ReservedOutboxEvent): Promise<void> {
    const start = Date.now();
    const envelope = event.payload as EventEnvelope;
    const eventId = event.id;
    const orderId = event.aggregate_id;

    this.logger.info(
      {
        component: 'OutboxProcessor',
        operation: 'outbox.event_claimed',
        event_id: eventId,
        order_id: orderId,
        attempt: event.attempts,
      },
      'Evento reservado para publicação',
    );

    try {
      this.logger.info(
        {
          component: 'OutboxProcessor',
          operation: 'outbox.publish_started',
          event_id: eventId,
          order_id: orderId,
          attempt: event.attempts,
        },
        'Publicando evento no RabbitMQ',
      );

      // Publica e AGUARDA o Publisher Confirm do broker.
      await this.publisher.publishWithConfirm(envelope);

      this.logger.info(
        {
          component: 'OutboxProcessor',
          operation: 'outbox.publish_confirmed',
          event_id: eventId,
          order_id: orderId,
          attempt: event.attempts,
        },
        'Publicação confirmada pelo broker',
      );

      // --- Ponto de simulação de falha (Cenário 4) ---
      // Habilitado via SIMULATE_CRASH_AFTER_CONFIRM=true apenas para demo.
      // Simula queda do worker APÓS o confirm, mas ANTES de marcar PUBLISHED.
      // Resultado: o lease expira, o evento volta a PENDING e é republicado,
      // chegando novamente ao consumer — onde a idempotência entra em ação.
      if (this.config.simulateCrashAfterConfirm) {
        this.logger.error(
          {
            component: 'OutboxProcessor',
            operation: 'outbox.simulated_crash',
            event_id: eventId,
            order_id: orderId,
          },
          'Falha simulada após o confirm (SIMULATE_CRASH_AFTER_CONFIRM=true)',
        );
        // Encerra o processo abruptamente, sem marcar PUBLISHED.
        process.exit(1);
      }

      // Sucesso: marca como PUBLISHED.
      await this.repo.markPublished(eventId);
      this.logger.info(
        {
          component: 'OutboxProcessor',
          operation: 'outbox.event_published',
          event_id: eventId,
          order_id: orderId,
          attempt: event.attempts,
          status: 'PUBLISHED',
          duration_ms: Date.now() - start,
        },
        'Evento publicado com sucesso',
      );
    } catch (error) {
      await this.handlePublishFailure(event, error, start);
    }
  }

  /**
   * Trata a falha de publicação: decide entre retry (com backoff) ou FAILED.
   */
  private async handlePublishFailure(
    event: ReservedOutboxEvent,
    error: unknown,
    start: number,
  ): Promise<void> {
    const eventId = event.id;
    const orderId = event.aggregate_id;
    const message = error instanceof Error ? error.message : String(error);

    this.logger.error(
      {
        component: 'OutboxProcessor',
        operation: 'outbox.publish_failed',
        event_id: eventId,
        order_id: orderId,
        attempt: event.attempts,
        duration_ms: Date.now() - start,
        error: message,
      },
      'Falha ao publicar evento',
    );

    // event.attempts já reflete a tentativa atual (incrementada na reserva).
    if (event.attempts >= this.config.maxAttempts) {
      await this.repo.markFailed(eventId, message);
      this.logger.error(
        {
          component: 'OutboxProcessor',
          operation: 'outbox.event_failed',
          event_id: eventId,
          order_id: orderId,
          attempt: event.attempts,
          status: 'FAILED',
        },
        'Evento excedeu o máximo de tentativas e foi marcado como FAILED',
      );
      return;
    }

    // Backoff exponencial limitado pelo teto configurado.
    const delay = this.computeBackoff(event.attempts);
    await this.repo.scheduleRetry(eventId, delay, message);
    this.logger.warn(
      {
        component: 'OutboxProcessor',
        operation: 'outbox.retry_scheduled',
        event_id: eventId,
        order_id: orderId,
        attempt: event.attempts,
        next_delay_ms: delay,
      },
      `Retry agendado em ${delay}ms`,
    );
  }

  /**
   * Backoff exponencial: base * 2^(attempt-1), limitado ao teto.
   * Ex. base=2000: 2s, 4s, 8s, 16s, ... até retryBackoffMaxMs.
   */
  private computeBackoff(attempt: number): number {
    const exp = this.config.retryBackoffBaseMs * Math.pow(2, Math.max(0, attempt - 1));
    return Math.min(exp, this.config.retryBackoffMaxMs);
  }

  onModuleDestroy(): void {
    // Remove o intervalo de polling no encerramento gracioso.
    try {
      this.scheduler.deleteInterval(OutboxProcessor.INTERVAL_NAME);
    } catch {
      // Intervalo já removido; ignoramos.
    }
    this.logger.info(
      { component: 'OutboxProcessor', operation: 'worker.shutdown' },
      'Worker encerrando',
    );
  }
}
