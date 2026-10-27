import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

/**
 * Serviço central de configuração.
 *
 * Centraliza a leitura das variáveis de ambiente em um único lugar,
 * convertendo e validando tipos. Assim o restante do código consome
 * valores já tipados, sem espalhar `process.env` pela aplicação.
 *
 * Observação didática: usamos o ConfigService nativo do NestJS por baixo,
 * apenas expondo getters explícitos e fáceis de ler.
 */
@Injectable()
export class AppConfigService {
  constructor(private readonly config: ConfigService) {}

  // ---------- PostgreSQL ----------

  get databaseUrl(): string {
    // Se DATABASE_URL existir, tem prioridade (útil em alguns ambientes).
    const url = this.config.get<string>('DATABASE_URL');
    if (url) {
      return url;
    }
    const host = this.config.get<string>('POSTGRES_HOST', 'localhost');
    const port = this.config.get<string>('POSTGRES_PORT', '5432');
    const user = this.config.get<string>('POSTGRES_USER', 'postgres');
    const password = this.config.get<string>('POSTGRES_PASSWORD', 'postgres');
    const db = this.config.get<string>('POSTGRES_DB', 'outbox');
    return `postgres://${user}:${password}@${host}:${port}/${db}`;
  }

  // ---------- RabbitMQ ----------

  get rabbitmqUrl(): string {
    return this.config.get<string>('RABBITMQ_URL', 'amqp://guest:guest@localhost:5672');
  }

  get exchange(): string {
    return this.config.get<string>('RABBITMQ_EXCHANGE', 'orders.exchange');
  }

  get queue(): string {
    return this.config.get<string>('RABBITMQ_QUEUE', 'orders.created.queue');
  }

  get routingKey(): string {
    return this.config.get<string>('RABBITMQ_ROUTING_KEY', 'order.created');
  }

  // ---------- Worker ----------

  /** Intervalo de polling do worker, em milissegundos. */
  get pollingIntervalMs(): number {
    return this.numeric('OUTBOX_POLLING_INTERVAL_MS', 2000);
  }

  /** Quantos eventos o worker reserva por ciclo. */
  get outboxBatchSize(): number {
    return this.numeric('OUTBOX_BATCH_SIZE', 10);
  }

  /** Duração da reserva (lease) de um evento, em milissegundos. */
  get leaseDurationMs(): number {
    return this.numeric('OUTBOX_LEASE_DURATION_MS', 30000);
  }

  /** Número máximo de tentativas antes de marcar o evento como FAILED. */
  get maxAttempts(): number {
    return this.numeric('OUTBOX_MAX_ATTEMPTS', 5);
  }

  /** Backoff base em milissegundos (crescimento exponencial). */
  get retryBackoffBaseMs(): number {
    return this.numeric('OUTBOX_RETRY_BACKOFF_BASE_MS', 2000);
  }

  /** Teto do backoff em milissegundos. */
  get retryBackoffMaxMs(): number {
    return this.numeric('OUTBOX_RETRY_BACKOFF_MAX_MS', 60000);
  }

  // ---------- Consumer ----------

  get prefetch(): number {
    return this.numeric('CONSUMER_PREFETCH', 1);
  }

  // ---------- Demonstração de falhas ----------

  /**
   * Cenário 3: simula erro na API entre persistir o pedido e inserir o evento,
   * forçando o ROLLBACK. Desabilitado por padrão.
   */
  get simulateApiFailure(): boolean {
    return this.boolean('SIMULATE_API_FAILURE', false);
  }

  /**
   * Cenário 4: simula falha no worker após o RabbitMQ confirmar a publicação,
   * mas antes de atualizar o status para PUBLISHED. Desabilitado por padrão.
   */
  get simulateCrashAfterConfirm(): boolean {
    return this.boolean('SIMULATE_CRASH_AFTER_CONFIRM', false);
  }

  // ---------- Infra ----------

  get apiPort(): number {
    return this.numeric('API_PORT', 8080);
  }

  get logLevel(): string {
    return this.config.get<string>('LOG_LEVEL', 'info');
  }

  get prettyLogs(): boolean {
    return this.boolean('LOG_PRETTY', false);
  }

  private numeric(key: string, fallback: number): number {
    const raw = this.config.get<string>(key);
    if (raw === undefined || raw === '') {
      return fallback;
    }
    const parsed = Number(raw);
    return Number.isFinite(parsed) ? parsed : fallback;
  }

  private boolean(key: string, fallback: boolean): boolean {
    const raw = this.config.get<string>(key);
    if (raw === undefined || raw === '') {
      return fallback;
    }
    return raw.toLowerCase() === 'true' || raw === '1';
  }
}
