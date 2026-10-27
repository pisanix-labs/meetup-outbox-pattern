import {
  Injectable,
  OnApplicationShutdown,
  OnModuleInit,
} from '@nestjs/common';
import { Pool, PoolClient, QueryResult, QueryResultRow } from 'pg';
import { AppConfigService } from '../config/app-config.service';

/**
 * Serviço que encapsula o pool de conexões do PostgreSQL (biblioteca `pg`).
 *
 * Expomos o pool diretamente para que o controle de transações fique
 * explícito no OrdersService e no worker — este é um dos pontos centrais
 * da demonstração do padrão. Não há ORM escondendo o SQL.
 */
@Injectable()
export class DatabaseService implements OnModuleInit, OnApplicationShutdown {
  private readonly pool: Pool;

  constructor(private readonly config: AppConfigService) {
    this.pool = new Pool({
      connectionString: this.config.databaseUrl,
      // Mantemos o pool pequeno: é um projeto didático.
      max: 10,
      connectionTimeoutMillis: 5000,
      idleTimeoutMillis: 30000,
    });
  }

  async onModuleInit(): Promise<void> {
    // Valida a conectividade no startup (falha cedo se o banco estiver fora).
    await this.pool.query('SELECT 1');
  }

  /**
   * Executa uma query simples usando o pool (sem transação dedicada).
   * Útil para leituras e operações atômicas de comando único.
   */
  async query<T extends QueryResultRow = QueryResultRow>(
    text: string,
    params?: unknown[],
  ): Promise<QueryResult<T>> {
    return this.pool.query<T>(text, params);
  }

  /**
   * Obtém um client dedicado do pool.
   *
   * O chamador é responsável por chamar `client.release()` em um bloco
   * `finally`. Isso é intencional: queremos que o BEGIN/COMMIT/ROLLBACK
   * fique visível no código do serviço.
   */
  async getClient(): Promise<PoolClient> {
    return this.pool.connect();
  }

  /** Expõe o pool para verificações de saúde. */
  get rawPool(): Pool {
    return this.pool;
  }

  async onApplicationShutdown(): Promise<void> {
    // Libera todas as conexões no encerramento gracioso (SIGTERM/SIGINT).
    await this.pool.end();
  }
}
