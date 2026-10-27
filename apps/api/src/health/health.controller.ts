import { Controller, Get } from '@nestjs/common';
import { DatabaseService } from '@app/common';

/**
 * Health check da API.
 * GET /health -> reporta a saúde da API e a conectividade com o PostgreSQL.
 */
@Controller('health')
export class HealthController {
  constructor(private readonly db: DatabaseService) {}

  @Get()
  async check(): Promise<{ status: string; postgres: string }> {
    try {
      await this.db.query('SELECT 1');
      return { status: 'ok', postgres: 'up' };
    } catch {
      // Se o banco estiver fora, reportamos degradação (mas sem derrubar a API).
      return { status: 'degraded', postgres: 'down' };
    }
  }
}
