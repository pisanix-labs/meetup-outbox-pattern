import { Global, Module } from '@nestjs/common';
import { DatabaseService } from './database.service';

/**
 * Módulo global de acesso ao banco.
 * Exporta o DatabaseService (pool `pg`) para toda a aplicação.
 */
@Global()
@Module({
  providers: [DatabaseService],
  exports: [DatabaseService],
})
export class DatabaseModule {}
