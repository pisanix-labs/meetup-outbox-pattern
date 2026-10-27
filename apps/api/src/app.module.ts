import { Module } from '@nestjs/common';
import {
  AppConfigModule,
  AppLoggerModule,
  DatabaseModule,
} from '@app/common';
import { OrdersModule } from './orders/orders.module';
import { HealthController } from './health/health.controller';

/**
 * Módulo raiz da API.
 * Importa configuração, logging e banco (globais) e o módulo de pedidos.
 * Note que NÃO há MessagingModule aqui: a API não publica no RabbitMQ.
 */
@Module({
  imports: [
    AppLoggerModule.forService('api'),
    AppConfigModule,
    DatabaseModule,
    OrdersModule,
  ],
  controllers: [HealthController],
})
export class AppModule {}
