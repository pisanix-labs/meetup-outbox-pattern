import { Module } from '@nestjs/common';
import {
  AppConfigModule,
  AppLoggerModule,
  DatabaseModule,
} from '@app/common';
import { OrderConsumer } from './consumer/order.consumer';
import { ProcessedEventsService } from './consumer/processed-events.service';

/**
 * Módulo raiz do Consumer.
 * Não importa MessagingModule: o consumer gerencia sua própria conexão
 * de consumo (com ACK manual e reconexão) dentro do OrderConsumer.
 */
@Module({
  imports: [
    AppLoggerModule.forService('consumer'),
    AppConfigModule,
    DatabaseModule,
  ],
  providers: [OrderConsumer, ProcessedEventsService],
})
export class AppModule {}
