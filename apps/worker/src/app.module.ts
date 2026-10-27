import { Module } from '@nestjs/common';
import { ScheduleModule } from '@nestjs/schedule';
import {
  AppConfigModule,
  AppLoggerModule,
  DatabaseModule,
  MessagingModule,
} from '@app/common';
import { OutboxProcessor } from './outbox/outbox.processor';
import { OutboxRepository } from './outbox/outbox.repository';

/**
 * Módulo raiz do Outbox Worker.
 * Importa o MessagingModule (publisher) pois o worker é quem publica.
 * Usa ScheduleModule para o polling periódico.
 */
@Module({
  imports: [
    AppLoggerModule.forService('worker'),
    AppConfigModule,
    DatabaseModule,
    MessagingModule,
    ScheduleModule.forRoot(),
  ],
  providers: [OutboxProcessor, OutboxRepository],
})
export class AppModule {}
