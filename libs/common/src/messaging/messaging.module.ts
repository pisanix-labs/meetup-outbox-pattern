import { Module } from '@nestjs/common';
import { RabbitmqPublisher } from './rabbitmq-publisher.service';

/**
 * Módulo de mensageria para quem PUBLICA (o worker).
 * O consumer gerencia sua própria conexão de consumo em seu app.
 */
@Module({
  providers: [RabbitmqPublisher],
  exports: [RabbitmqPublisher],
})
export class MessagingModule {}
