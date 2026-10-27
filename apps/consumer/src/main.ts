import { NestFactory } from '@nestjs/core';
import { Logger } from 'nestjs-pino';
import { AppModule } from './app.module';

/**
 * Bootstrap do Consumer.
 *
 * Aplicação SEM servidor HTTP. O consumo é orientado a callbacks do amqplib.
 * Ligamos os hooks de shutdown para fechar canal/conexão de forma graciosa.
 */
async function bootstrap(): Promise<void> {
  const app = await NestFactory.createApplicationContext(AppModule, {
    bufferLogs: true,
  });

  app.useLogger(app.get(Logger));
  app.enableShutdownHooks();
}

void bootstrap();
