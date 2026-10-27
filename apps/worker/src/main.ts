import { NestFactory } from '@nestjs/core';
import { Logger } from 'nestjs-pino';
import { AppModule } from './app.module';

/**
 * Bootstrap do Outbox Worker.
 *
 * Aplicação SEM servidor HTTP: criamos apenas o contexto da aplicação
 * (createApplicationContext), ligamos os hooks de shutdown e deixamos o
 * polling agendado rodando até receber SIGTERM/SIGINT.
 */
async function bootstrap(): Promise<void> {
  const app = await NestFactory.createApplicationContext(AppModule, {
    bufferLogs: true,
  });

  app.useLogger(app.get(Logger));

  // Encerramento gracioso: libera pool do banco, intervalo e conexão do broker.
  app.enableShutdownHooks();
}

void bootstrap();
