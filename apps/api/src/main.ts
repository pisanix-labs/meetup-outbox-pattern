import { NestFactory } from '@nestjs/core';
import { ValidationPipe } from '@nestjs/common';
import { Logger } from 'nestjs-pino';
import { AppModule } from './app.module';
import { AppConfigService } from '@app/common';

/**
 * Bootstrap da API HTTP.
 */
async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule, { bufferLogs: true });

  // Usa o logger estruturado (nestjs-pino) como logger da aplicação.
  app.useLogger(app.get(Logger));

  // Validação global dos DTOs com class-validator.
  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true, // remove campos não declarados no DTO
      forbidNonWhitelisted: true, // rejeita campos extras
      transform: true, // converte tipos (ex.: string -> number)
    }),
  );

  // Habilita hooks de shutdown (SIGTERM/SIGINT) para encerramento gracioso.
  app.enableShutdownHooks();

  const config = app.get(AppConfigService);
  await app.listen(config.apiPort, '0.0.0.0');
}

void bootstrap();
