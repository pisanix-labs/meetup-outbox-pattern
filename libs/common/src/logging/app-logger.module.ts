import { DynamicModule, Module } from '@nestjs/common';
import { LoggerModule } from 'nestjs-pino';

/**
 * Módulo de logging estruturado baseado em nestjs-pino.
 *
 * Cada aplicação chama `buildLoggerModule('api' | 'worker' | 'consumer')`,
 * fixando o campo `service` em todos os logs. A saída é JSON por padrão
 * (ideal para `docker compose logs`), com opção de formatação legível em dev
 * via LOG_PRETTY=true.
 *
 * Os logs são um requisito central do projeto: eles contam a história de cada
 * evento, da criação do pedido até o processamento no consumer.
 */
@Module({})
export class AppLoggerModule {
  static forService(service: 'api' | 'worker' | 'consumer'): DynamicModule {
    const level = process.env.LOG_LEVEL ?? 'info';
    const pretty =
      (process.env.LOG_PRETTY ?? 'false').toLowerCase() === 'true';

    return {
      module: AppLoggerModule,
      imports: [
        LoggerModule.forRoot({
          pinoHttp: {
            level,
            // Campo fixo presente em todos os logs do serviço.
            base: { service },
            // Formatação legível apenas em desenvolvimento.
            transport: pretty
              ? {
                  target: 'pino-pretty',
                  options: {
                    singleLine: true,
                    colorize: true,
                    translateTime: 'SYS:HH:MM:ss.l',
                  },
                }
              : undefined,
            // Em produção/demonstração o padrão é JSON puro no stdout.
            autoLogging: service === 'api',
          },
        }),
      ],
      exports: [LoggerModule],
    };
  }
}
