import { Global, Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { AppConfigService } from './app-config.service';

/**
 * Módulo global de configuração.
 *
 * Marcado como @Global para que o AppConfigService fique disponível
 * em todas as três aplicações sem precisar reimportar o módulo.
 */
@Global()
@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      // Lê o .env na raiz quando rodando fora do Docker.
      envFilePath: ['.env'],
    }),
  ],
  providers: [AppConfigService],
  exports: [AppConfigService],
})
export class AppConfigModule {}
