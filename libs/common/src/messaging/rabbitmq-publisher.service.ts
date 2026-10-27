import {
  Injectable,
  OnApplicationShutdown,
  OnModuleInit,
} from '@nestjs/common';
import { InjectPinoLogger, PinoLogger } from 'nestjs-pino';
import * as amqp from 'amqplib';
import { AppConfigService } from '../config/app-config.service';
import { EXCHANGE_TYPE } from './rabbitmq.constants';
import { EventEnvelope } from '../domain/outbox.types';

/**
 * Publisher do RabbitMQ usado pelo Outbox Worker.
 *
 * Pontos importantes para a demonstração:
 *  - Usa ConfirmChannel: só consideramos um evento publicado APÓS o broker
 *    confirmar (Publisher Confirms). Isso evita perda silenciosa de mensagens.
 *  - Declara exchange e fila duráveis e publica mensagens persistentes.
 *  - Trata mensagens não roteadas (evento `return`).
 *  - Reconecta automaticamente quando o RabbitMQ volta a ficar disponível.
 *
 * A API NÃO usa este publisher: quem publica é exclusivamente o worker.
 */
@Injectable()
export class RabbitmqPublisher implements OnModuleInit, OnApplicationShutdown {
  private connection: amqp.ChannelModel | null = null;
  private channel: amqp.ConfirmChannel | null = null;
  private connecting = false;
  private shuttingDown = false;
  // Flag ligada quando uma mensagem publicada não pôde ser roteada.
  private lastReturnWasUnroutable = false;

  constructor(
    private readonly config: AppConfigService,
    @InjectPinoLogger(RabbitmqPublisher.name)
    private readonly logger: PinoLogger,
  ) {}

  async onModuleInit(): Promise<void> {
    // Tenta conectar no startup, mas não derruba a aplicação se falhar:
    // o worker continua rodando e tentará reconectar nos próximos ciclos.
    await this.ensureConnection().catch((err) => {
      this.logger.warn(
        { component: 'RabbitmqPublisher', operation: 'rabbitmq.connect_failed', error: this.toError(err) },
        'Falha ao conectar no RabbitMQ no startup; tentaremos novamente no polling',
      );
    });
  }

  /** Indica se há um canal pronto para publicar. */
  isReady(): boolean {
    return this.channel !== null && this.connection !== null;
  }

  /**
   * Garante que exista uma conexão e um ConfirmChannel válidos.
   * Idempotente: se já estiver conectado, retorna imediatamente.
   */
  async ensureConnection(): Promise<void> {
    if (this.channel && this.connection) {
      return;
    }
    if (this.connecting) {
      // Evita conexões concorrentes durante o startup/reconexão.
      return;
    }
    this.connecting = true;
    try {
      const connection = await amqp.connect(this.config.rabbitmqUrl);
      const channel = await connection.createConfirmChannel();

      // Topologia durável: sobrevive a reinícios do broker.
      await channel.assertExchange(this.config.exchange, EXCHANGE_TYPE, {
        durable: true,
      });
      await channel.assertQueue(this.config.queue, { durable: true });
      await channel.bindQueue(
        this.config.queue,
        this.config.exchange,
        this.config.routingKey,
      );

      // Detecta mensagens que não puderam ser roteadas (publicadas com mandatory).
      channel.on('return', (msg) => {
        this.lastReturnWasUnroutable = true;
        this.logger.error(
          {
            component: 'RabbitmqPublisher',
            operation: 'rabbitmq.message_returned',
            event_id: msg.properties.messageId,
          },
          'Mensagem retornada pelo broker (não roteável)',
        );
      });

      // Limpa o estado quando a conexão cai, para forçar reconexão.
      connection.on('error', (err) => {
        this.logger.warn(
          { component: 'RabbitmqPublisher', operation: 'rabbitmq.connection_error', error: this.toError(err) },
          'Erro na conexão com o RabbitMQ',
        );
      });
      connection.on('close', () => {
        this.connection = null;
        this.channel = null;
        if (!this.shuttingDown) {
          this.logger.warn(
            { component: 'RabbitmqPublisher', operation: 'rabbitmq.connection_closed' },
            'Conexão com o RabbitMQ encerrada; reconectaremos sob demanda',
          );
        }
      });

      this.connection = connection;
      this.channel = channel;
      this.logger.info(
        { component: 'RabbitmqPublisher', operation: 'rabbitmq.connected' },
        'Conectado ao RabbitMQ (ConfirmChannel pronto)',
      );
    } finally {
      this.connecting = false;
    }
  }

  /**
   * Publica um evento e AGUARDA o Publisher Confirm do broker.
   *
   * Resolve apenas quando o RabbitMQ confirma (ack). Rejeita se:
   *  - o broker enviar nack;
   *  - a mensagem for retornada como não roteável;
   *  - não houver canal disponível.
   *
   * @param envelope Envelope do evento (serializado como JSON no corpo).
   * @returns Promise que resolve após a confirmação do broker.
   */
  async publishWithConfirm(envelope: EventEnvelope): Promise<void> {
    await this.ensureConnection();
    if (!this.channel) {
      throw new Error('Canal do RabbitMQ indisponível');
    }

    this.lastReturnWasUnroutable = false;
    const channel = this.channel;
    const body = Buffer.from(JSON.stringify(envelope));

    await new Promise<void>((resolve, reject) => {
      channel.publish(
        this.config.exchange,
        this.config.routingKey,
        body,
        {
          // Mensagem persistente: sobrevive a reinício do broker.
          persistent: true,
          contentType: 'application/json',
          // messageId imutável = chave de idempotência no consumer.
          messageId: envelope.event_id,
          type: envelope.event_type,
          timestamp: Date.now(),
          // mandatory: dispara `return` se não houver fila roteável.
          mandatory: true,
        },
        (err) => {
          // Callback do Publisher Confirm: chamado no ack/nack do broker.
          if (err) {
            return reject(err);
          }
          if (this.lastReturnWasUnroutable) {
            return reject(new Error('Mensagem não roteável (sem fila de destino)'));
          }
          resolve();
        },
      );
    });
  }

  async onApplicationShutdown(): Promise<void> {
    this.shuttingDown = true;
    try {
      await this.channel?.close();
      await this.connection?.close();
    } catch {
      // Ignoramos erros no fechamento durante o shutdown.
    }
  }

  private toError(err: unknown): string {
    return err instanceof Error ? err.message : String(err);
  }
}
