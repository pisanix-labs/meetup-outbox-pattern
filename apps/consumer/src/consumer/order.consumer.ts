import {
  Injectable,
  OnApplicationShutdown,
  OnModuleInit,
} from '@nestjs/common';
import { InjectPinoLogger, PinoLogger } from 'nestjs-pino';
import * as amqp from 'amqplib';
import {
  AppConfigService,
  EventEnvelope,
  EXCHANGE_TYPE,
} from '@app/common';
import { ProcessedEventsService } from './processed-events.service';

/**
 * Consumer de eventos OrderCreated.
 *
 * Responsabilidades:
 *   - Conectar ao RabbitMQ e consumir a fila com ACK MANUAL.
 *   - Processar o evento de forma IDEMPOTENTE (processed_events).
 *   - Confirmar (ack) a mensagem SOMENTE após persistir o processamento.
 *   - Reconectar automaticamente quando a conexão cair.
 *
 * Observação: este app NÃO tem servidor HTTP. O consumo é orientado a
 * callbacks do amqplib, aproveitando o event loop do Node.js.
 */
@Injectable()
export class OrderConsumer implements OnModuleInit, OnApplicationShutdown {
  private connection: amqp.ChannelModel | null = null;
  private channel: amqp.Channel | null = null;
  private shuttingDown = false;
  private reconnectTimer: NodeJS.Timeout | null = null;

  constructor(
    private readonly config: AppConfigService,
    private readonly processed: ProcessedEventsService,
    @InjectPinoLogger(OrderConsumer.name)
    private readonly logger: PinoLogger,
  ) {}

  async onModuleInit(): Promise<void> {
    await this.connectWithRetry();
  }

  /**
   * Tenta conectar; em caso de falha, reagenda a tentativa.
   * Não lança erro para não derrubar o processo (reconexão resiliente).
   */
  private async connectWithRetry(): Promise<void> {
    if (this.shuttingDown) {
      return;
    }
    try {
      await this.connect();
    } catch (err) {
      this.logger.warn(
        {
          component: 'OrderConsumer',
          operation: 'consumer.connect_failed',
          error: err instanceof Error ? err.message : String(err),
        },
        'Falha ao conectar no RabbitMQ; tentando novamente em 3s',
      );
      this.scheduleReconnect();
    }
  }

  /** Estabelece conexão, declara topologia e inicia o consumo. */
  private async connect(): Promise<void> {
    const connection = await amqp.connect(this.config.rabbitmqUrl);
    const channel = await connection.createChannel();

    // Mesma topologia durável declarada pelo publisher (idempotente).
    await channel.assertExchange(this.config.exchange, EXCHANGE_TYPE, {
      durable: true,
    });
    await channel.assertQueue(this.config.queue, { durable: true });
    await channel.bindQueue(
      this.config.queue,
      this.config.exchange,
      this.config.routingKey,
    );

    // Prefetch: limita quantas mensagens não confirmadas o broker entrega.
    await channel.prefetch(this.config.prefetch);

    // Reconexão: ao cair a conexão, limpamos estado e reagendamos.
    connection.on('close', () => {
      this.connection = null;
      this.channel = null;
      if (!this.shuttingDown) {
        this.logger.warn(
          { component: 'OrderConsumer', operation: 'consumer.connection_closed' },
          'Conexão com o RabbitMQ encerrada; reconectando',
        );
        this.scheduleReconnect();
      }
    });
    connection.on('error', (err) => {
      this.logger.warn(
        {
          component: 'OrderConsumer',
          operation: 'consumer.connection_error',
          error: err instanceof Error ? err.message : String(err),
        },
        'Erro na conexão com o RabbitMQ',
      );
    });

    this.connection = connection;
    this.channel = channel;

    // Registra o callback de consumo (ACK manual: noAck=false).
    await channel.consume(
      this.config.queue,
      (msg) => {
        if (msg) {
          void this.handleMessage(channel, msg);
        }
      },
      { noAck: false },
    );

    this.logger.info(
      {
        component: 'OrderConsumer',
        operation: 'consumer.ready',
        queue: this.config.queue,
        prefetch: this.config.prefetch,
      },
      'Consumer conectado e aguardando mensagens',
    );
  }

  /**
   * Processa uma mensagem recebida.
   *
   * Ordem didática crucial:
   *   1. Desserializa o envelope.
   *   2. Tenta registrar na tabela de idempotência.
   *   3. Se for duplicado, apenas confirma (ack) e sai.
   *   4. Se for novo, "processa" e confirma (ack) só depois de persistir.
   *   5. Em erro, faz nack com requeue para reprocessar (at-least-once).
   */
  private async handleMessage(
    channel: amqp.Channel,
    msg: amqp.ConsumeMessage,
  ): Promise<void> {
    const start = Date.now();
    let envelope: EventEnvelope;

    try {
      envelope = JSON.parse(msg.content.toString()) as EventEnvelope;
    } catch (err) {
      // Mensagem malformada: descartamos (nack sem requeue) para não travar a fila.
      this.logger.error(
        {
          component: 'OrderConsumer',
          operation: 'consumer.invalid_message',
          error: err instanceof Error ? err.message : String(err),
        },
        'Mensagem inválida descartada',
      );
      channel.nack(msg, false, false);
      return;
    }

    // Usa o messageId (igual ao event_id) como chave de idempotência,
    // com fallback para o campo do envelope.
    const eventId = msg.properties.messageId ?? envelope.event_id;

    this.logger.info(
      {
        component: 'OrderConsumer',
        operation: 'consumer.event_received',
        event_id: eventId,
        event_type: envelope.event_type,
      },
      'Evento recebido',
    );

    try {
      // Idempotência: só processa se for a primeira vez que vemos este event_id.
      const isNew = await this.processed.tryMarkProcessed(eventId);

      if (!isNew) {
        this.logger.warn(
          {
            component: 'OrderConsumer',
            operation: 'consumer.duplicate_detected',
            event_id: eventId,
          },
          'Evento duplicado detectado — ignorando reprocessamento',
        );
        // Mesmo duplicado, confirmamos: a mensagem já cumpriu seu papel.
        channel.ack(msg);
        this.logger.info(
          {
            component: 'OrderConsumer',
            operation: 'consumer.message_acknowledged',
            event_id: eventId,
          },
          'Mensagem confirmada (duplicada)',
        );
        return;
      }

      // --- Efeito de negócio da demonstração ---
      // O registro em processed_events (feito acima) É o efeito observável.
      // Em um sistema real, aqui entraria a lógica de negócio.
      this.logger.info(
        {
          component: 'OrderConsumer',
          operation: 'consumer.event_processed',
          event_id: eventId,
          duration_ms: Date.now() - start,
        },
        'Evento processado com sucesso',
      );

      // ACK manual: confirmamos SOMENTE após persistir o processamento.
      channel.ack(msg);
      this.logger.info(
        {
          component: 'OrderConsumer',
          operation: 'consumer.message_acknowledged',
          event_id: eventId,
        },
        'Mensagem confirmada (ack)',
      );
    } catch (err) {
      // Falha ao processar: devolvemos para a fila (requeue) e tentaremos de novo.
      this.logger.error(
        {
          component: 'OrderConsumer',
          operation: 'consumer.processing_failed',
          event_id: eventId,
          error: err instanceof Error ? err.message : String(err),
        },
        'Falha ao processar evento; devolvendo à fila (nack+requeue)',
      );
      channel.nack(msg, false, true);
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer || this.shuttingDown) {
      return;
    }
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.connectWithRetry();
    }, 3000);
  }

  async onApplicationShutdown(): Promise<void> {
    this.shuttingDown = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    try {
      await this.channel?.close();
      await this.connection?.close();
    } catch {
      // Ignoramos erros no fechamento durante o shutdown.
    }
  }
}
