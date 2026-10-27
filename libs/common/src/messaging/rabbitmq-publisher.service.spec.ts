import { getLoggerToken, PinoLogger } from 'nestjs-pino';
import { RabbitmqPublisher } from './rabbitmq-publisher.service';
import { AppConfigService } from '../config/app-config.service';
import { EventEnvelope } from '../domain/outbox.types';
import * as amqp from 'amqplib';

jest.mock('amqplib');

/**
 * Testes unitários do RabbitmqPublisher.
 *
 * Garantia central: publishWithConfirm só RESOLVE após o broker confirmar
 * (callback do Publisher Confirm sem erro) e REJEITA em nack/erro. Também
 * verifica propriedades essenciais: persistent, messageId = event_id, mandatory.
 */
describe('RabbitmqPublisher (unidade)', () => {
  let publisher: RabbitmqPublisher;
  let channel: {
    assertExchange: jest.Mock;
    assertQueue: jest.Mock;
    bindQueue: jest.Mock;
    on: jest.Mock;
    publish: jest.Mock;
    returnHandler?: (msg: unknown) => void;
  };
  let connection: { createConfirmChannel: jest.Mock; on: jest.Mock };

  const config: Partial<AppConfigService> = {
    rabbitmqUrl: 'amqp://localhost',
    exchange: 'orders.exchange',
    queue: 'orders.created.queue',
    routingKey: 'order.created',
  };

  const envelope: EventEnvelope = {
    event_id: 'evt-123',
    event_type: 'OrderCreated',
    occurred_at: new Date().toISOString(),
    data: { order_id: 'ord-1', customer_name: 'Joao', amount: 10 },
  };

  beforeEach(() => {
    channel = {
      assertExchange: jest.fn().mockResolvedValue(undefined),
      assertQueue: jest.fn().mockResolvedValue(undefined),
      bindQueue: jest.fn().mockResolvedValue(undefined),
      on: jest.fn((event: string, cb: (msg: unknown) => void) => {
        if (event === 'return') {
          channel.returnHandler = cb;
        }
      }),
      publish: jest.fn(),
    };
    connection = {
      createConfirmChannel: jest.fn().mockResolvedValue(channel),
      on: jest.fn(),
    };
    (amqp.connect as jest.Mock).mockResolvedValue(connection);

    const logger = {
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
      debug: jest.fn(),
    } as unknown as PinoLogger;

    publisher = new RabbitmqPublisher(config as AppConfigService, logger);
  });

  it('resolve quando o broker confirma (ack) e envia as propriedades corretas', async () => {
    // Simula o Publisher Confirm chamando o callback sem erro.
    channel.publish.mockImplementation(
      (_ex, _rk, _body, _opts, cb: (err: Error | null) => void) => {
        cb(null);
        return true;
      },
    );

    await expect(publisher.publishWithConfirm(envelope)).resolves.toBeUndefined();

    const [exchange, routingKey, body, opts] = channel.publish.mock.calls[0];
    expect(exchange).toBe('orders.exchange');
    expect(routingKey).toBe('order.created');
    expect(opts.persistent).toBe(true);
    expect(opts.mandatory).toBe(true);
    // messageId imutável = event_id (chave de idempotência).
    expect(opts.messageId).toBe('evt-123');
    // Corpo é o envelope serializado.
    expect(JSON.parse((body as Buffer).toString()).event_id).toBe('evt-123');
  });

  it('rejeita quando o broker responde com nack (erro no confirm)', async () => {
    channel.publish.mockImplementation(
      (_ex, _rk, _body, _opts, cb: (err: Error | null) => void) => {
        cb(new Error('nack'));
        return true;
      },
    );

    await expect(publisher.publishWithConfirm(envelope)).rejects.toThrow('nack');
  });

  it('rejeita quando a mensagem é retornada como não roteável', async () => {
    channel.publish.mockImplementation(
      (_ex, _rk, _body, _opts, cb: (err: Error | null) => void) => {
        // Antes do confirm, o broker devolve a mensagem (sem fila roteável).
        channel.returnHandler?.({ properties: { messageId: 'evt-123' } });
        cb(null);
        return true;
      },
    );

    await expect(publisher.publishWithConfirm(envelope)).rejects.toThrow(
      /não roteável/,
    );
  });
});
