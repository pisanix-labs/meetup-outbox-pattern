import { getLoggerToken, PinoLogger } from 'nestjs-pino';
import { OutboxProcessor } from './outbox.processor';
import { OutboxRepository, ReservedOutboxEvent } from './outbox.repository';
import {
  AppConfigService,
  EventEnvelope,
  RabbitmqPublisher,
} from '@app/common';
import { SchedulerRegistry } from '@nestjs/schedule';

/**
 * Testes unitários do OutboxProcessor.
 *
 * Garantias cobertas:
 *  - só marca PUBLISHED APÓS o Publisher Confirm (publishWithConfirm resolve);
 *  - em falha de publicação, agenda RETRY com backoff (abaixo do máximo);
 *  - ao exceder o máximo de tentativas, marca FAILED.
 */
describe('OutboxProcessor (unidade)', () => {
  let processor: OutboxProcessor;
  let repo: jest.Mocked<Pick<OutboxRepository, 'markPublished' | 'scheduleRetry' | 'markFailed'>>;
  let publisher: { publishWithConfirm: jest.Mock };
  let config: Partial<AppConfigService>;

  const makeEvent = (attempts: number): ReservedOutboxEvent => {
    const envelope: EventEnvelope = {
      event_id: 'evt-1',
      event_type: 'OrderCreated',
      occurred_at: new Date().toISOString(),
      data: { order_id: 'ord-1', customer_name: 'Joao', amount: 10 },
    };
    return {
      id: 'evt-1',
      aggregate_id: 'ord-1',
      event_type: 'OrderCreated',
      payload: envelope,
      attempts,
    };
  };

  beforeEach(() => {
    repo = {
      markPublished: jest.fn().mockResolvedValue(undefined),
      scheduleRetry: jest.fn().mockResolvedValue(undefined),
      markFailed: jest.fn().mockResolvedValue(undefined),
    };
    publisher = { publishWithConfirm: jest.fn() };
    config = {
      maxAttempts: 5,
      retryBackoffBaseMs: 2000,
      retryBackoffMaxMs: 60000,
      simulateCrashAfterConfirm: false,
    };

    const logger = {
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
      debug: jest.fn(),
    } as unknown as PinoLogger;

    processor = new OutboxProcessor(
      repo as unknown as OutboxRepository,
      publisher as unknown as RabbitmqPublisher,
      config as AppConfigService,
      {} as SchedulerRegistry,
      logger,
    );
  });

  it('marca PUBLISHED somente após a confirmação do broker', async () => {
    publisher.publishWithConfirm.mockResolvedValue(undefined);

    await processor.processEvent(makeEvent(1));

    expect(publisher.publishWithConfirm).toHaveBeenCalledTimes(1);
    expect(repo.markPublished).toHaveBeenCalledWith('evt-1');
    expect(repo.scheduleRetry).not.toHaveBeenCalled();
    expect(repo.markFailed).not.toHaveBeenCalled();
  });

  it('NÃO marca PUBLISHED se a confirmação falhar; agenda retry com backoff', async () => {
    publisher.publishWithConfirm.mockRejectedValue(new Error('broker down'));

    // attempts=1 -> ainda abaixo do máximo (5): deve reagendar.
    await processor.processEvent(makeEvent(1));

    expect(repo.markPublished).not.toHaveBeenCalled();
    expect(repo.markFailed).not.toHaveBeenCalled();
    expect(repo.scheduleRetry).toHaveBeenCalledTimes(1);

    // Backoff da 1a tentativa = base * 2^0 = 2000ms.
    const [eventId, delay, lastError] = repo.scheduleRetry.mock.calls[0];
    expect(eventId).toBe('evt-1');
    expect(delay).toBe(2000);
    expect(lastError).toContain('broker down');
  });

  it('aplica backoff exponencial crescente conforme as tentativas', async () => {
    publisher.publishWithConfirm.mockRejectedValue(new Error('x'));

    await processor.processEvent(makeEvent(2)); // 2000 * 2^1 = 4000
    await processor.processEvent(makeEvent(3)); // 2000 * 2^2 = 8000

    expect(repo.scheduleRetry.mock.calls[0][1]).toBe(4000);
    expect(repo.scheduleRetry.mock.calls[1][1]).toBe(8000);
  });

  it('respeita o teto do backoff (retryBackoffMaxMs)', async () => {
    publisher.publishWithConfirm.mockRejectedValue(new Error('x'));

    // attempts=4 -> 2000 * 2^3 = 16000 (< 60000, não limitado ainda)
    // forçamos um teto baixo para provar o clamp.
    (config as { retryBackoffMaxMs: number }).retryBackoffMaxMs = 5000;
    await processor.processEvent(makeEvent(4));

    expect(repo.scheduleRetry.mock.calls[0][1]).toBe(5000);
  });

  it('marca FAILED quando excede o número máximo de tentativas', async () => {
    publisher.publishWithConfirm.mockRejectedValue(new Error('fatal'));

    // attempts=5 == maxAttempts -> deve ir para FAILED.
    await processor.processEvent(makeEvent(5));

    expect(repo.markFailed).toHaveBeenCalledTimes(1);
    expect(repo.markFailed.mock.calls[0][0]).toBe('evt-1');
    expect(repo.scheduleRetry).not.toHaveBeenCalled();
    expect(repo.markPublished).not.toHaveBeenCalled();
  });
});
