import { Test } from '@nestjs/testing';
import { getLoggerToken, PinoLogger } from 'nestjs-pino';
import { OrdersService } from './orders.service';
import { AppConfigService, DatabaseService } from '@app/common';
import { CreateOrderDto } from './dto/create-order.dto';

/**
 * Testes unitários do OrdersService.
 *
 * Foco nas GARANTIAS do Outbox na API:
 *  - pedido e evento são gravados na MESMA transação (BEGIN ... COMMIT);
 *  - em caso de erro, há ROLLBACK e nada é persistido.
 *
 * Usamos um client `pg` mockado para inspecionar a sequência exata de queries.
 */
describe('OrdersService (unidade)', () => {
  let service: OrdersService;
  let queries: string[];
  let client: { query: jest.Mock; release: jest.Mock };
  let config: { simulateApiFailure: boolean };

  beforeEach(async () => {
    queries = [];
    client = {
      query: jest.fn((sql: string) => {
        // Registra apenas o verbo/início do SQL para asserções legíveis.
        queries.push(sql.trim().split('\n')[0].trim());
        return Promise.resolve({ rows: [], rowCount: 1 });
      }),
      release: jest.fn(),
    };
    config = { simulateApiFailure: false };

    const db: Partial<DatabaseService> = {
      getClient: jest.fn().mockResolvedValue(client),
    };

    // Logger pino mockado (silencia a saída nos testes).
    const logger = {
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
      debug: jest.fn(),
    } as unknown as PinoLogger;

    const moduleRef = await Test.createTestingModule({
      providers: [
        OrdersService,
        { provide: DatabaseService, useValue: db },
        { provide: AppConfigService, useValue: config },
        { provide: getLoggerToken(OrdersService.name), useValue: logger },
      ],
    }).compile();

    service = moduleRef.get(OrdersService);
  });

  const dto: CreateOrderDto = { customer_name: 'Joao', amount: 150.0 };

  it('grava pedido e evento na MESMA transação e faz COMMIT', async () => {
    const result = await service.createOrder(dto);

    // Deve ter retornado id + status CREATED.
    expect(result.status).toBe('CREATED');
    expect(result.id).toBeDefined();

    // Sequência esperada: BEGIN -> INSERT orders -> INSERT outbox -> COMMIT.
    expect(queries[0]).toBe('BEGIN');
    expect(queries[1]).toContain('INSERT INTO orders');
    expect(queries[2]).toContain('INSERT INTO outbox_events');
    expect(queries[3]).toBe('COMMIT');

    // Não pode haver ROLLBACK no caminho feliz.
    expect(queries).not.toContain('ROLLBACK');
    // O client sempre é devolvido ao pool.
    expect(client.release).toHaveBeenCalledTimes(1);
  });

  it('faz ROLLBACK quando ocorre erro após persistir o pedido (Cenário 3)', async () => {
    config.simulateApiFailure = true;

    await expect(service.createOrder(dto)).rejects.toThrow();

    // Deve iniciar a transação e inserir o pedido...
    expect(queries[0]).toBe('BEGIN');
    expect(queries[1]).toContain('INSERT INTO orders');
    // ...mas NÃO deve inserir o evento Outbox (falha antes disso)...
    expect(queries.some((q) => q.includes('INSERT INTO outbox_events'))).toBe(
      false,
    );
    // ...e deve reverter tudo com ROLLBACK (sem COMMIT).
    expect(queries).toContain('ROLLBACK');
    expect(queries).not.toContain('COMMIT');
    expect(client.release).toHaveBeenCalledTimes(1);
  });

  it('insere o pedido e a Outbox com o MESMO aggregate_id/order_id', async () => {
    // Captura os parâmetros das inserções para checar a correlação.
    const calls: Array<{ sql: string; params: unknown[] }> = [];
    client.query = jest.fn((sql: string, params?: unknown[]) => {
      calls.push({ sql, params: params ?? [] });
      return Promise.resolve({ rows: [], rowCount: 1 });
    });

    const result = await service.createOrder(dto);

    const orderInsert = calls.find((c) => c.sql.includes('INSERT INTO orders'));
    const outboxInsert = calls.find((c) =>
      c.sql.includes('INSERT INTO outbox_events'),
    );
    expect(orderInsert).toBeDefined();
    expect(outboxInsert).toBeDefined();

    // order.id == outbox.aggregate_id == result.id
    const orderId = orderInsert!.params[0];
    const aggregateId = outboxInsert!.params[1];
    expect(orderId).toBe(result.id);
    expect(aggregateId).toBe(result.id);
  });
});
