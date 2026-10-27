import { ProcessedEventsService } from './processed-events.service';
import { DatabaseService } from '@app/common';

/**
 * Testes unitários da idempotência do consumer.
 *
 * tryMarkProcessed usa INSERT ... ON CONFLICT DO NOTHING:
 *  - rowCount=1 -> evento novo (true);
 *  - rowCount=0 -> duplicado (false).
 */
describe('ProcessedEventsService (unidade)', () => {
  let service: ProcessedEventsService;
  let db: { query: jest.Mock };

  beforeEach(() => {
    db = { query: jest.fn() };
    service = new ProcessedEventsService(db as unknown as DatabaseService);
  });

  it('retorna true quando o evento é inserido pela primeira vez', async () => {
    db.query.mockResolvedValue({ rowCount: 1, rows: [] });

    const isNew = await service.tryMarkProcessed('evt-1');

    expect(isNew).toBe(true);
    // Garante o uso do ON CONFLICT DO NOTHING (operação idempotente).
    const sql = db.query.mock.calls[0][0] as string;
    expect(sql).toContain('ON CONFLICT');
    expect(sql).toContain('DO NOTHING');
  });

  it('retorna false quando o evento já existe (duplicado)', async () => {
    db.query.mockResolvedValue({ rowCount: 0, rows: [] });

    const isNew = await service.tryMarkProcessed('evt-1');

    expect(isNew).toBe(false);
  });

  it('trata rowCount nulo como duplicado (defensivo)', async () => {
    db.query.mockResolvedValue({ rowCount: null, rows: [] });

    const isNew = await service.tryMarkProcessed('evt-1');

    expect(isNew).toBe(false);
  });
});
