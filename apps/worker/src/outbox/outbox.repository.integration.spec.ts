import { Pool } from 'pg';
import { randomUUID } from 'crypto';
import { OutboxRepository } from './outbox.repository';
import { DatabaseService } from '@app/common';

/**
 * Testes de INTEGRAÇÃO do OutboxRepository contra o PostgreSQL real.
 *
 * Requer o banco do docker-compose no ar:
 *   docker compose up -d postgres
 *
 * Caso o banco não esteja acessível, os testes são PULADOS (skip) com um
 * aviso, para não quebrar a suíte em ambientes sem infraestrutura.
 *
 * Cobre as garantias do padrão difíceis de testar sem banco:
 *  - reserva de eventos pendentes com FOR UPDATE SKIP LOCKED;
 *  - recuperação de reservas (lease) expiradas.
 */

const DATABASE_URL =
  process.env.DATABASE_URL ??
  `postgres://postgres:postgres@localhost:${process.env.POSTGRES_PORT ?? '5432'}/outbox`;

// Helper que cria um DatabaseService-like apontando para um pool real.
function makeDb(pool: Pool): DatabaseService {
  return {
    query: (text: string, params?: unknown[]) => pool.query(text, params),
    getClient: () => pool.connect(),
  } as unknown as DatabaseService;
}

describe('OutboxRepository (integração)', () => {
  let pool: Pool;
  let repo: OutboxRepository;
  let available = false;

  beforeAll(async () => {
    pool = new Pool({ connectionString: DATABASE_URL, connectionTimeoutMillis: 2000 });
    try {
      await pool.query('SELECT 1');
      available = true;
      repo = new OutboxRepository(makeDb(pool));
    } catch {
      available = false;
      // eslint-disable-next-line no-console
      console.warn(
        '\n[integração] PostgreSQL indisponível — testes de integração PULADOS.\n' +
          'Suba o banco com: docker compose up -d postgres\n',
      );
    }
  });

  afterAll(async () => {
    await pool?.end().catch(() => undefined);
  });

  // Insere um evento de teste diretamente na outbox.
  async function seedEvent(overrides: Partial<{ nextAttemptAt: string }> = {}): Promise<string> {
    const id = randomUUID();
    const orderId = randomUUID();
    const envelope = {
      event_id: id,
      event_type: 'OrderCreated',
      occurred_at: new Date().toISOString(),
      data: { order_id: orderId, customer_name: 'Teste', amount: 1 },
    };
    await pool.query(
      `INSERT INTO outbox_events
         (id, aggregate_id, event_type, payload, status, attempts, created_at, next_attempt_at)
       VALUES ($1, $2, 'OrderCreated', $3, 'PENDING', 0, now(), ${overrides.nextAttemptAt ?? 'now()'})`,
      [id, orderId, JSON.stringify(envelope)],
    );
    return id;
  }

  it('reserva eventos PENDING elegíveis e os move para PROCESSING (SKIP LOCKED)', async () => {
    if (!available) return;

    const id = await seedEvent();

    const reserved = await repo.reserveBatch(10, 30000);
    const mine = reserved.find((e) => e.id === id);

    expect(mine).toBeDefined();
    expect(mine!.attempts).toBe(1); // incrementado na reserva

    const row = await pool.query(
      'SELECT status, locked_until FROM outbox_events WHERE id = $1',
      [id],
    );
    expect(row.rows[0].status).toBe('PROCESSING');
    expect(row.rows[0].locked_until).not.toBeNull();

    // Limpeza.
    await pool.query('DELETE FROM outbox_events WHERE id = $1', [id]);
  });

  it('NÃO reserva eventos cujo next_attempt_at está no futuro', async () => {
    if (!available) return;

    const id = await seedEvent({ nextAttemptAt: "now() + interval '1 hour'" });

    const reserved = await repo.reserveBatch(10, 30000);
    expect(reserved.find((e) => e.id === id)).toBeUndefined();

    const row = await pool.query(
      'SELECT status FROM outbox_events WHERE id = $1',
      [id],
    );
    expect(row.rows[0].status).toBe('PENDING');

    await pool.query('DELETE FROM outbox_events WHERE id = $1', [id]);
  });

  it('recupera reservas (lease) expiradas, voltando o evento para PENDING', async () => {
    if (!available) return;

    const id = randomUUID();
    const orderId = randomUUID();
    const envelope = { event_id: id, event_type: 'OrderCreated', occurred_at: new Date().toISOString(), data: {} };
    // Evento PROCESSING com lease já expirado (locked_until no passado).
    await pool.query(
      `INSERT INTO outbox_events
         (id, aggregate_id, event_type, payload, status, attempts, created_at, next_attempt_at, locked_until)
       VALUES ($1, $2, 'OrderCreated', $3, 'PROCESSING', 1, now(), now(), now() - interval '1 minute')`,
      [id, orderId, JSON.stringify(envelope)],
    );

    const recovered = await repo.recoverExpiredLeases();
    expect(recovered).toBeGreaterThanOrEqual(1);

    const row = await pool.query(
      'SELECT status, locked_until FROM outbox_events WHERE id = $1',
      [id],
    );
    expect(row.rows[0].status).toBe('PENDING');
    expect(row.rows[0].locked_until).toBeNull();

    await pool.query('DELETE FROM outbox_events WHERE id = $1', [id]);
  });

  it('markPublished marca PUBLISHED e limpa o lease', async () => {
    if (!available) return;

    const id = await seedEvent();
    await repo.reserveBatch(10, 30000); // vai para PROCESSING
    await repo.markPublished(id);

    const row = await pool.query(
      'SELECT status, published_at, locked_until FROM outbox_events WHERE id = $1',
      [id],
    );
    expect(row.rows[0].status).toBe('PUBLISHED');
    expect(row.rows[0].published_at).not.toBeNull();
    expect(row.rows[0].locked_until).toBeNull();

    await pool.query('DELETE FROM outbox_events WHERE id = $1', [id]);
  });
});
