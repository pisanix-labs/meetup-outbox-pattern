/**
 * Constantes de topologia do RabbitMQ compartilhadas entre worker e consumer.
 *
 * Centralizar aqui garante que publisher e consumer declarem exatamente
 * a mesma exchange, fila e binding — evitando divergência de configuração.
 */

/** Tipo da exchange: direct (roteamento por routing key exata). */
export const EXCHANGE_TYPE = 'direct';
