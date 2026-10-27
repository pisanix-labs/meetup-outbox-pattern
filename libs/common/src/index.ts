/**
 * Barrel da biblioteca compartilhada @app/common.
 * Reexporta módulos e serviços usados pelas três aplicações.
 */

// Config
export * from './config/app-config.module';
export * from './config/app-config.service';

// Logging
export * from './logging/app-logger.module';

// Database
export * from './database/database.module';
export * from './database/database.service';

// Messaging
export * from './messaging/messaging.module';
export * from './messaging/rabbitmq-publisher.service';
export * from './messaging/rabbitmq.constants';

// Domain
export * from './domain/outbox.types';
