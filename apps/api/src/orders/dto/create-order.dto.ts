import { IsNumber, IsPositive, IsString, MaxLength, MinLength } from 'class-validator';

/**
 * DTO de entrada para criação de pedido.
 * Validado automaticamente pelo ValidationPipe global (class-validator).
 */
export class CreateOrderDto {
  @IsString()
  @MinLength(1)
  @MaxLength(255)
  customer_name!: string;

  // Valor monetário positivo. maxDecimalPlaces mantém coerência com NUMERIC(12,2).
  @IsNumber({ maxDecimalPlaces: 2 })
  @IsPositive()
  amount!: number;
}
