import {
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Param,
  ParseUUIDPipe,
  Post,
} from '@nestjs/common';
import { CreateOrderDto } from './dto/create-order.dto';
import { OrdersService } from './orders.service';

/**
 * Controller HTTP de pedidos.
 * Mantém-se fino: apenas recebe a requisição, delega ao serviço e responde.
 */
@Controller('orders')
export class OrdersController {
  constructor(private readonly ordersService: OrdersService) {}

  /**
   * POST /orders
   * Cria um pedido e, na mesma transação, o evento OrderCreated.
   * Responde 201 com o id e o status do pedido.
   */
  @Post()
  @HttpCode(HttpStatus.CREATED)
  async create(
    @Body() dto: CreateOrderDto,
  ): Promise<{ id: string; status: string }> {
    return this.ordersService.createOrder(dto);
  }

  /**
   * GET /orders/:id
   * Retorna os dados de um pedido.
   */
  @Get(':id')
  async findOne(@Param('id', new ParseUUIDPipe()) id: string) {
    const order = await this.ordersService.findOne(id);
    return {
      id: order.id,
      customer_name: order.customer_name,
      amount: Number(order.amount),
      status: order.status,
      created_at: order.created_at,
    };
  }
}
