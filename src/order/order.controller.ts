import { Controller, Inject, OnModuleInit, Post } from '@nestjs/common';
import {
  ClientKafka,
  ClientProxy,
  Ctx,
  EventPattern,
  KafkaContext,
  MessagePattern,
  Payload,
} from '@nestjs/microservices';
import { countReset } from 'console';
import { Producer } from 'kafkajs';
import { KafkaCustomContext } from 'src/KafkaContext';

@Controller('order')
export class OrderController {
  private identity: number;
  constructor(@Inject('KAFKA_PRODUCER') private client: Producer) {
    this.identity = new Date().getTime();
  }

  @EventPattern('pedido-criado')
  async criarOrder(
    @Payload() message: any,
    @Ctx() context: KafkaCustomContext,
  ) {
    console.log(
      `Topic: ${context.getTopic()} Message: ${message.value.nome} App: ${
        this.identity
      }`,
    );
    await this.client.send({
      messages: [
        {
          value: 'teste',
        },
      ],
      topic: 'pagamento-aprovado',
      acks: 0,
    });

    const consumer = context.getConsumer();
    await consumer.commitOffsets([
      {
        topic: message.topic,
        partition: message.partition,
        offset: message.offset,
      },
    ]);
  }

  @EventPattern('pagamento-aprovado')
  async pagamentoAprovado(
    @Payload() message: any,
    @Ctx() context: KafkaCustomContext,
  ) {
    const consumer = context.getConsumer();
    console.log(
      `Topic: ${context.getTopic()} Message: ${JSON.stringify(message)}`,
    );

    await consumer.commitOffsets([message]);
  }

  @EventPattern('stock')
  async criarMercadoria(
    @Payload() message: any,
    @Ctx() context: KafkaCustomContext,
  ) {
    const consumer = context.getConsumer();
    console.log(
      `Topic: ${context.getTopic()} Message: ${JSON.stringify(message)}`,
    );

    await consumer.commitOffsets([message]);
  }

  @Post('teste')
  teste(): void {
    console.log('ok');
  }
}
