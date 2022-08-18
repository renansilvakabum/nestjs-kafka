import { Module } from '@nestjs/common';
import { ClientKafka, ClientsModule, Transport } from '@nestjs/microservices';

import { OrderController } from './order.controller';

@Module({
  controllers: [OrderController],
  imports: [
    ClientsModule.register([
      {
        name: 'KAFKA_SERVICE',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'ms-order',
            brokers: ['localhost:9092'],
          },
        },
      },
    ]),
  ],
  providers: [
    {
      provide: 'KAFKA_PRODUCER',
      useFactory: (kafkaService: ClientKafka) => {
        return kafkaService.connect();
      },
      inject: ['KAFKA_SERVICE'],
    },
  ],
})
export class OrderModule {}
