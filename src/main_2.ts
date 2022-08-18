import { NestFactory } from '@nestjs/core';
import { KafkaOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  const configKafka: KafkaOptions = {
    transport: Transport.KAFKA,
    options: {
      client: {
        brokers: ['localhost:9092'],
        retry: {
          //A funcionalidade de retry � para perda de conex�es
          retries: 30, //� a quantidade m�xima de tentativas de retry
          initialRetryTime: 10000, //Tempo inicial para tentar efetuar uma nova tentativa de conex�o
          maxRetryTime: 5000, //Tempo dos demais intervalos de tentativas de conex�o
        },
      },
      consumer: {
        groupId: 'ms-order',
        maxBytes: 200000,
      },
      run: {
        autoCommitInterval: 10000,
        eachBatchAutoResolve: false,
      },
    },
  };
  app.connectMicroservice(configKafka);
  await app.startAllMicroservices();
  await app.listen(3000);
}
bootstrap();
