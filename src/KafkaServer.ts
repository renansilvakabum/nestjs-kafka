import { Logger } from '@nestjs/common/services/logger.service';
import { isNil } from '@nestjs/common/utils/shared.utils';
import {
  CustomTransportStrategy,
  KafkaContext,
  KafkaHeaders,
  KafkaLogger,
  KafkaOptions,
  KafkaParser,
  OutgoingResponse,
  Serializer,
  Server,
  Transport,
} from '@nestjs/microservices';
import {
  BrokersFunction,
  Consumer,
  ConsumerConfig,
  EachMessagePayload,
  Kafka,
  KafkaConfig,
  KafkaMessage,
  Message,
  Producer,
  RecordMetadata,
} from 'kafkajs';
import { Observable } from 'rxjs';
export const NO_MESSAGE_HANDLER = `There is no matching message handler defined in the remote service.`;
let kafkaPackage: any = {};

import {
  isObject,
  isPlainObject,
  isString,
  isUndefined,
} from '@nestjs/common/utils/shared.utils';
import { KafkaCustomContext } from './KafkaContext';

export interface KafkaRequest<T = any> {
  key: Buffer | string | null;
  value: T;
  headers: Record<string, any>;
}

export class KafkaRequestSerializer
  implements Serializer<any, KafkaRequest | Promise<KafkaRequest>>
{
  serialize(value: any) {
    const isNotKafkaMessage =
      isNil(value) ||
      !isObject(value) ||
      (!('key' in value) && !('value' in value));

    if (isNotKafkaMessage) {
      value = { value };
    }
    value.value = this.encode(value.value);
    if (!isNil(value.key)) {
      value.key = this.encode(value.key);
    }
    if (isNil(value.headers)) {
      value.headers = {};
    }
    return value;
  }

  public encode(value: any): Buffer | string | null {
    const isObjectOrArray =
      !isNil(value) && !isString(value) && !Buffer.isBuffer(value);

    if (isObjectOrArray) {
      return isPlainObject(value) || Array.isArray(value)
        ? JSON.stringify(value)
        : value.toString();
    } else if (isUndefined(value)) {
      return null;
    }
    return value;
  }
}

export class ServerKafka extends Server implements CustomTransportStrategy {
  public readonly transportId = Transport.KAFKA;

  protected logger = new Logger(ServerKafka.name);
  protected client: Kafka = null;
  protected consumer: Consumer = null;
  protected producer: Producer = null;
  protected parser: KafkaParser = null;

  protected brokers: string[] | BrokersFunction;
  protected clientId: string;
  protected groupId: string;

  constructor(protected readonly options: KafkaOptions['options']) {
    super();

    const clientOptions =
      this.getOptionsProp(this.options, 'client') || ({} as KafkaConfig);
    const consumerOptions =
      this.getOptionsProp(this.options, 'consumer') || ({} as ConsumerConfig);
    const postfixId =
      this.getOptionsProp(this.options, 'postfixId') || '-server';

    this.brokers = clientOptions.brokers;

    // append a unique id to the clientId and groupId
    // so they don't collide with a microservices client
    this.clientId = clientOptions.clientId + postfixId;
    this.groupId = consumerOptions.groupId + postfixId;

    kafkaPackage = this.loadPackage('kafkajs', ServerKafka.name, () =>
      require('kafkajs'),
    );

    this.parser = new KafkaParser((options && options.parser) || undefined);

    this.initializeSerializer(options);
    this.initializeDeserializer(options);
  }

  public async listen(
    callback: (err?: unknown, ...optionalParams: unknown[]) => void,
  ): Promise<void> {
    try {
      this.client = this.createClient();
      await this.start(callback);
    } catch (err) {
      callback(err);
    }
  }

  public async close(): Promise<void> {
    this.consumer && (await this.consumer.disconnect());
    this.producer && (await this.producer.disconnect());
    this.consumer = null;
    this.producer = null;
    this.client = null;
  }

  public async start(callback: () => void): Promise<void> {
    const consumerOptions = Object.assign(this.options.consumer || {}, {
      groupId: this.groupId,
    });
    this.consumer = this.client.consumer(consumerOptions);
    this.producer = this.client.producer(this.options.producer);

    await this.consumer.connect();
    await this.producer.connect();
    await this.bindEvents(this.consumer);
    callback();
  }

  public createClient<T = any>(): T {
    return new kafkaPackage.Kafka(
      Object.assign(
        { logCreator: KafkaLogger.bind(null, this.logger) },
        this.options.client,
        { clientId: this.clientId, brokers: this.brokers },
      ) as KafkaConfig,
    );
  }

  public async bindEvents(consumer: Consumer) {
    const registeredPatterns = [...this.messageHandlers.keys()];
    const consumerSubscribeOptions = this.options.subscribe || {};
    const subscribeToPattern = async (pattern: string) =>
      consumer.subscribe({
        topic: pattern,
        ...consumerSubscribeOptions,
      });
    await Promise.all(registeredPatterns.map(subscribeToPattern));

    await consumer.run({
      autoCommit: false,
      eachBatchAutoResolve: false,
      eachBatch: async ({
        batch,
        resolveOffset,
        heartbeat,
        commitOffsetsIfNecessary,
        uncommittedOffsets,
        isRunning,
        isStale,
      }) => {
        for (const message of batch.messages) {
          this.handleMessage({
            heartbeat,
            message,
            topic: batch.topic,
            partition: batch.partition,
          });
          resolveOffset(message.offset);
          await heartbeat();
        }
      },
    });
  }

  public getMessageHandler() {
    return async (payload: EachMessagePayload) => this.handleMessage(payload);
  }

  public getPublisher(
    replyTopic: string,
    replyPartition: string,
    correlationId: string,
  ): (data: any) => Promise<RecordMetadata[]> {
    return (data: any) =>
      this.sendMessage(data, replyTopic, replyPartition, correlationId);
  }

  public async handleMessage(payload: EachMessagePayload) {
    const channel = payload.topic;
    const rawMessage = this.parser.parse<KafkaMessage>(
      Object.assign(payload.message, {
        topic: payload.topic,
        partition: payload.partition,
      }),
    );
    const headers = rawMessage.headers as unknown as Record<string, any>;
    const correlationId = headers[KafkaHeaders.CORRELATION_ID];
    const replyTopic = headers[KafkaHeaders.REPLY_TOPIC];
    const replyPartition = headers[KafkaHeaders.REPLY_PARTITION];

    const packet = await this.deserializer.deserialize(rawMessage, { channel });
    const kafkaContext = new KafkaCustomContext([
      rawMessage,
      payload.partition,
      payload.topic,
      this.consumer,
    ]);
    const handler = this.getHandlerByPattern(packet.pattern);
    // if the correlation id or reply topic is not set
    // then this is an event (events could still have correlation id)
    if (handler?.isEventHandler || !correlationId || !replyTopic) {
      return this.handleEvent(packet.pattern, packet, kafkaContext);
    }

    const publish = this.getPublisher(
      replyTopic,
      replyPartition,
      correlationId,
    );

    if (!handler) {
      return publish({
        id: correlationId,
        err: NO_MESSAGE_HANDLER,
      });
    }

    const response$ = this.transformToObservable(
      await handler(packet.data, kafkaContext),
    );
    response$ && this.send(response$, publish);
  }

  public async sendMessage(
    message: OutgoingResponse,
    replyTopic: string,
    replyPartition: string,
    correlationId: string,
  ): Promise<RecordMetadata[]> {
    const outgoingMessage = await this.serializer.serialize(message.response);
    this.assignReplyPartition(replyPartition, outgoingMessage);
    this.assignCorrelationIdHeader(correlationId, outgoingMessage);
    this.assignErrorHeader(message, outgoingMessage);
    this.assignIsDisposedHeader(message, outgoingMessage);

    const replyMessage = Object.assign(
      {
        topic: replyTopic,
        messages: [outgoingMessage],
      },
      this.options.send || {},
    );
    return this.producer.send(replyMessage);
  }

  public assignIsDisposedHeader(
    outgoingResponse: OutgoingResponse,
    outgoingMessage: Message,
  ) {
    if (!outgoingResponse.isDisposed) {
      return;
    }
    outgoingMessage.headers[KafkaHeaders.NEST_IS_DISPOSED] = Buffer.alloc(1);
  }

  public assignErrorHeader(
    outgoingResponse: OutgoingResponse,
    outgoingMessage: Message,
  ) {
    if (!outgoingResponse.err) {
      return;
    }
    outgoingMessage.headers[KafkaHeaders.NEST_ERR] = Buffer.from(
      outgoingResponse.err,
    );
  }

  public assignCorrelationIdHeader(
    correlationId: string,
    outgoingMessage: Message,
  ) {
    outgoingMessage.headers[KafkaHeaders.CORRELATION_ID] =
      Buffer.from(correlationId);
  }

  public assignReplyPartition(
    replyPartition: string,
    outgoingMessage: Message,
  ) {
    if (isNil(replyPartition)) {
      return;
    }
    outgoingMessage.partition = parseFloat(replyPartition);
  }

  protected initializeSerializer(options: KafkaOptions['options']) {
    this.serializer =
      (options && options.serializer) || new KafkaRequestSerializer();
  }
}
