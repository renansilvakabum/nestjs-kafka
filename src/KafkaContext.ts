import { KafkaContext } from '@nestjs/microservices';
import { BaseRpcContext } from '@nestjs/microservices/ctx-host/base-rpc.context';
import { Consumer, KafkaMessage } from 'kafkajs';
declare type KafkaContextArgs = [KafkaMessage, number, string, Consumer];

export class KafkaCustomContext extends BaseRpcContext<KafkaContextArgs> {
  constructor(args: KafkaContextArgs) {
    super(args);
  }
  getMessage(): KafkaMessage {
    return this.args[0];
  }
  /**
   * Returns the partition.
   */
  getPartition(): number {
    return this.args[1];
  }
  /**
   * Returns the name of the topic.
   */
  getTopic(): string {
    return this.args[2];
  }

  getConsumer(): Consumer {
    return this.args[3];
  }
}
