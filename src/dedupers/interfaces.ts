import { TopicPartitionOffsetAndMetadata } from 'kafkajs';
import { WrappedKafkaMessage } from '../batcher';

export interface SetHasConsumedParams {
  messages: WrappedKafkaMessage[];
  commitOptions: TopicPartitionOffsetAndMetadata[];
}

export interface CheckIfAlreadyConsumedParams {
  topic: string;
  partition: number;
  offset: string;
}

export interface OffsetDeduper {
  setHasConsumed: (params: SetHasConsumedParams) => Promise<any>;
  checkIfAlreadyConsumed: (params: CheckIfAlreadyConsumedParams) => Promise<any>;
  [key: string]: any;
}
