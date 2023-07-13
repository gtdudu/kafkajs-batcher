import _ from 'lodash';
import { CheckIfAlreadyConsumedParams, SetHasConsumedParams, OffsetDeduper } from './interfaces';

interface RedisOffsetDeduperConstructorOptions {
  redisClient: any;
  namespace: string;
}

export class RedisOffsetDeduper implements OffsetDeduper {
  /**
   * ioredis client
   */
  redisClient: any;
  /**
   * base prefix for redis sets
   */
  namespace: string;

  constructor(opts: RedisOffsetDeduperConstructorOptions) {
    this.redisClient = opts.redisClient;
    this.namespace = opts.namespace || 'kafkajs-batcher';
  }

  private getPrefixedKey({ topic, partition }: { topic: string; partition: number }) {
    return `${this.namespace}:${topic}:${partition}`;
  }

  public async setHasConsumed({ messages, commitOptions }: SetHasConsumedParams) {
    const op: any[] = [];

    const commitOffsets: { [setKey: string]: string } = {};

    _.each(commitOptions, ({ topic, partition, offset }) => {
      const setKey = this.getPrefixedKey({ topic, partition });
      commitOffsets[setKey] = offset;
      op.push(['zremrangebyscore', setKey, String(0), (BigInt(offset) - BigInt(1)).toString()]);
    });

    _.map(messages, (m) => {
      const { topic, partition, offset } = m;

      const setKey = this.getPrefixedKey({ topic, partition });
      const commitOffset = _.get(commitOffsets, setKey);
      if (commitOffset && BigInt(offset) <= BigInt(commitOffset) - BigInt(1)) return;

      op.push(['zadd', setKey, offset, offset]);
    });

    return this.redisClient.multi(op).exec();
  }

  public async checkIfAlreadyConsumed({ topic, partition, offset }: CheckIfAlreadyConsumedParams) {
    const setKey = this.getPrefixedKey({ topic, partition });
    const isItStored = await this.redisClient.zscore(setKey, offset);
    return Boolean(isItStored);
  }
}
