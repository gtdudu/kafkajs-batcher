import debug from 'debug';
import { KafkaMessage, Consumer, TopicPartitionOffsetAndMetadata } from 'kafkajs';
import _ from 'lodash';
import { Queue } from './queue';
import { FsOffsetDeduper } from './dedupers/fsOffsetDeduper';
import { RedisOffsetDeduper } from './dedupers/redisOffsetDeduper';
import { OffsetDeduper } from './dedupers/interfaces';

const log = debug('gtdudu:batcher');

export type WrappedKafkaMessage = {
  message: KafkaMessage;
  topic: string;
  partition: number;
  offset: string;
};

interface BatchContext {
  [key: string]: unknown;
}

interface Store {
  [key: string]: {
    messages: WrappedKafkaMessage[];
    count: number;
    batchContext: BatchContext;
  };
}

type Timer = ReturnType<typeof setTimeout>;
interface Timeouts {
  [key: string]: Timer;
}

interface BatcherHandlerParams {
  messages: WrappedKafkaMessage[];
  batchContext: BatchContext;
}
type BatcherHandler = (params: BatcherHandlerParams) => Promise<any>;

type GetMessageFunctionInfo = (
  kafkaMessage: any,
  topic: string,
  partition: number,
) =>
  | Promise<{
      [key: string]: unknown;
      count: number;
      storeKey: string;
    }>
  | {
      [key: string]: unknown;
      count: number;
      storeKey: string;
    };

interface BatcherConstructorOptions {
  handler: BatcherHandler;
  consumer: Consumer;
  getMessageInfo?: GetMessageFunctionInfo;
  batchSize?: number;
  maxIdleMs?: number;
  offsetDeduper?: OffsetDeduper;
}

interface IngestBatchParams {
  messages: KafkaMessage[];
  resolveOffset(offset: string): void;
  topic: string;
  partition: number;
}

interface FlushParams {
  messages: WrappedKafkaMessage[];
  batchContext: BatchContext;
  storeKey: string;
  count: number;
  commitOptions: TopicPartitionOffsetAndMetadata[];
}

export class Batcher {
  private store: Store;
  private timeouts: Timeouts;
  private one: bigint;
  private ready: boolean;
  private maxIdleMs: number;
  private batchSize: number;
  private handler: BatcherHandler;
  private getMessageInfo: GetMessageFunctionInfo;
  private consumer: Consumer;
  private flushQueue: Queue;
  private prepareQueue: Queue;
  private offsetDeduper: OffsetDeduper | FsOffsetDeduper | RedisOffsetDeduper | null;

  constructor(opts: BatcherConstructorOptions) {
    if (!_.isFunction(opts.handler)) {
      throw new Error('Batcher.handler must be a function');
    }
    this.handler = opts.handler;

    if (
      !(
        opts.consumer &&
        opts.consumer.commitOffsets &&
        _.isFunction(opts?.consumer.commitOffsets) &&
        _.isFunction(opts?.consumer.pause)
      )
    ) {
      throw new Error('Batcher.consumer - kafkajs consumer expected');
    }

    this.consumer = opts.consumer;

    this.offsetDeduper = null;
    if (!_.isUndefined(opts.offsetDeduper)) {
      this.offsetDeduper = opts.offsetDeduper;
    }

    this.getMessageInfo = opts.getMessageInfo || this._getMessageInfo;

    this.batchSize = opts.batchSize || 3;
    this.maxIdleMs = opts.maxIdleMs || 5000;

    this.ready = true;
    this.store = {};
    this.timeouts = {};
    this.one = BigInt(1); // needed to compute commit offset

    this.prepareQueue = new Queue({
      handler: this.prepare.bind(this),
    });
    this.flushQueue = new Queue({
      handler: this.flush.bind(this),
    });
  }

  public async stop() {
    this.ready = false;

    const storeKeys = _.keys(this.timeouts);
    _.each(storeKeys, (storeKey) => {
      this.clearTimeout(storeKey);
    });

    await Promise.all([this.flushQueue.stop(), this.prepareQueue.stop()]);
  }

  private getBatchCommitMap(messages: WrappedKafkaMessage[]) {
    return _(messages)
      .groupBy('topic')
      .values()
      .map((topicBatch) => _(topicBatch).groupBy('partition').values().map(_.last).value())
      .flattenDeep()
      .compact()
      .transform(
        (acc, m) => {
          if (!acc[m.topic]) acc[m.topic] = {};
          acc[m.topic][m.partition] = BigInt(m.offset) + this.one;
        },
        {} as { [key: string]: { [key: number]: bigint } },
      )
      .value();
  }

  private getPendingCommitMap() {
    return _(this.store)
      .values()
      .map((o) => o.messages) // grab all messages
      .flattenDeep() // flatten
      .groupBy('topic') // group them by topic
      .values()
      .map((topicMessages) => _(topicMessages).groupBy('partition').values().map(_.first).value()) // get the first message for each partition (smallestOffset)
      .flattenDeep()
      .compact()
      .transform(
        (acc, m) => {
          // format for easier consumption: topic.partition = offset
          if (!acc[m.topic]) acc[m.topic] = {};
          acc[m.topic][m.partition] = BigInt(m.offset);
        },
        {} as { [key: string]: { [key: number]: bigint } },
      )
      .value();
  }

  private async commitSkipped(skipped: WrappedKafkaMessage[]) {
    const commitMap = this.getBatchCommitMap(skipped);
    const commitOptions: TopicPartitionOffsetAndMetadata[] = [];
    _.each(commitMap, (topicPartitionMap, topic) =>
      _.each(topicPartitionMap, (offset, partition) => {
        commitOptions.push({
          topic,
          partition: _.parseInt(partition),
          offset: offset.toString(),
        });
      }),
    );

    await this.consumer.commitOffsets(commitOptions);
  }

  private getCommitOptions(messages: WrappedKafkaMessage[]) {
    const batchCommitMap = this.getBatchCommitMap(messages);
    const pendingCommitMap = this.getPendingCommitMap();

    const options: TopicPartitionOffsetAndMetadata[] = [];
    _.each(batchCommitMap, (topicPartitionMap, topic) => {
      _.each(topicPartitionMap, (offset, partition) => {
        const pending = _.get(pendingCommitMap, [topic, partition]);
        const partitionInt = _.parseInt(partition);

        if (_.isUndefined(pending)) {
          options.push({
            topic,
            partition: partitionInt,
            offset: offset.toString(),
          });
          return;
        }

        options.push({
          topic,
          partition: partitionInt,
          offset: (pending > offset ? offset : pending).toString(),
        });
      });
    });

    return options;
  }

  private async flush({ storeKey, messages, count, batchContext, commitOptions }: FlushParams) {
    log('flush', { storeKey, count, batchContext, messages });

    try {
      await this.handler({ messages, batchContext });
      log('commiting', commitOptions);

      await this.consumer.commitOffsets(commitOptions);

      // everything went fine for current storeKey
      // mark all spec as done in kafkaDeduper in case
      // other storeKey for same partition fail in the future
      // otherwise some message may be consumed more than once
      this.offsetDeduper && (await this.offsetDeduper.setHasConsumed({ messages, commitOptions }));
    } catch (error) {
      const pauseOptions = _(messages)
        .groupBy('topic')
        .keys()
        .map((key) => ({ topic: key }))
        .value();

      log('pausing topic', pauseOptions);

      this.consumer.pause(pauseOptions);
      this.ready = false;
      log('Flush error', error);
    }
  }

  private async _getMessageInfo(message: KafkaMessage, topic: string, partition: number) {
    return {
      storeKey: `${topic}-${partition}`,
      count: 1,
    };
  }

  private clearTimeout(storeKey: string) {
    log(`CLEARTIMEOUT ${storeKey}`);

    clearTimeout(this.timeouts[storeKey]);
    delete this.timeouts[storeKey];
  }

  private push({ topic, partition, message, count, storeKey, batchContext }: any) {
    if (!this.store[storeKey]) {
      this.store[storeKey] = {
        messages: [],
        count: 0,
        batchContext,
      };
    }
    this.store[storeKey].messages.push({
      message,
      topic,
      partition,
      offset: message.offset,
    });
    this.store[storeKey].count += count;

    log(`${storeKey}: ${topic}/${partition}/${message.offset} PUSH`, {
      incBy: count,
      totalCount: this.store[storeKey].count,
    });

    return this.store[storeKey].count;
  }

  private async prepare(storeKey: string) {
    const { messages, count, batchContext } = this.store[storeKey];
    delete this.store[storeKey];

    const commitOptions = this.getCommitOptions(messages);
    await this.flushQueue.enqueue({ storeKey, messages, count, batchContext, commitOptions });
  }

  private setTimeouts(storeKeys: string[]) {
    _.each(storeKeys, (storeKey) => {
      log(`SETTIMOUT ${storeKey}`);

      const timeout = setTimeout(async () => {
        log(`MAX_TIME_ELAPSED ${storeKey}`);
        this.clearTimeout(storeKey);
        await this.prepareQueue.enqueue(storeKey);
      }, this.maxIdleMs);

      this.timeouts[storeKey] = timeout;
    });
  }

  public async ingestBatch({
    messages,
    topic,
    partition,
    resolveOffset,
  }: IngestBatchParams): Promise<undefined> {
    if (!this.ready) {
      log('cannot ingest batch - no longer ready');
      return;
    }

    const storeKeys = [];
    const skipped = [];

    for (let index = 0; index < messages.length; index++) {
      const message = messages[index];
      const { offset } = message;

      if (!this.ready) {
        log('cannot ingest batch - no longer ready');
        resolveOffset(offset);
        return;
      }

      const alreadyConsumed =
        this.offsetDeduper &&
        (await this.offsetDeduper.checkIfAlreadyConsumed({
          topic,
          partition,
          offset,
        }));

      if (alreadyConsumed) {
        log(`Skipping message ${topic}/${partition}/${offset} - already consumed`);
        resolveOffset(offset);
        skipped.push({
          message,
          topic,
          partition,
          offset,
        });
        continue;
      }

      try {
        // eslint-disable-next-line no-var
        var { count, storeKey, ...batchContext } = await this.getMessageInfo(
          message,
          topic,
          partition,
        );
      } catch (error) {
        log(`Skipping message ${topic}/${partition}/${offset} - get message info errored`, error);
        resolveOffset(offset);
        continue;
      }

      // remove timeout if it exists
      this.clearTimeout(storeKey);
      const countForStoreKey = this.push({
        topic,
        partition,
        message,
        count,
        storeKey,
        batchContext,
      });

      // resolve offset to prevent consuming same message over and over - but do not commit!
      resolveOffset(offset);

      // if batch size is reached flush right away
      if (countForStoreKey >= this.batchSize) {
        await this.prepareQueue.enqueue(storeKey);
        _.pull(storeKeys, storeKey);
        continue;
      }

      if (_.includes(storeKeys, storeKey)) continue;
      storeKeys.push(storeKey);
    }

    // create timeout for all storeKeys that were not flushed
    this.setTimeouts(storeKeys);

    if (_.isEmpty(this.store) && _.size(skipped)) {
      await this.commitSkipped(skipped);
    }
  }
}
