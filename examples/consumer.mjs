import _ from 'lodash';
import { Kafka, logLevel } from 'kafkajs';
import Redis from 'ioredis';
import { Batcher, /* FsOffsetDeduper, */ RedisOffsetDeduper } from '../dist/index.mjs';
import { delay } from './delay.mjs';
import { gracefull } from './gracefull.mjs';

const TOPIC = 'example-topic';
const TOPIC_NUM_PARTITIONS = 2;

async function main() {
  const redisClient = new Redis();

  const kafka = new Kafka({
    logLevel: logLevel.WARN,
    clientId: 'my-app',
    brokers: ['localhost:29092'],
  });

  const admin = kafka.admin();

  await admin.connect();
  try {
    const { topics } = await admin.fetchTopicMetadata({ topics: [TOPIC] });
    const { partitions } = topics[0];
    if (_.size(partitions) !== TOPIC_NUM_PARTITIONS) {
      await admin.deleteTopics({ topics: [TOPIC] });
      throw new Error('Invalid partitions count - recreating topic');
    }
  } catch (error) {
    console.warn('kafka-admin', error.message);
    await admin.createTopics({
      topics: [
        {
          topic: TOPIC,
          numPartitions: TOPIC_NUM_PARTITIONS,
        },
      ],
    });
  }

  const consumer = kafka.consumer({
    groupId: 'example-group-id',
    sessionTimeout: 6_000,
  });

  await consumer.connect();
  await consumer.subscribe({ topics: [TOPIC], fromBeginning: true });

  const batcher = new Batcher({
    consumer,
    batchSize: 2,
    maxIdleMs: 5000,
    // offsetDeduper: new FsOffsetDeduper(),
    offsetDeduper: new RedisOffsetDeduper({ redisClient }),
    getMessageInfo: (message) => {
      return {
        count: 1,
        // storeKey: `${topic}-${partition}`,
        storeKey: message.value.toString(),
      };
    },
    handler: async ({ messages /* batchContext */ }) => {
      console.log(
        'batch handler',
        _.map(messages, (m) => m.message.value.toString()),
      );
      // long running task
      await delay(10_000);
    },
  });

  await consumer.run({
    eachBatchAutoResolve: false,
    autoCommit: false,
    eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
      const { topic, partition, messages } = batch;
      const interval = setInterval(async () => {
        try {
          await heartbeat();
        } catch (error) {
          console.log('heatbeat error', error);
        }
      }, 1_000);

      try {
        await batcher.ingestBatch({
          messages,
          topic,
          partition,
          resolveOffset,
        });
      } catch (error) {
        console.log('ingestBatch error', error);
      }

      clearInterval(interval);
    },
  });

  gracefull({
    graceMs: 20_000,
    signals: ['SIGTERM', 'SIGINT'],
    fn: async () => {
      consumer.pause([{ topic: TOPIC }]);
      await batcher.stop();
      await consumer.disconnect();
      await redisClient.quit();
      await admin.disconnect();
    },
  });
}

main().catch((err) => console.log(err));
