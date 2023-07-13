import debug from 'debug';
import { Consumer, Kafka, Producer, logLevel, Admin } from 'kafkajs';
import { Batcher, FsOffsetDeduper } from '../src';

const TOPIC = 'kafkajs-batcher-topic';
const BATCH_SIZE = 2;
const MAX_IDLE_MS = 2_000;
const LESS_THAN_MAX_IDLE_MS = 500;
const MORE_THAN_MAX_IDLE_MS = 2_500;

const log = debug('gtdudu:jest');

jest.setTimeout(60000);

const delay = (delayMs: number) =>
  new Promise<void>((resolve) => {
    setTimeout(() => {
      resolve();
    }, delayMs);
  });

describe('Batcher', () => {
  let kafka: Kafka;
  let batcher: Batcher;
  let consumer: Consumer;
  let producer: Producer;
  let admin: Admin;
  let deduper: FsOffsetDeduper;

  const batcherHandler = jest.fn().mockImplementation(async (args) => {
    log('batchHandler', args);
  });

  const getMessageInfo = jest.fn().mockImplementation((message) => {
    return {
      count: 1,
      storeKey: message.value.toString(),
    };
  });

  beforeAll(async () => {
    kafka = new Kafka({
      logLevel: logLevel.NOTHING,
      clientId: 'my-app',
      brokers: ['localhost:29092'],
    });

    admin = kafka.admin();
    producer = kafka.producer();
    consumer = kafka.consumer({
      groupId: 'test-group',
      sessionTimeout: 6_000,
    });

    deduper = new FsOffsetDeduper();
    batcher = new Batcher({
      handler: batcherHandler,
      getMessageInfo,
      consumer,
      batchSize: BATCH_SIZE,
      maxIdleMs: MAX_IDLE_MS,
      offsetDeduper: deduper,
    });

    await admin.connect();
    await consumer.connect();
    await producer.connect();

    await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
    await consumer.run({
      eachBatchAutoResolve: false,
      autoCommit: false,
      eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
        const { topic, partition, messages } = batch;

        const interval = setInterval(async () => {
          try {
            await heartbeat();
          } catch (error) {
            log('heatbeat error', error);
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
          log('ingestBatch error', error);
        }

        clearInterval(interval);
      },
    });
  });

  beforeEach(async () => {
    await admin.createTopics({
      topics: [
        {
          topic: TOPIC,
          numPartitions: 2,
        },
      ],
    });
  });

  afterEach(async () => {
    await admin.deleteTopics({
      topics: [TOPIC],
    });
  });

  afterAll(async () => {
    consumer.pause([{ topic: TOPIC }]);
    await batcher.stop();
    await consumer.disconnect();
    await producer.disconnect();

    await admin.disconnect();
  });

  test('full batch', async () => {
    await producer.send({
      topic: TOPIC,
      messages: [{ value: 'A' }, { value: 'A' }, { value: 'A' }, { value: 'A' }],
    });
    await delay(LESS_THAN_MAX_IDLE_MS);
    expect(batcherHandler).toHaveBeenCalledTimes(2);
  });

  test('partial batch', async () => {
    await producer.send({
      topic: TOPIC,
      messages: [{ value: 'A' }],
    });
    await delay(LESS_THAN_MAX_IDLE_MS);
    expect(batcherHandler).toHaveBeenCalledTimes(0);
    await delay(MORE_THAN_MAX_IDLE_MS);
    expect(batcherHandler).toHaveBeenCalledTimes(1);
  });

  test('full then partial batch', async () => {
    await producer.send({
      topic: TOPIC,
      messages: [{ value: 'A' }, { value: 'A' }, { value: 'A' }],
    });
    await delay(LESS_THAN_MAX_IDLE_MS);
    expect(batcherHandler).toHaveBeenCalledTimes(1);
    await delay(MORE_THAN_MAX_IDLE_MS);
    expect(batcherHandler).toHaveBeenCalledTimes(2);
  });

  test('deduper', async () => {
    await producer.send({
      topic: TOPIC,
      messages: [
        { value: 'A', partition: 0 },
        { value: 'B', partition: 0 },
        { value: 'B', partition: 1 },
      ],
    });
    await delay(LESS_THAN_MAX_IDLE_MS);
    expect(deduper.store).toEqual({
      [TOPIC]: [
        {
          commitOffset: '0',
          offsets: ['1'],
        },
        {
          commitOffset: '1',
        },
      ],
    });
    await producer.send({
      topic: TOPIC,
      messages: [{ value: 'A', partition: 1 }],
    });
    await delay(LESS_THAN_MAX_IDLE_MS);
    expect(deduper.store).toEqual({
      [TOPIC]: [
        {
          commitOffset: '1',
          offsets: ['1'],
        },
        {
          commitOffset: '2',
        },
      ],
    });

    await producer.send({
      topic: TOPIC,
      messages: [
        { value: 'B', partition: 0 },
        { value: 'B', partition: 0 },
      ],
    });
    await delay(LESS_THAN_MAX_IDLE_MS);
    expect(deduper.store).toEqual({
      [TOPIC]: [
        {
          commitOffset: '4',
          offsets: [],
        },
        {
          commitOffset: '2',
        },
      ],
    });
  });
});
