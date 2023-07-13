import { Kafka, Partitioners } from 'kafkajs';

const TOPIC = 'example-topic';

async function main() {
  const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:29092'],
  });

  const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner });

  await producer.connect();

  await producer.send({
    topic: TOPIC,
    messages: [
      // { value: 'A', partition: 0 },
      { value: 'B', partition: 0 },
      { value: 'B', partition: 1 },
    ],
  });

  await producer.disconnect();
}

main().catch((err) => console.log(err));
