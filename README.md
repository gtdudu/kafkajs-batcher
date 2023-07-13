# @gtdudu/kafkajs-batcher

Control batch size for kafkajs consumers.  
Takes care of commiting offsets properly.  
Written in typescript.  


# Table of contents

- [Table of contents](#table-of-contents)
  - [Usage](#usage)
  - [Develop](#develop)
    - [Prerequisites](#prerequisites)
    - [Install](#install)
  - [Tests](#tests)
    - [Integration tests](#integration-tests)
  - [Contribute](#contribute)
    - [Publish](#publish)

## Usage

### Install

```sh
npm install @gtdudu/kafkajs-batcher --save
```

### Initialize

```mjs
import { Batcher } from '@gtdudu/kafkajs-batcher'

// ...
// init everything then

const batcher = new Batcher({
  // return of kafka.consumer(config)
  consumer,
  // function executed when a batch is flushed
  handler: ({ messages }) => {
    // your code here
  }, 
  // how many messages before flushing batch
  batchSize: 100,
  // after this, an incomplete batch will be flushed anyway
  // counter is reset every time a message is pushed
  maxIdleMs: 2000,  
})

await consumer.run({
  eachBatchAutoResolve: false, // this must be false
  autoCommit: false, // this must be false
  eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
    const { topic, partition, messages } = batch;

    // most likely needed if handler takes a while
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
      // handle error
    }

    clearInterval(interval);
  },
});


// down the road
// best to stop feeding new messages to batcher if we're stopping
  consumer.pause([{ topic: TOPIC }]); 
  // any handler already in progress will be awaited
  // other messages will be discarded.. until module is restarted
  await batcher.stop();
  // more clean up here 


```

### Default behavior

1. Messages sharing the same topic/partition will be batched together.  
The `storeKey` used is `${topic}-${partition}`.

2. One message will count as one element in the batch. 

Both those behaviours can be changed by providing your own `getMessageInfo` function to Batcher constructor

### getMessageInfo

Receives message, topic and partition and must return an object with the following properties
- `storeKey: string`: grouping is based on this.
- `count: number`: any integer > 0

When the sum of `count` for a given `storeKey` reaches `batchSize` or `maxIdleMs` has elapsed, batch is flushed and `handler` is called

**WARNING**   

Providing your own `getMessageInfo` is an adavanced use case.  

Depending on your `storeKey` for a given partition, offsets may not always be committed after batch is flushed if there are still lower offsets pending. To prevent re consumption in case of module restart, consumed offsets must be tracked somehow. 

In order to do this, you can pass an `offsetDeduper` to `Batcher` constructor.

Two kind of dedupers are exported by this module: 
- `const { FsOffsetDeduper } = require('@gtdudu/kafkajs-batcher')`  
  Saves consumed offsets to a file in tmp os directory. 
- `const { RedisOffsetDeduper } = require('@gtdudu/kafkajs-batcher')`  
  Stores consumed offsets in redis sorted sets.   
  `RedisOffsetDeduper` constructor expects a `redisClient` which can come from either `ioredis` or node `redis`  
  Use this when deploying on kubernetes cluster with multiple replicas. 

Both dedupers store as little as possible. 

### Logs

To get extensive logs run your project with: `DEBUG=gtdudu:*`

## Examples

* consumer: 
```
node examples/consumer.mjs
```
* producer
```
node examples/producer.mjs
```


## Develop

### Prerequisites

- [nvm](https://github.com/nvm-sh/nvm)
- [docker-compose](https://docs.docker.com/compose/)

### Install

```sh
nvm use
npm install
```

## Tests

### Integration tests

```
docker-compose up
npm test
```
