import debug from 'debug';
import EventEmitter from 'events';
import { Fifo } from './fifo';

const log = debug('gtdudu:queue');

type PromiseResolve<T> = (value?: T | PromiseLike<T>) => void;

type QueueHandlerFunction = (a: any) => Promise<any>;
interface QueueConstructorOptions {
  handler: QueueHandlerFunction;
}

export class Queue extends EventEmitter {
  private handler: QueueHandlerFunction;
  private fifo: Fifo;
  private running: number;
  private inProgress: Promise<any> | null;
  private stopping: boolean | undefined;

  constructor(opts: QueueConstructorOptions) {
    super();

    if (!(typeof opts?.handler === 'function')) {
      throw new Error('Queue.handler must be a function');
    }

    this.inProgress = null;
    this.fifo = new Fifo();
    this.running = 0;
    this.handler = opts.handler;

    this.on('run', () => this.run());
    this.on('dequeue', async () => this.dequeue());
  }

  async enqueue(task: any): Promise<any> {
    return new Promise((resolve, reject) => {
      this.fifo.enqueue({ task, resolve, reject });
      this.emit('run');
    });
  }

  async stop() {
    this.stopping = true;

    if (this.inProgress) {
      log(`dequeue in progress - await end of ongoing task`);
      await this.inProgress;
    }

    while (this.fifo.len()) {
      const { task } = this.fifo.dequeue();
      log('clearing task', { task });
    }
  }

  private async dequeue(): Promise<undefined> {
    let resolver: PromiseResolve<undefined> | undefined;
    this.inProgress = new Promise((resolve) => {
      resolver = resolve;
    });

    const { task, resolve, reject } = this.fifo.dequeue();

    try {
      const res = await this.handler(task);
      resolve(res);
    } catch (error) {
      reject(error);
    }
    this.running--;

    if (resolver) resolver();
    this.inProgress = null;

    this.emit('run');
  }

  private run(): undefined {
    if (this.stopping || this.running || !this.fifo.peek()) return;
    this.running++;
    this.emit('dequeue');
  }
}
