import debug from 'debug';
import { writeFile } from 'node:fs/promises';
import { readFileSync } from 'node:fs';
import _ from 'lodash';
import { tmpdir } from 'os';
import { join } from 'path';

import { CheckIfAlreadyConsumedParams, SetHasConsumedParams, OffsetDeduper } from './interfaces';

const log = debug('gtdudu:deduper:fs');

interface FsOffsetDeduperConstructorOptions {
  filepath: string;
}

export class FsOffsetDeduper implements OffsetDeduper {
  public store: {
    [topic: string]: {
      [partition: number]: {
        offsets: string[];
        commitOffset: string;
      };
    };
  };
  private filePath: string;

  constructor(opts?: FsOffsetDeduperConstructorOptions) {
    this.store = {};
    this.filePath = (opts && opts.filepath) || join(tmpdir(), '.deduper-store');

    log(`fsDeduper output file is ${this.filePath}`);
  }

  private async save() {
    const str = JSON.stringify(this.store);
    await writeFile(this.filePath, str);
    log('writed store to file', this.store);
  }

  public load() {
    const str = readFileSync(this.filePath);
    this.store = JSON.parse(str.toString());
    log('loaded store from file', this.store);
  }

  public async setHasConsumed({ messages, commitOptions }: SetHasConsumedParams): Promise<any> {
    log('setHasConsumed - before commitOptions', this.store);

    _.each(commitOptions, ({ topic, partition, offset }) => {
      _.set(this.store, [topic, partition, 'commitOffset'], offset);
      let currentOffsets = _.get(this.store, [topic, partition, 'offsets'], []);
      if (!_.size(currentOffsets)) return;
      currentOffsets = currentOffsets.filter((o) => BigInt(o) > BigInt(offset) - BigInt(1));
      _.set(this.store, [topic, partition, 'offsets'], currentOffsets);
    });

    log('setHasConsumed - after commitOptions', this.store);

    _.each(messages, ({ topic, partition, offset }) => {
      const commitOffset = _.get(this.store, [topic, partition, 'commitOffset']);

      log('setHasConsumed - message', { offset, commitOffset });

      if (commitOffset && BigInt(offset) <= BigInt(commitOffset) - BigInt(1)) return;

      let currentOffsets = _.get(this.store, [topic, partition, 'offsets']);
      currentOffsets = currentOffsets ? [...currentOffsets, offset] : [offset];
      _.set(this.store, [topic, partition, 'offsets'], currentOffsets);
    });

    await this.save();
  }

  public async checkIfAlreadyConsumed({ topic, partition, offset }: CheckIfAlreadyConsumedParams) {
    if (!_.get(this.store, [topic, partition, 'offsets'])) {
      return false;
    }

    return _.includes(this.store[topic][partition]['offsets'], offset);
  }
}
