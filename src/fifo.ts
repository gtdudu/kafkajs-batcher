interface Item {
  next: Item | null;
  value: any;
}

export class Fifo {
  private _head: Item | undefined | null;
  private _last: Item | undefined | null;
  private _len: number;

  constructor() {
    this._head = null;
    this._last = null;
    this._len = 0;
  }

  enqueue(value: any) {
    this._len++;

    const item = {
      next: null,
      value,
    };

    if (this._last == null) {
      this._head = item;
    } else {
      this._last.next = item;
    }

    this._last = item;
  }

  dequeue(): any {
    if (this._head == null) {
      return null;
    }

    this._len--;

    const item = this._head;
    this._head = item.next;

    if (this._head == null) {
      this._last = null;
    }

    return item.value;
  }

  peek(): any {
    return this?._head?.value;
  }

  peekLast(): any {
    return this?._last?.value;
  }

  len(): number {
    return this._len;
  }
}
