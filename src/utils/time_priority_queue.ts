/* eslint-disable max-classes-per-file */
import FastPriorityQueue from 'fastpriorityqueue';

export const TIME_OUT = 30 * 1000; // cache all message within 30s before poll()

interface TimestampMsg {
  exchange: string;
  pair: string;
  timestamp: number;
}

// eslint-disable-next-line import/prefer-default-export
export class TimePriorityQueue<T extends TimestampMsg> {
  private queue: FastPriorityQueue<T>;

  constructor(comparator = (x: T, y: T): boolean => x.timestamp < y.timestamp) {
    this.queue = new FastPriorityQueue<T>(comparator);
  }

  public offer(msg: T): void {
    if (msg.timestamp + TIME_OUT < Date.now()) return;
    this.queue.add(msg);
  }

  public pollAll(): readonly T[] {
    const result: T[] = [];

    let msg: T | undefined;
    do {
      msg = this.poll();
      if (msg) result.push(msg);
    } while (msg);

    return result;
  }

  private poll(): T | undefined {
    const top = this.queue.peek();
    if (!top) return undefined;
    if (top.timestamp + TIME_OUT >= Date.now()) return undefined; // still fresh, hide it from outside

    return this.queue.poll();
  }
}
