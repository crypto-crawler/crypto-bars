import { BboMsg } from 'coin-bbo';
import { Msg, TradeMsg } from 'crypto-crawler';
import { BarGenerator } from './bar_generator';

// eslint-disable-next-line import/prefer-default-export
export class TimeBarGenerator extends BarGenerator {
  private static N = 3; // keep three bars in memory

  // timestamp -> array
  private barsTrade = new Map<number, TradeMsg[]>(); // N bars

  private barsBbo = new Map<number, BboMsg[]>(); // N bars

  private timer: NodeJS.Timeout;

  constructor(barSize: number) {
    super('TimeBar', barSize);

    this.timer = setInterval(this.scanOldBars.bind(this), this.barSize * 1000);
  }

  public append(msg: Msg): void {
    this.checkTimestamp(msg);

    const currentBar = Math.floor(Date.now() / (1000 * this.barSize)) * 1000 * this.barSize;
    const oldestBar = currentBar - (TimeBarGenerator.N - 1) * 1000 * this.barSize;
    const msgBar = Math.floor(msg.timestamp / (1000 * this.barSize)) * 1000 * this.barSize;

    // msgBar should be in [oldestBar, currentBar] range
    if (msgBar > currentBar) {
      this.emit('log', {
        level: 'warn',
        message: `${JSON.stringify(msg)} timestamp is greater than now, impossible`,
      });
      return;
    }
    if (msgBar < oldestBar) {
      this.emit('log', {
        level: 'warn',
        message: `${JSON.stringify(msg)} arrives ${
          Date.now() - msg.timestamp
        } milliseconds late, ignore it`,
      });
      return;
    }

    if (msg.channelType === 'Trade') {
      this.appendTradeMsg(msg as TradeMsg);
    } else if (msg.channelType === 'BBO') {
      this.appendBboMsg(msg as BboMsg);
    } else {
      // do nothing
    }
  }

  protected appendTradeMsg(msg: TradeMsg): void {
    const msgBarIndex = Math.floor(msg.timestamp / (1000 * this.barSize)) * 1000 * this.barSize;

    if (!this.barsTrade.has(msgBarIndex)) {
      // create a new bar
      this.barsTrade.set(msgBarIndex, []);
    }

    const tradeMsges = this.barsTrade.get(msgBarIndex)!;
    tradeMsges.push(msg);
  }

  protected appendBboMsg(msg: BboMsg): void {
    const msgBarIndex = Math.floor(msg.timestamp / (1000 * this.barSize)) * 1000 * this.barSize;

    if (!this.barsBbo.has(msgBarIndex)) {
      // create a new bar
      this.barsBbo.set(msgBarIndex, []);
    }

    const bboMsges = this.barsBbo.get(msgBarIndex)!;
    bboMsges.push(msg);
  }

  private scanOldBars(): void {
    const currentBarTimestamp =
      Math.floor(Date.now() / (1000 * this.barSize)) * 1000 * this.barSize;
    const oldestBarTimestamp = currentBarTimestamp - (TimeBarGenerator.N - 1) * 1000 * this.barSize;

    // scan old bars
    Array.from(this.barsTrade.keys())
      .filter((x) => x < oldestBarTimestamp)
      .sort((x, y) => x - y)
      .forEach((barTimestamp) => {
        const tradeMsges = this.barsTrade.get(barTimestamp) || [];
        const bboMsges = this.barsBbo.get(barTimestamp) || [];
        // Remove this bar
        this.barsTrade.delete(barTimestamp);
        this.barsBbo.delete(barTimestamp);

        const barMsg = this.convertToBarMsg(barTimestamp, tradeMsges, bboMsges);

        if (barMsg !== undefined) {
          this.emit('bar', barMsg);
        }
      });

    // scan old BBO messages
    Array.from(this.barsBbo.keys())
      .filter((x) => x < oldestBarTimestamp)
      .forEach((barTimestamp) => {
        this.barsBbo.delete(barTimestamp);
      });
  }

  public close(): void {
    clearInterval(this.timer);
    super.close();
  }
}
