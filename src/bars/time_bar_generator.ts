import { BboMsg } from 'coin-bbo';
import { TradeMsg } from 'crypto-crawler';
import { BarGenerator } from './bar_generator';

// eslint-disable-next-line import/prefer-default-export
export class TimeBarGenerator extends BarGenerator {
  private tradeMsges: TradeMsg[] = [];

  private bboMsges: BboMsg[] = [];

  private timestamp = -1; // Current bar timestamp

  constructor(barSize: number) {
    super('TimeBar', barSize);
  }

  protected appendTradeMsg(msg: TradeMsg): void {
    const timestampThreshold =
      (Math.floor(msg.timestamp / (1000 * this.barSize)) + 1) * 1000 * this.barSize;

    // Init current bar timestamp with first tick's boundary timestamp
    if (this.timestamp === -1) {
      this.timestamp =
        (Math.floor(msg.timestamp / (1000 * this.barSize)) + 1) * 1000 * this.barSize;
    } else if (this.timestamp < timestampThreshold) {
      // Current tick's bar timestamp differs from current bar timestamp
      const barMsg = this.convertToBarMsg(this.timestamp, this.tradeMsges, this.bboMsges);

      if (barMsg !== undefined) {
        this.emit('bar', barMsg);
      }

      this.timestamp = timestampThreshold;
      this.tradeMsges = [];
      this.bboMsges = [];
    }

    this.tradeMsges.push(msg);
  }

  protected appendBboMsg(msg: BboMsg): void {
    this.bboMsges.push(msg as BboMsg);
  }
}
