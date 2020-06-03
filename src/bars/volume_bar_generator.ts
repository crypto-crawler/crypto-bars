// eslint-disable-next-line max-classes-per-file
import { BboMsg } from 'coin-bbo';
import { TradeMsg } from 'crypto-crawler';
import { BarGenerator } from './bar_generator';

// eslint-disable-next-line import/prefer-default-export
export class VolumeBarGenerator extends BarGenerator {
  private cur = 0; // current number of ticks, ETHs, BTCs or USDs, depends on barType

  private tradeMsges: TradeMsg[] = [];

  private bboMsges: BboMsg[] = [];

  protected appendTradeMsg(msg: TradeMsg): void {
    this.tradeMsges.push(msg);

    switch (this.barType) {
      case 'TickBar':
        this.cur += 1;
        break;
      case 'VolumeBar':
        this.cur += msg.quantity;
        break;
      case 'DollarBar':
        this.cur += msg.quantity * msg.price;
        break;
      default:
        throw new Error(`Unknown bar type: ${this.barType}`);
    }

    if (this.cur >= this.barSize) {
      if (this.tradeMsges.length <= 0) {
        this.emit('log', { level: 'warn', message: 'empty tradeMsges' });
        return;
      }

      const barMsg = this.convertToBarMsg(
        this.tradeMsges[0].timestamp,
        this.tradeMsges,
        this.bboMsges,
      );
      if (barMsg !== undefined) {
        this.emit('bar', barMsg);
      }

      // reset
      this.cur = 0;
      this.tradeMsges = [];
      this.bboMsges = [];
    }
  }

  protected appendBboMsg(msg: BboMsg): void {
    this.bboMsges.push(msg);
  }
}
