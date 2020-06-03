// eslint-disable-next-line max-classes-per-file
import { strict as assert } from 'assert';
import { BboMsg } from 'coin-bbo';
import { Msg, TradeMsg } from 'crypto-crawler';
import { MarketType } from 'crypto-markets';
import path from 'path';
import { REDIS_TOPIC_PREFIX } from '../crawlers/common';
import { createLogger, FileMsgWriter, Logger, Publisher } from '../utils';
import { BarType, TimeBarMsg, VolumeBarMsg } from './bar_msg';
import { aggregateBbo, aggregateTrade } from './common';

// eslint-disable-next-line import/prefer-default-export
// for a single pair
export class VolumeBarBuilderSingle {
  private logger: Logger;

  private barType: BarType;

  private barSize: number;

  private fileWriter: FileMsgWriter;

  private publisher = new Publisher<TimeBarMsg>(process.env.REDIS_URL || 'redis://localhost:6379');

  private cur = 0; // current number of ticks, ETHs, BTCs or USDs, depends on barType

  private tradeMsges: TradeMsg[] = [];

  private bboMsges: BboMsg[] = [];

  constructor(
    exchange: string,
    marketType: MarketType,
    pair: string,
    rawPair: string,
    barType: 'TickBar' | 'VolumeBar' | 'DollarBar',
    barSize: number,
    outputDir: string,
  ) {
    this.barType = barType;
    this.barSize = barSize;

    this.fileWriter = new FileMsgWriter(
      marketType === 'Spot' || marketType === 'Swap'
        ? path.join(outputDir, `${exchange}-${marketType}`, pair, barSize.toString())
        : path.join(outputDir, `${exchange}-${marketType}`, pair, rawPair, barSize.toString()),
    );

    this.logger = createLogger(
      `${barType}-${barSize}-${exchange}-${marketType}-${pair}-${rawPair}`,
    );
  }

  public append(msg: Msg): void {
    assert.ok(Number.isInteger(msg.timestamp));
    if (msg.timestamp.toString().length !== 13) {
      this.logger.error('msg.timestamp is not 13 length');
      this.logger.error(JSON.stringify(msg));
      process.exit(1);
    }

    if (msg.channelType === 'Trade') {
      this.appendTradeMsg(msg as TradeMsg);
    } else if (msg.channelType === 'BBO') {
      this.appendBboMsg(msg as BboMsg);
    } else {
      // do nothing
    }
  }

  private appendTradeMsg(msg: TradeMsg): void {
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
      this.writeBar();
    }
  }

  private appendBboMsg(msg: BboMsg): void {
    this.bboMsges.push(msg);
  }

  // output a bar
  private async writeBar(): Promise<void> {
    if (this.tradeMsges.length <= 0) return;

    const { exchange, marketType, pair, rawPair } = this.tradeMsges[0];

    const barMsg: VolumeBarMsg = {
      exchange,
      market_type: marketType,
      pair,
      raw_rair: rawPair,
      bar_type: this.barType,
      interval: this.barSize,
      timestamp: this.tradeMsges[0].timestamp,
      timestamp_end: this.tradeMsges[this.tradeMsges.length - 1].timestamp,
    };

    const { trade, trade_indicators } = aggregateTrade(this.tradeMsges);
    barMsg.trade = trade;
    barMsg.trade_indicators = trade_indicators;

    if (this.bboMsges.length > 0) {
      barMsg.bbo = aggregateBbo(this.bboMsges);
    }

    this.publisher.publish(
      `${REDIS_TOPIC_PREFIX}:${this.barType}:${pair}:${marketType}:${this.barSize}`,
      barMsg,
    );
    this.fileWriter.write([barMsg]);

    // reset
    this.cur = 0;
    this.tradeMsges = [];
    this.bboMsges = [];
  }
}

export class VolumeBarBuilder {
  private barType: 'TickBar' | 'VolumeBar' | 'DollarBar';

  private barSizes: { [key: string]: number[] }; // base -> barSizes

  private outputDir: string;

  // key = exchange-marketType-pair-rawPair
  private barBuilders = new Map<string, VolumeBarBuilderSingle>();

  constructor(
    barType: 'TickBar' | 'VolumeBar' | 'DollarBar',
    barSizes: { [key: string]: number[] }, // base -> barSizes
    outputDir: string,
  ) {
    this.barType = barType;

    this.barSizes = barSizes;

    this.outputDir = outputDir;
  }

  public append(msg: Msg): void {
    this.barSizes[msg.pair.split('_')[0]].forEach((barSize) => {
      const key = `${msg.exchange}-${msg.marketType}-${msg.pair}-${msg.rawPair}-${barSize}`;

      if (!this.barBuilders.has(key)) {
        this.barBuilders.set(
          key,
          new VolumeBarBuilderSingle(
            msg.exchange,
            msg.marketType,
            msg.pair,
            msg.rawPair,
            this.barType,
            barSize,
            this.outputDir,
          ),
        );
      } else {
        this.barBuilders.get(key)!.append(msg);
      }
    });
  }
}
