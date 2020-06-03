import { strict as assert } from 'assert';
import { BboMsg } from 'coin-bbo';
import { Msg, SUPPORTED_EXCHANGES, TradeMsg } from 'crypto-crawler';
import { IndexTickerMsg } from 'crypto-crawler/dist/crawler/okex';
import { MarketType, MARKET_TYPES } from 'crypto-markets';
import path from 'path';
import yargs from 'yargs';
import { TIME_BAR_SIZES } from '../config/hyper_parameters';
import { REDIS_TOPIC_PREFIX, REDIS_TOPIC_SPOT_INDEX_PRICE } from '../crawlers/common';
import { FileMsgWriter, Publisher, Subscriber } from '../utils';
import { calcVOIandOIR } from './common';
import { BarMsg, TimeBarGenerator } from './index';

class MultiTimeBarGenerator {
  private barSizes: number[];

  private outputDir: string;

  // key = exchange-marketType-pair-rawPair
  private barGenerators = new Map<string, TimeBarGenerator>();

  private fileWriters = new Map<string, FileMsgWriter>();

  private publisher = new Publisher<BarMsg>(process.env.REDIS_URL || 'redis://localhost:6379');

  constructor(barSizes: number[], outputDir: string) {
    this.barSizes = barSizes;
    this.outputDir = outputDir;
  }

  public append(msg: Msg): void {
    this.barSizes.forEach((barSize) => {
      const { exchange, marketType, pair, rawPair } = msg;

      const key = `${msg.exchange}-${msg.marketType}-${msg.pair}-${msg.rawPair}-${barSize}`;

      if (!this.fileWriters.has(key)) {
        this.fileWriters.set(
          key,
          new FileMsgWriter(
            marketType === 'Spot' || marketType === 'Swap'
              ? path.join(this.outputDir, barSize.toString(), `${exchange}-${marketType}`, pair)
              : path.join(
                  this.outputDir,
                  barSize.toString(),
                  `${exchange}-${marketType}`,
                  pair,
                  rawPair,
                ),
          ),
        );
      }
      const fileWriter = this.fileWriters.get(key)!;

      if (!this.barGenerators.has(key)) {
        const barGenerator = new TimeBarGenerator(barSize);

        barGenerator.on('bar', (barMsg) => {
          fileWriter.write([barMsg]);

          this.publisher.publish(
            `${REDIS_TOPIC_PREFIX}:TimeBar:${pair}:${marketType}:${barSize}`,
            barMsg,
          );
        });

        this.barGenerators.set(key, barGenerator);
      } else {
        this.barGenerators.get(key)!.append(msg);
      }
    });
  }
}

const commandModule: yargs.CommandModule = {
  command: 'time_bar <exchange> <marketType>',
  describe: 'Build time bars',
  // eslint-disable-next-line no-shadow
  builder: (yargs) =>
    yargs
      .positional('exchange', {
        choices: SUPPORTED_EXCHANGES,
        type: 'string',
        demandOption: true,
      })
      .positional('marketType', {
        choices: MARKET_TYPES,
        type: 'string',
        demandOption: true,
      }),
  handler: async (argv) => {
    const params: {
      exchange: string;
      marketType: MarketType;
    } = argv as any; // eslint-disable-line @typescript-eslint/no-explicit-any
    assert.ok(process.env.DATA_DIR, 'Please define a DATA_DIR environment variable in .envrc');

    const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

    // pair -> price
    const priceIndexMap = new Map<string, number>();

    const spotIndexPriceSubscriber = new Subscriber(
      async (tickerMsg: IndexTickerMsg) => {
        priceIndexMap.set(tickerMsg.pair, tickerMsg.last);
      },
      REDIS_TOPIC_SPOT_INDEX_PRICE,
      REDIS_URL,
    );
    spotIndexPriceSubscriber.run();

    await new Promise((resolve) => setTimeout(resolve, 5000));
    // params.pairs.forEach((pair) => {
    //   assert.ok(priceIndexMap.get(pair)! > 0);
    // });

    const multiTimeBarGenerator = new MultiTimeBarGenerator(
      TIME_BAR_SIZES,
      path.join(process.env.DATA_DIR!, 'TimeBar'),
    );

    const tradeSubscriber = new Subscriber(
      async (tradeMsg: TradeMsg) => {
        if (!priceIndexMap.has(tradeMsg.pair)) return;
        const latestPrice = priceIndexMap.get(tradeMsg.pair)!;

        const basis = tradeMsg.price - latestPrice;

        // eslint-disable-next-line no-param-reassign
        ((tradeMsg as unknown) as { basis: number }).basis = basis;

        multiTimeBarGenerator.append(tradeMsg);
      },
      `${REDIS_TOPIC_PREFIX}:trade-${params.exchange}-${params.marketType}`,
      REDIS_URL,
    );
    tradeSubscriber.run();

    // rawPair -> BboMsg
    const prevBboMsgMap = new Map<string, BboMsg>();

    const bboSubscriber = new Subscriber(
      async (bboMsg: BboMsg) => {
        if (!prevBboMsgMap.has(bboMsg.rawPair)) {
          prevBboMsgMap.set(bboMsg.rawPair, bboMsg);
          return;
        }
        const prevBbo = prevBboMsgMap.get(bboMsg.rawPair)!;

        if (!priceIndexMap.has(bboMsg.pair)) return;
        const latestPrice = priceIndexMap.get(bboMsg.pair)!;

        const basis = (bboMsg.askPrice + bboMsg.bidPrice) / 2 - latestPrice;
        const spread = Math.abs(bboMsg.askPrice - bboMsg.bidPrice);

        // calculate technical indicators on the fly
        const tmp = (bboMsg as unknown) as {
          basis: number;
          voi: number;
          oir: number;
          // divided by spread
          basis_norm: number;
          voi_norm: number;
          oir_norm: number;
        };
        const { voi, oir } = calcVOIandOIR(prevBbo, bboMsg);
        tmp.basis = basis;
        tmp.voi = voi;
        tmp.oir = oir;
        tmp.basis_norm = basis / spread;
        tmp.voi_norm = voi / spread;
        tmp.oir_norm = oir / spread;

        multiTimeBarGenerator.append(bboMsg);
      },
      `${REDIS_TOPIC_PREFIX}:bbo-${params.exchange}-${params.marketType}`,
      REDIS_URL,
    );

    await bboSubscriber.run();
  },
};

export default commandModule;
