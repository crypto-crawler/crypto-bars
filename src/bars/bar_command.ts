/* eslint-disable max-classes-per-file */
/* eslint-disable @typescript-eslint/no-unused-vars */
import { strict as assert } from 'assert';
import { BboMsg } from 'coin-bbo';
import { Msg, SUPPORTED_EXCHANGES, TradeMsg } from 'crypto-crawler';
import { IndexTickerMsg } from 'crypto-crawler/dist/crawler/okex';
import { MarketType, MARKET_TYPES } from 'crypto-markets';
import path from 'path';
import yargs from 'yargs';
import {
  DOLLAR_BAR_SIZES,
  TICK_BAR_SIZES,
  TIME_BAR_SIZES,
  VOLUME_BAR_SIZES,
} from '../config/hyper_parameters';
import { REDIS_TOPIC_PREFIX, REDIS_TOPIC_SPOT_INDEX_PRICE } from '../crawlers/common';
import {
  createLogger,
  FileMsgWriter,
  Logger,
  Publisher,
  Subscriber,
  TimePriorityQueue,
} from '../utils';
import { calcVOIandOIR } from './common';
import { BarGenerator, BarMsg, BarType, TimeBarGenerator, VolumeBarGenerator } from './index';

class AllInOneBarGenerator {
  private logger: Logger;

  // base -> barTypes
  private barTypes: { [key: string]: readonly { barType: BarType; barSize: number }[] };

  private rootDir: string;

  // key = exchange-marketType-pair-rawPair-barType-barSize
  private barGenerators = new Map<string, BarGenerator>();

  private fileWriters = new Map<string, FileMsgWriter>();

  private publisher = new Publisher<BarMsg>(process.env.REDIS_URL || 'redis://localhost:6379');

  constructor(exchange: string, marketType: MarketType, rootDir: string) {
    this.logger = createLogger(`bar-${exchange}-${marketType}`);
    this.rootDir = rootDir;

    this.barTypes = {};

    Object.keys(TIME_BAR_SIZES).forEach((base) => {
      this.barTypes[base] = TIME_BAR_SIZES[base].map((second) => ({
        barType: 'TimeBar',
        barSize: second,
      }));
    });
    Object.keys(TICK_BAR_SIZES).forEach((base) => {
      this.barTypes[base] = this.barTypes[base].concat(
        TICK_BAR_SIZES[base].map((numTicks) => ({
          barType: 'TickBar',
          barSize: numTicks,
        })),
      );
    });
    Object.keys(VOLUME_BAR_SIZES).forEach((base) => {
      this.barTypes[base] = this.barTypes[base].concat(
        VOLUME_BAR_SIZES[base].map((volume) => ({
          barType: 'VolumeBar',
          barSize: volume,
        })),
      );
    });
    Object.keys(DOLLAR_BAR_SIZES).forEach((base) => {
      this.barTypes[base] = this.barTypes[base].concat(
        DOLLAR_BAR_SIZES[base].map((dollar) => ({
          barType: 'DollarBar',
          barSize: dollar,
        })),
      );
    });
    console.info(this.barTypes);
  }

  public append(msg: Msg): void {
    const base = msg.pair.split('_')[0];
    if (!base) {
      this.logger.error(`${base} does NOT have bar configuration`);
      return;
    }

    this.barTypes[base].forEach(({ barType, barSize }) => {
      const { exchange, marketType, pair, rawPair } = msg;

      const key = `${msg.exchange}-${msg.marketType}-${msg.pair}-${msg.rawPair}-${barType}-${barSize}`;

      if (!this.fileWriters.has(key)) {
        this.fileWriters.set(
          key,
          new FileMsgWriter(
            marketType === 'Spot' || marketType === 'Swap'
              ? path.join(
                  this.rootDir,
                  barType,
                  barSize.toString(),
                  `${exchange}-${marketType}`,
                  pair,
                )
              : path.join(
                  this.rootDir,
                  barType,
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
        const barGenerator =
          barType === 'TimeBar'
            ? new TimeBarGenerator(barSize)
            : new VolumeBarGenerator(barType, barSize);

        barGenerator.on('bar', (barMsg) => {
          fileWriter.write([barMsg]);

          this.publisher.publish(
            `${REDIS_TOPIC_PREFIX}:${barType}:${pair}:${marketType}:${barSize}`,
            barMsg,
          );
        });

        this.barGenerators.set(key, barGenerator);
      }

      this.barGenerators.get(key)!.append(msg);
    });
  }
}

const commandModule: yargs.CommandModule = {
  command: `bar <exchange> <marketType>`,
  describe: `Build bars`,
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
    console.info(params);
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

    const allInOneBarGenerator = new AllInOneBarGenerator(
      params.exchange,
      params.marketType,
      path.join(process.env.DATA_DIR!, 'bars'),
    );
    const tradeQueue = new TimePriorityQueue<TradeMsg>((x, y) =>
      x.timestamp !== y.timestamp
        ? x.timestamp < y.timestamp
        : x.trade_id.localeCompare(y.trade_id) < 0,
    );

    const tradeSubscriber = new Subscriber(
      async (tradeMsg: TradeMsg) => {
        tradeQueue.offer(tradeMsg);

        // eslint-disable-next-line no-shadow
        tradeQueue.pollAll().forEach((tradeMsg) => {
          if (!priceIndexMap.has(tradeMsg.pair)) return;
          const latestPrice = priceIndexMap.get(tradeMsg.pair)!;

          const basis = tradeMsg.price - latestPrice;

          // eslint-disable-next-line no-param-reassign
          ((tradeMsg as unknown) as { basis: number }).basis = basis;

          allInOneBarGenerator.append(tradeMsg);
        });
      },
      `${REDIS_TOPIC_PREFIX}:trade-${params.exchange}-${params.marketType}`,
      REDIS_URL,
    );
    tradeSubscriber.run();

    // rawPair -> BboMsg
    const prevBboMsgMap = new Map<string, BboMsg>();
    const bboQueue = new TimePriorityQueue<BboMsg>();

    const bboSubscriber = new Subscriber(
      async (bboMsg: BboMsg) => {
        bboQueue.offer(bboMsg);

        // eslint-disable-next-line no-shadow
        bboQueue.pollAll().forEach((bboMsg) => {
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

          allInOneBarGenerator.append(bboMsg);
        });
      },
      `${REDIS_TOPIC_PREFIX}:bbo-${params.exchange}-${params.marketType}`,
      REDIS_URL,
    );

    await bboSubscriber.run();
  },
};

export default commandModule;
