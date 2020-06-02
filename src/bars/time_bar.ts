import { strict as assert } from 'assert';
import { BboMsg } from 'coin-bbo';
import { Msg, SUPPORTED_EXCHANGES, TradeMsg } from 'crypto-crawler';
import { IndexTickerMsg } from 'crypto-crawler/dist/crawler/okex';
import { MarketType, MARKET_TYPES } from 'crypto-markets';
import path from 'path';
import yargs from 'yargs';
import { TIME_BAR_SIZES } from '../config/hyper_parameters';
import { REDIS_TOPIC_PREFIX, REDIS_TOPIC_SPOT_INDEX_PRICE } from '../crawlers/common';
import { createLogger, FileMsgWriter, Logger, Publisher, Subscriber } from '../utils';
import { TimeBarMsg } from './bar_msg';
import { aggregateBbo, aggregateTrade, calcVOIandOIR } from './common';

const INTERVAL_NAME_SECOND_MAP: { [key: string]: number } = {
  // '1s': 1,  // 1 second time bars have too many empty bars, the empty ratio can be above 70%, so it is removed
  '10s': 10,
  '1m': 60,
  '3m': 180,
  '5m': 300,
  '15m': 900,
  '30m': 1800,
  '1H': 3600,
  '4H': 14400,
};

class TimeBarBuilder {
  private static N = 3; // keep N bars in memory

  private logger: Logger;

  private interval: string; // in second

  private dataDir: string;

  // key = exchange-marketType-pair-rawPair
  private barsTrade = new Map<number, Map<string, TradeMsg[]>>(); // N bars

  private barsBbo = new Map<number, Map<string, BboMsg[]>>(); // N bars

  private fileWriters = new Map<string, FileMsgWriter>();

  private publisher = new Publisher<TimeBarMsg>(process.env.REDIS_URL || 'redis://localhost:6379');

  constructor(interval: string, dataDir: string) {
    this.interval = interval;

    this.dataDir = dataDir;

    this.logger = createLogger(`time_bar-${interval}`);

    setInterval(this.scanOldBars.bind(this), INTERVAL_NAME_SECOND_MAP[this.interval] * 1000);
  }

  public append(msg: Msg): void {
    assert.ok(Number.isInteger(msg.timestamp));
    if (msg.timestamp.toString().length !== 13) {
      this.logger.error('msg.timestamp is not 13 length');
      this.logger.error(JSON.stringify(msg));
      process.exit(1);
    }

    const intervalSeconds = INTERVAL_NAME_SECOND_MAP[this.interval];
    const currentBar = Math.floor(Date.now() / (1000 * intervalSeconds)) * 1000 * intervalSeconds;
    const oldestBar = currentBar - (TimeBarBuilder.N - 1) * 1000 * intervalSeconds;
    const msgBar = Math.floor(msg.timestamp / (1000 * intervalSeconds)) * 1000 * intervalSeconds;

    // msgBar should be in [oldestBar, currentBar] range
    if (msgBar > currentBar) {
      this.logger.error('This msg comes from future, impossible');
      this.logger.error(msg);
      return;
    }
    if (msgBar < oldestBar) {
      this.logger.info(
        `This msg arrives ${Date.now() - msg.timestamp} milliseconds late, ignore it`,
      );
      this.logger.info(msg);
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

  private appendTradeMsg(msg: TradeMsg): void {
    const intervalSeconds = INTERVAL_NAME_SECOND_MAP[this.interval];
    const msgBar = Math.floor(msg.timestamp / (1000 * intervalSeconds)) * 1000 * intervalSeconds;

    if (!this.barsTrade.has(msgBar)) {
      // create a new bar
      this.barsTrade.set(msgBar, new Map<string, TradeMsg[]>());
    }

    const barsMap = this.barsTrade.get(msgBar)!;
    assert.ok(barsMap);

    const key = `${msg.exchange}-${msg.marketType}-${msg.pair}-${msg.rawPair}`;
    if (!barsMap.has(key)) {
      barsMap.set(key, []);
    }

    barsMap.get(key)!.push(msg);
  }

  private appendBboMsg(msg: BboMsg): void {
    const intervalSeconds = INTERVAL_NAME_SECOND_MAP[this.interval];
    const msgBar = Math.floor(msg.timestamp / (1000 * intervalSeconds)) * 1000 * intervalSeconds;

    if (!this.barsBbo.has(msgBar)) {
      // create a new bar
      this.barsBbo.set(msgBar, new Map<string, BboMsg[]>());
    }

    const barsMap = this.barsBbo.get(msgBar)!;
    assert.ok(barsMap);

    const key = `${msg.exchange}-${msg.marketType}-${msg.pair}-${msg.rawPair}`;
    if (!barsMap.has(key)) {
      barsMap.set(key, []);
    }

    barsMap.get(key)!.push(msg);
  }

  private async scanOldBars(): Promise<void> {
    const intervalSeconds = INTERVAL_NAME_SECOND_MAP[this.interval];
    const currentBar = Math.floor(Date.now() / (1000 * intervalSeconds)) * 1000 * intervalSeconds;
    const oldestBar = currentBar - (TimeBarBuilder.N - 1) * 1000 * intervalSeconds;

    // scan old bars
    Array.from(this.barsTrade.keys())
      .concat(Array.from(this.barsBbo.keys()))
      .filter((x) => x < oldestBar)
      .sort((x, y) => x - y)
      .forEach((t) => {
        const tradeBar = this.barsTrade.get(t) || new Map<string, TradeMsg[]>();
        const bboBar = this.barsBbo.get(t) || new Map<string, BboMsg[]>();
        // Remove this bar
        this.barsTrade.delete(t);
        this.barsBbo.delete(t);

        const keys = new Set(Array.from(tradeBar.keys()).concat(Array.from(bboBar.keys())));
        keys.forEach((key) => {
          const { exchange, marketType, pair, rawPair } = (tradeBar.get(key) ||
            bboBar.get(key)!)[0];

          if (!this.fileWriters.has(key)) {
            this.fileWriters.set(
              key,
              new FileMsgWriter(
                marketType === 'Spot' || marketType === 'Swap'
                  ? path.join(this.dataDir, this.interval, `${exchange}-${marketType}`, pair)
                  : path.join(
                      this.dataDir,
                      this.interval,
                      `${exchange}-${marketType}`,
                      pair,
                      rawPair,
                    ),
              ),
            );
          }

          const snapshot: TimeBarMsg = {
            exchange,
            market_type: marketType,
            pair,
            raw_rair: rawPair,
            bar_type: 'TimeBar',
            interval: INTERVAL_NAME_SECOND_MAP[this.interval],
            timestamp: t,
            timestamp_end: t + INTERVAL_NAME_SECOND_MAP[this.interval] * 1000,
          };

          if (tradeBar.has(key)) {
            const { trade, trade_indicators } = aggregateTrade(
              tradeBar
                .get(key)!
                .sort((x, y) =>
                  x.timestamp !== y.timestamp
                    ? x.timestamp - y.timestamp
                    : x.trade_id.localeCompare(y.trade_id),
                ),
            );
            snapshot.trade = trade;
            snapshot.trade_indicators = trade_indicators;
          }
          if (bboBar.has(key)) {
            snapshot.bbo = aggregateBbo(bboBar.get(key)!.sort((x, y) => x.timestamp - y.timestamp));
          }

          this.publisher.publish(
            `${REDIS_TOPIC_PREFIX}:TimeBar:${pair}:${marketType}${this.interval}`,
            snapshot,
          );
          this.fileWriters.get(key)!.write([snapshot]);
        });
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

    const timeBarBuilders = TIME_BAR_SIZES.map(
      (barSize) => new TimeBarBuilder(barSize, path.join(process.env.DATA_DIR!, 'TimeBar')),
    );

    const tradeSubscriber = new Subscriber(
      async (tradeMsg: TradeMsg) => {
        if (!priceIndexMap.has(tradeMsg.pair)) return;
        const latestPrice = priceIndexMap.get(tradeMsg.pair)!;

        const basis = tradeMsg.price - latestPrice;

        // eslint-disable-next-line no-param-reassign
        ((tradeMsg as unknown) as { basis: number }).basis = basis;

        timeBarBuilders.forEach((x) => x.append(tradeMsg));
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

        timeBarBuilders.forEach((x) => x.append(bboMsg));
      },
      `${REDIS_TOPIC_PREFIX}:bbo-${params.exchange}-${params.marketType}`,
      REDIS_URL,
    );

    await bboSubscriber.run();
  },
};

export default commandModule;
