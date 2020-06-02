import { strict as assert } from 'assert';
import { BboMsg } from 'coin-bbo';
import { TradeMsg } from 'crypto-crawler';
import { IndexTickerMsg } from 'crypto-crawler/dist/crawler/okex';
import { MarketType, MARKET_TYPES } from 'crypto-markets';
import path from 'path';
import yargs from 'yargs';
import { DOLLAR_BAR_SIZES, TICK_BAR_SIZES, VOLUME_BAR_SIZES } from '../config/hyper_parameters';
import { REDIS_TOPIC_PREFIX, REDIS_TOPIC_SPOT_INDEX_PRICE } from '../crawlers/common';
import { Subscriber, TimePriorityQueue } from '../utils';
import { VolumeBarBuilder } from './bar_builder';
import { BarType } from './bar_msg';
import { calcVOIandOIR } from './common';

function createModule(barType: BarType): yargs.CommandModule {
  let commandName = 'unknown';
  let barSizes: { [key: string]: number[] } = {};

  switch (barType) {
    case 'TickBar':
      commandName = 'tick_bar';
      barSizes = TICK_BAR_SIZES;
      break;
    case 'VolumeBar':
      commandName = 'volume_bar';
      barSizes = VOLUME_BAR_SIZES;
      break;
    case 'DollarBar':
      commandName = 'dollar_bar';
      barSizes = DOLLAR_BAR_SIZES;
      break;
    default:
      throw new Error(`Unknown bar type: ${barType}`);
  }

  const commandModule: yargs.CommandModule = {
    command: `${commandName} <exchange> <marketType>`,
    describe: `Build ${commandName.split('_')[0]} bars`,
    // eslint-disable-next-line no-shadow
    builder: (yargs) =>
      yargs
        .positional('exchange', {
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

      const barBuilder = new VolumeBarBuilder(
        barType,
        barSizes,
        path.join(process.env.DATA_DIR!, barType),
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

            barBuilder.append(tradeMsg);
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

            barBuilder.append(bboMsg);
          });
        },
        `${REDIS_TOPIC_PREFIX}:bbo-${params.exchange}-${params.marketType}`,
        REDIS_URL,
      );

      await bboSubscriber.run();
    },
  };

  return commandModule;
}

export const tickBarCommandModule = createModule('TickBar');
export const volumeBarCommandModule = createModule('VolumeBar');
export const dollarBarCommandModule = createModule('DollarBar');
