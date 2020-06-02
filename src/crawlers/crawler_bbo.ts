import { strict as assert } from 'assert';
import crawl, { BboMsg } from 'coin-bbo';
import { SUPPORTED_EXCHANGES } from 'crypto-crawler';
import { MarketType, MARKET_TYPES } from 'crypto-markets';
import yargs from 'yargs';
import { createLogger, Heartbeat, Publisher } from '../utils';
import { calcRedisTopic } from './common';

const commandModule: yargs.CommandModule = {
  command: 'crawler_bbo <exchange> <marketType>',
  describe: 'BBO crawler',
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
      })
      .options({
        pairs: {
          type: 'array',
          demandOption: true,
        },
      }),
  handler: async (argv) => {
    const params: {
      exchange: string;
      marketType: MarketType;
      pairs: string[];
    } = argv as any; // eslint-disable-line @typescript-eslint/no-explicit-any
    assert.ok(params.pairs.length > 0);

    const logger = createLogger(`crawler-bbo-${params.exchange}-${params.marketType}`);
    const hearbeat = new Heartbeat(logger, 60);

    // BBO messages are very fast, it is recommended to use a local Redis server
    const publisher = new Publisher<BboMsg>(process.env.REDIS_URL || 'redis://localhost:6379');

    crawl(
      params.exchange,
      params.marketType,
      params.pairs,
      async (msg): Promise<void> => {
        assert.equal(msg.channelType, 'BBO');
        hearbeat.updateHeartbeat();

        const bboMsg = msg as BboMsg;
        publisher.publish(calcRedisTopic(bboMsg), bboMsg);
      },
    );
  },
};

export default commandModule;
