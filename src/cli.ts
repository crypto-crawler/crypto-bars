#!/usr/bin/env node
import chalk from 'chalk';
import figlet from 'figlet';
import yargs from 'yargs';
import timeBarModule from './bars/time_bar';
import {
  dollarBarCommandModule,
  tickBarCommandModule,
  volumeBarCommandModule,
} from './bars/volume_bar';
import crawlerBboModule from './crawlers/crawler_bbo';
import crawlerSpotIndexPriceModule from './crawlers/crawler_spot_index_price';
import crawlerTradeModule from './crawlers/crawler_trade';

console.info(chalk.green(figlet.textSync('Crypto Trader')));

// eslint-disable-next-line no-unused-expressions
yargs
  .command(crawlerBboModule)
  .command(crawlerSpotIndexPriceModule)
  .command(crawlerTradeModule)
  .command(timeBarModule)
  .command(tickBarCommandModule)
  .command(volumeBarCommandModule)
  .command(dollarBarCommandModule)
  .wrap(null)
  .demandCommand(1, '').argv;
