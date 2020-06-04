/* eslint-disable @typescript-eslint/explicit-function-return-type, @typescript-eslint/no-var-requires */
const assert = require('assert').strict;
const exchangePairs = require('./exchange_pairs');

assert.ok(process.env.PAIRS, 'Please define a PAIRS environment variable in .envrc');
const PAIRS = process.env.PAIRS.split(' ');
assert.ok(PAIRS.length > 0);

const apps = [];

Object.keys(exchangePairs).forEach((exchange) => {
  Object.keys(exchangePairs[exchange]).forEach((marketType) => {
    const pairs = exchangePairs[exchange][marketType].filter((x) => PAIRS.includes(x));
    if (pairs.length <= 0) return;

    const app = {
      name: `crawler-bbo-${exchange}-${marketType}`,
      script: 'dist/cli.js',
      args: `crawler_bbo ${exchange} ${marketType} --pairs ${pairs.join(' ')}`,
      instances: 1,
      autorestart: true,
      watch: false,
    };

    apps.push(app);
  });
});

apps.splice(0, apps.length); // disable BBO crawlers

module.exports = {
  apps,
};
