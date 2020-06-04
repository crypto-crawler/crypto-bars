/* eslint-disable @typescript-eslint/explicit-function-return-type, @typescript-eslint/no-var-requires */
const assert = require('assert');
const exchangePairs = require('./exchange_pairs');

assert.ok(process.env.DATA_DIR, 'Please define the DATA_DIR environment variable in .envrc');

assert.ok(process.env.PAIRS, 'Please define the PAIRS environment variable in .envrc');
const PAIRS = process.env.PAIRS.split(' ');
assert.ok(PAIRS.length > 0);

const apps = [];

Object.keys(exchangePairs).forEach((exchange) => {
  Object.keys(exchangePairs[exchange]).forEach((marketType) => {
    const pairs = exchangePairs[exchange][marketType].filter((x) => PAIRS.includes(x));
    if (pairs.length <= 0) return;

    apps.push({
      name: `bar-${exchange}-${marketType}`,
      script: 'dist/cli.js',
      args: `bar ${exchange} ${marketType}`,
      instances: 1,
      autorestart: true,
      watch: false,
    });
  });
});

module.exports = {
  apps,
};
