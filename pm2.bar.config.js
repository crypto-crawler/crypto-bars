/* eslint-disable @typescript-eslint/explicit-function-return-type, @typescript-eslint/no-var-requires */
const assert = require('assert');
const exchangePairs = require('./exchange_pairs');

assert.ok(process.env.DATA_DIR, 'Please define the DATA_DIR environment variable in .envrc');

const apps = [];

Object.keys(exchangePairs).forEach((exchange) => {
  Object.keys(exchangePairs[exchange]).forEach((marketType) => {
    apps.push({
      name: `time_bar-${exchange}-${marketType}`,
      script: 'dist/cli.js',
      args: `time_bar ${exchange} ${marketType}`,
      instances: 1,
      autorestart: true,
      watch: false,
    });

    apps.push({
      name: `tick_bar-${exchange}-${marketType}`,
      script: 'dist/cli.js',
      args: `tick_bar ${exchange} ${marketType}`,
      instances: 1,
      autorestart: true,
      watch: false,
    });

    apps.push({
      name: `volume_bar-${exchange}-${marketType}`,
      script: 'dist/cli.js',
      args: `volume_bar ${exchange} ${marketType}`,
      instances: 1,
      autorestart: true,
      watch: false,
    });

    apps.push({
      name: `dollar_bar-${exchange}-${marketType}`,
      script: 'dist/cli.js',
      args: `dollar_bar ${exchange} ${marketType}`,
      instances: 1,
      autorestart: true,
      watch: false,
    });
  });
});

module.exports = {
  apps,
};
