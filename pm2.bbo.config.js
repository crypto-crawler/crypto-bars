/* eslint-disable @typescript-eslint/explicit-function-return-type, @typescript-eslint/no-var-requires */
const exchangePairs = require('./exchange_pairs');

const PAIRS = (process.env.PAIRS || ' ').split(' ').filter((x) => x);

const apps = [];

Object.keys(exchangePairs).forEach((exchange) => {
  Object.keys(exchangePairs[exchange]).forEach((marketType) => {
    let pairs = exchangePairs[exchange][marketType];
    if (PAIRS.length > 0) {
      pairs = pairs.filter((x) => PAIRS.includes(x));
    }
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
