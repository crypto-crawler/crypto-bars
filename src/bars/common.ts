import { strict as assert } from 'assert';
import { BboMsg } from 'coin-bbo';
import { TradeMsg } from 'crypto-crawler';
import _ from 'lodash';
import { AggregateMsg, BboIndicators, TradeAggregateMsg, TradeIndicators } from './bar_msg';

export function aggregate(nums: readonly number[]): AggregateMsg {
  assert.ok(nums.length > 0);
  const open = nums[0];
  const close = nums[nums.length - 1];

  nums = [...nums].sort((x, y) => x - y); // eslint-disable-line no-param-reassign

  const high = nums[nums.length - 1];
  const low = nums[0];

  const mean = _.mean(nums);

  const mid = Math.ceil(nums.length / 2);

  const median = nums.length % 2 === 0 ? (nums[mid] + nums[mid - 1]) / 2 : nums[mid - 1];

  return {
    open,
    high,
    low,
    close,
    mean,
    median,
  };
}

// forked from mlfinlab/util/fast_ewma.py
export function ewma(arr_in: Float32Array, window: number): Float32Array {
  assert.ok(Number.isInteger(window));
  const arr_length = arr_in.length;
  const ewma_arr = new Float32Array(arr_in.length);
  const alpha = 2 / (window + 1);
  let weight = 1;
  let ewma_old = arr_in[0];
  ewma_arr[0] = ewma_old;
  for (let i = 1; i < arr_length; i += 1) {
    weight += (1 - alpha) ** i;
    ewma_old = ewma_old * (1 - alpha) + arr_in[i];
    ewma_arr[i] = ewma_old / weight;
  }

  return ewma_arr;
}

export function aggregateTrade(
  trades: readonly TradeMsg[],
): { trade: TradeAggregateMsg; trade_indicators: TradeIndicators } {
  const priceOHLC: AggregateMsg = aggregate(trades.map((x) => x.price));

  const volume = _.sum(trades.map((t) => t.quantity));
  const volume_sell = _.sum(trades.filter((t) => t.side).map((t) => t.quantity));
  const volume_buy = _.sum(trades.filter((t) => !t.side).map((t) => t.quantity));
  const volume_quote = _.sum(trades.map((t) => t.quantity * t.price));
  const volume_quote_sell = _.sum(trades.filter((t) => t.side).map((t) => t.quantity * t.price));
  const volume_quote_buy = _.sum(trades.filter((t) => !t.side).map((t) => t.quantity * t.price));

  const trade: TradeAggregateMsg = {
    ...priceOHLC,
    volume,
    volume_sell,
    volume_buy,
    volume_quote,
    volume_quote_sell,
    volume_quote_buy,

    vwap: volume_quote / volume,

    count: trades.length,
    count_sell: trades.filter((t) => t.side).length,
    count_buy: trades.filter((t) => !t.side).length,
  };

  const trade_basis = aggregate(trades.map((t) => ((t as unknown) as { basis: number }).basis));
  const basis_vw =
    _.sum(trades.map((t) => ((t as unknown) as { basis: number }).basis * t.quantity)) / volume;

  return {
    trade,
    trade_indicators: {
      basis: trade_basis,
      basis_vw,
      vpin: Math.abs(volume_buy - volume_sell) / (volume_sell + volume_sell),
    },
  };
}

export function aggregateBbo(bboMsges: readonly BboMsg[]): BboIndicators {
  const vwBid = _.sum(bboMsges.map((x) => x.bidPrice * x.bidQuantity));
  const vwAsk = _.sum(bboMsges.map((x) => x.askPrice * x.askQuantity));

  const result = {
    bid: aggregate(bboMsges.map((msg) => msg.bidPrice)),

    ask: aggregate(bboMsges.map((msg) => msg.askPrice)),

    mid: aggregate(bboMsges.map((msg) => (msg.bidPrice + msg.askPrice) / 2)),

    count: bboMsges.length,

    vw_spread_global: (vwBid - vwAsk) / (vwBid + vwAsk),
    // spread = askPrice - bidPrice
    spread: aggregate(bboMsges.map((msg) => msg.askPrice - msg.bidPrice)),
    vw_spread: aggregate(
      bboMsges.map(
        (msg) =>
          (msg.askPrice * msg.askQuantity - msg.bidPrice * msg.bidQuantity) /
          (msg.askPrice * msg.askQuantity + msg.bidPrice * msg.bidQuantity),
      ),
    ),

    basis: aggregate(bboMsges.map((t) => ((t as unknown) as { basis: number }).basis)),
    // volume order imbalance
    voi: aggregate(bboMsges.map((t) => ((t as unknown) as { voi: number }).voi)),
    // Order Imbalance Ratio
    oir: aggregate(bboMsges.map((t) => ((t as unknown) as { oir: number }).oir)),

    basis_norm: aggregate(
      bboMsges.map((t) => ((t as unknown) as { basis_norm: number }).basis_norm),
    ),
    voi_norm: aggregate(bboMsges.map((t) => ((t as unknown) as { voi_norm: number }).voi_norm)),
    oir_norm: aggregate(bboMsges.map((t) => ((t as unknown) as { oir_norm: number }).oir_norm)),
  };

  return result;
}

// Volume Order Imbalance
export function calcVOIandOIR(prev: BboMsg, cur: BboMsg): { voi: number; oir: number } {
  const EPSILON = 0.000000001;

  let vb = 0;
  // two float numbers are equal
  if (Math.abs(1 - cur.bidPrice / prev.bidPrice) < EPSILON) {
    vb = cur.bidQuantity - prev.bidQuantity;
  } else if (cur.bidPrice < prev.bidPrice) {
    vb = 0;
  } else {
    vb = cur.bidQuantity;
  }

  let va = 0;
  // two float numbers are equal
  if (Math.abs(1 - cur.askPrice / prev.askPrice) < EPSILON) {
    va = cur.askQuantity - prev.askQuantity;
  } else if (cur.askPrice < prev.askPrice) {
    va = cur.askQuantity;
  } else {
    va = 0;
  }

  return { voi: vb - va, oir: (vb - va) / (vb + va) };
}
