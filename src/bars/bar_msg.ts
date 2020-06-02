import { MarketType } from 'crypto-markets';

export const BAR_TYPES = ['TimeBar', 'TickBar', 'VolumeBar', 'DollarBar'] as const;

export type BarType = typeof BAR_TYPES[number];

export interface BarMsg {
  exchange: string;
  market_type: MarketType;
  pair: string;
  raw_rair: string;
  bar_type: BarType;
  interval: number; // in second, BTC, ETH, USD, etc.
  timestamp: number; // begin time
  timestamp_end: number; // end time
}

export interface AggregateMsg {
  open: number;
  high: number;
  low: number;
  close: number;

  mean: number;
  median: number;
}

export interface TradeAggregateMsg extends AggregateMsg {
  volume: number; // base volume
  volume_quote: number; // quote volume
  volume_sell: number; // base volume at sell side
  volume_buy: number; // base volume at buy side
  volume_quote_sell: number; // quote volume at sell side
  volume_quote_buy: number; // quote volume at buy side

  vwap: number; // volume weighted average price

  count: number; // number of trades
  count_sell: number; // number of sell trades
  count_buy: number; // number of buy trades
}

export interface TradeIndicators {
  // tradePrie - spotIndexPrice
  basis: AggregateMsg;
  basis_vw: number; // volume weighted basis
  // Ticker Rule VPIN if time bar, Bulk volume VPIN if volume bar or USD bar
  vpin: number; // |volume_sell - volume_buy| / volume
}

// best bid & offer
export interface BboIndicators {
  bid: AggregateMsg;

  ask: AggregateMsg;

  mid: AggregateMsg;

  count: number; // number of BBO messages received in the time window

  vw_spread_global: number; // [sum(bidSize x bidPrice) - sum(askSize x askPrice)] / [sum(bidSize x bidPrice) + sum(askSize x askPrice)]

  // spread = bidPrice - askPrice
  spread: AggregateMsg;
  // volume weighted spread, (bidSize x bidPrice - askSize x askPrice) / (bidSize x bidPrice + askSize x askPrice)
  vw_spread: AggregateMsg;

  basis: AggregateMsg;
  // volume order imbalance
  voi: AggregateMsg;
  // order imbalance ratio
  oir: AggregateMsg;

  // normalized by spread
  basis_norm: AggregateMsg;
  voi_norm: AggregateMsg;
  oir_norm: AggregateMsg;
}

export interface TimeBarMsg extends BarMsg {
  trade?: TradeAggregateMsg;

  // Indicators based on Trade
  trade_indicators?: TradeIndicators;

  // best bid & offer
  bbo?: BboIndicators;
}

export type VolumeBarMsg = TimeBarMsg;
