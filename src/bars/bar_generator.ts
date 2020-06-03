import { strict as assert } from 'assert';
import { BboMsg, Msg, TradeMsg } from 'crypto-crawler';
import { EventEmitter } from 'events';
import StrictEventEmitter from 'strict-event-emitter-types';
import { BarMsg, BarType } from './bar_msg';
import { aggregateBbo, aggregateTrade } from './common';

interface Events {
  error: Error;
  log: { level: 'info' | 'warn' | 'error'; message: string };
  bar: BarMsg;
}

type MyEmitter = StrictEventEmitter<EventEmitter, Events>;

// eslint-disable-next-line import/prefer-default-export
export class BarGenerator extends (EventEmitter as { new (): MyEmitter }) {
  protected barType: BarType;

  protected barSize: number;

  constructor(barType: BarType, barSize: number) {
    super();
    this.barType = barType;
    this.barSize = barSize;
  }

  public append(msg: Msg): void {
    this.checkTimestamp(msg);

    if (msg.channelType === 'Trade') {
      this.appendTradeMsg(msg as TradeMsg);
    } else if (msg.channelType === 'BBO') {
      this.appendBboMsg(msg as BboMsg);
    } else {
      // do nothing
    }
  }

  protected appendTradeMsg(msg: TradeMsg): void {
    assert.ok(msg.exchange);
    assert.ok(this.barType);
    assert.ok(this.barSize);

    throw new Error('Must be implemented by subclasses');
  }

  protected appendBboMsg(msg: BboMsg): void {
    assert.ok(msg.exchange);
    assert.ok(this.barType);
    assert.ok(this.barSize);

    throw new Error('Must be implemented by subclasses');
  }

  public close(): void {
    this.emit('log', { level: 'info', message: 'closed' });
  }

  protected checkTimestamp(msg: Msg): void {
    if (msg.timestamp.toString().length !== 13) {
      const error = new Error('msg.timestamp is not 13 length');
      this.emit('error', error);
      throw error;
    }
  }

  protected convertToBarMsg(
    barTimestamp: number,
    tradeMsges: TradeMsg[],
    bboMsges: BboMsg[],
  ): BarMsg | undefined {
    if (tradeMsges.length <= 0) return undefined;

    tradeMsges.sort((x, y) =>
      x.timestamp !== y.timestamp
        ? x.timestamp - y.timestamp
        : x.trade_id.localeCompare(y.trade_id),
    );

    const { exchange, marketType, pair, rawPair } = tradeMsges[0];
    const { trade, trade_indicators } = aggregateTrade(tradeMsges);

    const timestampBegin = this.barType === 'TimeBar' ? barTimestamp : tradeMsges[0].timestamp;
    const timestampEnd =
      this.barType === 'TimeBar'
        ? barTimestamp + this.barSize * 1000
        : tradeMsges[tradeMsges.length - 1].timestamp;

    const barMsg: BarMsg = {
      exchange,
      market_type: marketType,
      pair,
      raw_rair: rawPair,
      bar_type: this.barType,
      bar_size: this.barSize,
      timestamp: timestampBegin,
      timestamp_end: timestampEnd,
      trade,
      trade_indicators,
    };

    // eslint-disable-next-line no-param-reassign
    bboMsges = bboMsges.filter(
      (msg) => msg.timestamp >= timestampBegin && msg.timestamp < timestampEnd,
    );
    if (bboMsges.length > 0) {
      bboMsges.sort((x, y) => x.timestamp - y.timestamp);
      barMsg.bbo = aggregateBbo(bboMsges);
    }

    return barMsg;
  }
}
