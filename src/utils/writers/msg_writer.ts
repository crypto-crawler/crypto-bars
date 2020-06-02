export interface MsgWriter {
  // eslint-disable-next-line @typescript-eslint/ban-types
  write(messages: readonly object[]): Promise<void>;
}
