import { MsgWriter } from './msg_writer';
import RotatedFile from './rotated_file';

// eslint-disable-next-line import/prefer-default-export
export class FileMsgWriter implements MsgWriter {
  private rotatedFile: RotatedFile;

  constructor(rootDir: string, fileSize = 256) {
    this.rotatedFile = new RotatedFile(rootDir, fileSize);
  }

  // eslint-disable-next-line @typescript-eslint/ban-types
  public async write(messages: readonly object[]): Promise<void> {
    if (messages.length <= 0) return;
    this.rotatedFile.write(`${messages.map((x) => JSON.stringify(x)).join('\n')}\n`);
  }
}
