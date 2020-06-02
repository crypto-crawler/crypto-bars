import fs from 'fs';
import * as stream from 'stream';

const rfs = require('rotating-file-stream'); // eslint-disable-line @typescript-eslint/no-var-requires

export default class RotatedFile {
  private stream: stream.Writable;

  /**
   * @param rootDir Root dir
   * @param fileSize File size in megabytes
   */
  constructor(rootDir: string, fileSize = 256) {
    fs.mkdirSync(rootDir, { recursive: true });
    this.stream = rfs.createStream(RotatedFile.generator, {
      compress: 'gzip',
      path: rootDir,
      size: `${fileSize}M`, // rotate every 256 MegaBytes written
    });
  }

  public write(data: string): void {
    this.stream.write(data);
  }

  private static pad(num: number): string {
    return (num > 9 ? '' : '0') + num;
  }

  private static generator(time: Date, index: number): string {
    if (!time) {
      return 'file.log';
    }

    const month = `${time.getFullYear()}${RotatedFile.pad(time.getMonth() + 1)}`;
    const day = RotatedFile.pad(time.getDate());
    const hour = RotatedFile.pad(time.getHours());
    const minute = RotatedFile.pad(time.getMinutes());

    return `${month}${day}-${hour}${minute}-${index}.json.gz`;
  }
}
