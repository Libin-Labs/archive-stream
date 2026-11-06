import { writeZip, extractZipBuffered, crc32, ArchiveStreamError } from '@lp-labs/archive-stream';
const publicAPI = [writeZip, extractZipBuffered, crc32, ArchiveStreamError];
const apiCount: number = publicAPI.length;
void apiCount;
const result: AsyncGenerator<Uint8Array> = writeZip([{ name: 'a', data: new Uint8Array() }]);
const checksum: number = crc32(new Uint8Array());
void result;
void checksum;
