# @lp-labs/archive-stream

Standards-compatible streaming stored ZIP writing and bounded buffered ZIP extraction.

Original independent MIT TypeScript implementation with **zero runtime/peer dependencies**. ESM and CommonJS with separate declarations, Node 18+ (browser use when adapters/APIs are available). Development dependencies are not consumer runtime dependencies.

## Install

```sh
npm install @lp-labs/archive-stream
```

## Complete example

```js
import assert from 'node:assert/strict';
import { writeZip, extractZipBuffered, crc32, ArchiveStreamError } from '@lp-labs/archive-stream';
const segments = [];
for await (const chunk of writeZip([
  { name: 'hello.txt', data: new TextEncoder().encode('hello') },
]))
  segments.push(chunk);
const bytes = Uint8Array.from(segments.flatMap((chunk) => [...chunk]));
const files = [];
for await (const entry of extractZipBuffered(bytes)) files.push(entry);
assert.equal(files[0].name, 'hello.txt');
assert.equal(new TextDecoder().decode(files[0].data), 'hello');
assert.equal(crc32(new TextEncoder().encode('123456789')), 0xcbf43926);
```

CommonJS exposes the same API: `const api = require('@lp-labs/archive-stream')`.

## Scope, customization and performance

Writer emits real ZIP method0 with UTF8 names, signed descriptors, central directory and EOCD. Input chunks max1MiB, archive64MiB, entry16MiB, entries4096, metadata4MiB defaults. Extractor explicitly buffers entire archive (peak approximately twice archive bytes plus segments), NOT streaming decompression. Supports stored-only, ASCII/UTF8 names, ordinary comments/extras and signed/unsigned descriptors. Rejects compressed/encrypted/ZIP64/multidisk/preambles/gaps/overlap. CRC checked before each yield, not all entries before first yield; stage output before activation. No filesystem writes. Path policy is basic ZIP syntax, not OS extraction containment: use a dedicated guard. Returned data slices owned separately. Cancellation cooperative between chunks; writer consumers may receive partial output on late failure.

Configure explicit limits, clocks, policies and adapters through the API below. Error classes and standard TypeError/RangeError reject malformed input; adapter/handler errors propagate. Await asynchronous methods and handle rejection in your application; no success-shaped fallback on failed work. Inputs must be trusted ordinary data, not hostile proxies; adapters must meet documented atomicity, isolation and ownership contracts. This package never publishes data, loads external code or starts background services at import time.

## API reference

The complete exported TypeScript contract (including option defaults discussed above, method return shapes, and error codes) is included below. TypeScript users import interfaces using `import type`; JavaScript users pass matching ordinary objects. Signature optionals indicate intentionally optional fields, not silent recovery.

```ts
export type ArchiveErrorCode =
  'invalid' | 'limit' | 'unsupported' | 'corrupt' | 'truncated' | 'crc' | 'cancelled';
export declare class ArchiveStreamError extends Error {
  readonly code: ArchiveErrorCode;
  constructor(code: ArchiveErrorCode, message: string);
}
export interface ZipLimits {
  maxArchiveBytes?: number;
  maxEntryBytes?: number;
  maxEntries?: number;
  maxNameBytes?: number;
  maxMetadataBytes?: number;
  maxChunkBytes?: number;
  signal?: AbortSignal;
}
export interface ZipInputEntry {
  name: string;
  data: Uint8Array | AsyncIterable<Uint8Array>;
}
export interface ZipExtractedEntry {
  name: string;
  data: Uint8Array;
  crc32: number;
}
export declare function crc32(bytes: Uint8Array): number;
/** Streaming stored-method ZIP writer with signed data descriptors and bounded central metadata. */
export declare function writeZip(
  entries: AsyncIterable<ZipInputEntry> | Iterable<ZipInputEntry>,
  options?: ZipLimits,
): AsyncGenerator<Uint8Array>;
/** Intentionally buffers the entire archive before parsing; no decompression or filesystem writes. */
export declare function extractZipBuffered(
  source: AsyncIterable<Uint8Array> | Uint8Array,
  options?: ZipLimits,
): AsyncGenerator<ZipExtractedEntry>;
```

## Support

If this library is useful to you, consider supporting its maintenance with a [donation via PayPal](https://www.paypal.me/LibinPrasanth).

## Author

Created by [Libin Prasanth](https://www.linkedin.com/in/libinprasanth/). Connect on LinkedIn for updates and discussion.
