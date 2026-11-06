export type ArchiveErrorCode =
  'invalid' | 'limit' | 'unsupported' | 'corrupt' | 'truncated' | 'crc' | 'cancelled';
export class ArchiveStreamError extends Error {
  constructor(
    public readonly code: ArchiveErrorCode,
    message: string,
  ) {
    super(message);
    this.name = 'ArchiveStreamError';
  }
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
interface Bounds {
  archive: number;
  entry: number;
  entries: number;
  name: number;
  metadata: number;
  chunk: number;
}
function fail(code: ArchiveErrorCode, message: string): never {
  throw new ArchiveStreamError(code, message);
}
function bounds(options: ZipLimits): Bounds {
  const result = {
    archive: options.maxArchiveBytes ?? 67108864,
    entry: options.maxEntryBytes ?? 16777216,
    entries: options.maxEntries ?? 4096,
    name: options.maxNameBytes ?? 4096,
    metadata: options.maxMetadataBytes ?? 4194304,
    chunk: options.maxChunkBytes ?? 1048576,
  };
  for (const value of Object.values(result))
    if (!Number.isSafeInteger(value) || value < 1)
      fail('invalid', 'Limits must be positive safe integers');
  if (
    result.archive >= 0xffffffff ||
    result.entry >= 0xffffffff ||
    result.entries >= 0xffff ||
    result.name > 0xffff
  ) {
    fail('unsupported', 'ZIP64-sized limits are unsupported');
  }
  return result;
}
function check(signal?: AbortSignal): void {
  if (signal?.aborted) fail('cancelled', 'Archive operation cancelled');
}
function updateCrc(crc: number, bytes: Uint8Array): number {
  for (const byte of bytes) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit++) crc = (crc >>> 1) ^ (crc & 1 ? 0xedb88320 : 0);
  }
  return crc >>> 0;
}
export function crc32(bytes: Uint8Array): number {
  if (!(bytes instanceof Uint8Array)) fail('invalid', 'Expected Uint8Array');
  return (updateCrc(0xffffffff, bytes) ^ 0xffffffff) >>> 0;
}
function header(size: number): { bytes: Uint8Array; view: DataView } {
  const bytes = new Uint8Array(size);
  return { bytes, view: new DataView(bytes.buffer) };
}
function nameBytes(name: string, bound: Bounds): Uint8Array {
  if (
    typeof name !== 'string' ||
    !name ||
    name.startsWith('/') ||
    name.includes('\\') ||
    name.includes('\0') ||
    /^[A-Za-z]:/.test(name) ||
    name
      .split('/')
      .some(
        (part, index, array) =>
          part === '..' || part === '.' || (!part && index !== array.length - 1),
      )
  ) {
    fail('invalid', 'Unsafe or invalid archive path');
  }
  const bytes = new TextEncoder().encode(name);
  if (new TextDecoder('utf-8', { fatal: true }).decode(bytes) !== name)
    fail('invalid', 'Name has unpaired Unicode surrogates');
  if (bytes.length > bound.name) fail('limit', 'Name byte bound exceeded');
  return bytes;
}
interface RecordEntry {
  name: Uint8Array;
  crc: number;
  size: number;
  offset: number;
}
/** Streaming stored-method ZIP writer with signed data descriptors and bounded central metadata. */
export async function* writeZip(
  entries: AsyncIterable<ZipInputEntry> | Iterable<ZipInputEntry>,
  options: ZipLimits = {},
): AsyncGenerator<Uint8Array> {
  const bound = bounds(options);
  const records: RecordEntry[] = [];
  const names = new Set<string>();
  let offset = 0,
    metadata = 0;
  function emit(bytes: Uint8Array): Uint8Array {
    check(options.signal);
    if (offset + bytes.length > bound.archive) fail('limit', 'Archive byte bound exceeded');
    offset += bytes.length;
    return bytes;
  }
  for await (const entry of entries) {
    check(options.signal);
    if (records.length >= bound.entries) fail('limit', 'Entry count exceeded');
    const name = nameBytes(entry.name, bound);
    if (names.has(entry.name)) fail('invalid', 'Duplicate archive path');
    names.add(entry.name);
    metadata += 46 + name.length;
    if (metadata > bound.metadata) fail('limit', 'Central metadata bound exceeded');
    const start = offset;
    const local = header(30 + name.length);
    local.view.setUint32(0, 0x04034b50, true);
    local.view.setUint16(4, 20, true);
    local.view.setUint16(6, 0x0808, true);
    local.view.setUint16(12, 0x0021, true);
    local.view.setUint16(26, name.length, true);
    local.bytes.set(name, 30);
    yield emit(local.bytes);
    let size = 0,
      crc = 0xffffffff;
    const data = entry.data;
    const source =
      data instanceof Uint8Array
        ? (async function* () {
            for (let i = 0; i < data.length; i += bound.chunk)
              yield data.subarray(i, i + bound.chunk);
          })()
        : data;
    for await (const chunk of source) {
      check(options.signal);
      if (!(chunk instanceof Uint8Array)) fail('invalid', 'Entry source must yield Uint8Array');
      if (chunk.length > bound.chunk || size + chunk.length > bound.entry)
        fail('limit', 'Entry or input chunk bound exceeded');
      if (entry.name.endsWith('/') && chunk.length)
        fail('invalid', 'Directory must have empty data');
      const bytes = chunk.slice();
      size += bytes.length;
      crc = updateCrc(crc, bytes);
      yield emit(bytes);
    }
    crc = (crc ^ 0xffffffff) >>> 0;
    const descriptor = header(16);
    descriptor.view.setUint32(0, 0x08074b50, true);
    descriptor.view.setUint32(4, crc, true);
    descriptor.view.setUint32(8, size, true);
    descriptor.view.setUint32(12, size, true);
    yield emit(descriptor.bytes);
    records.push({ name, crc, size, offset: start });
  }
  const centralOffset = offset;
  for (const record of records) {
    const central = header(46 + record.name.length);
    central.view.setUint32(0, 0x02014b50, true);
    central.view.setUint16(4, 20, true);
    central.view.setUint16(6, 20, true);
    central.view.setUint16(8, 0x0808, true);
    central.view.setUint16(14, 0x0021, true);
    central.view.setUint32(16, record.crc, true);
    central.view.setUint32(20, record.size, true);
    central.view.setUint32(24, record.size, true);
    central.view.setUint16(28, record.name.length, true);
    central.view.setUint32(42, record.offset, true);
    central.bytes.set(record.name, 46);
    yield emit(central.bytes);
  }
  const centralSize = offset - centralOffset;
  const end = header(22);
  end.view.setUint32(0, 0x06054b50, true);
  end.view.setUint16(8, records.length, true);
  end.view.setUint16(10, records.length, true);
  end.view.setUint32(12, centralSize, true);
  end.view.setUint32(16, centralOffset, true);
  yield emit(end.bytes);
}
/** Intentionally buffers the entire archive before parsing; no decompression or filesystem writes. */
export async function* extractZipBuffered(
  source: AsyncIterable<Uint8Array> | Uint8Array,
  options: ZipLimits = {},
): AsyncGenerator<ZipExtractedEntry> {
  const bound = bounds(options);
  check(options.signal);
  const segments: Uint8Array[] = [];
  let length = 0;
  if (source instanceof Uint8Array) {
    if (source.length > bound.archive) fail('limit', 'Archive byte bound exceeded');
    segments.push(source.slice());
    length = source.length;
  } else {
    for await (const chunk of source) {
      check(options.signal);
      if (!(chunk instanceof Uint8Array)) fail('invalid', 'Archive source must yield Uint8Array');
      if (chunk.length > bound.chunk || length + chunk.length > bound.archive)
        fail('limit', 'Archive or input chunk bound exceeded');
      if (chunk.length) segments.push(chunk.slice());
      length += chunk.length;
    }
  }
  check(options.signal);
  if (length < 22) fail('truncated', 'Missing ZIP end record');
  const bytes = new Uint8Array(length);
  let cursor = 0;
  for (const chunk of segments) {
    bytes.set(chunk, cursor);
    cursor += chunk.length;
  }
  segments.length = 0;
  const view = new DataView(bytes.buffer);
  function need(at: number, size: number): void {
    if (!Number.isSafeInteger(at) || at < 0 || at + size > length)
      fail('truncated', 'ZIP record exceeds archive');
  }
  function u16(at: number): number {
    need(at, 2);
    return view.getUint16(at, true);
  }
  function u32(at: number): number {
    need(at, 4);
    return view.getUint32(at, true);
  }
  let end = -1;
  for (let i = length - 22; i >= Math.max(0, length - 65557); i--) {
    if (u32(i) === 0x06054b50 && i + 22 + u16(i + 20) === length) {
      end = i;
      break;
    }
  }
  if (end < 0) fail('truncated', 'Missing or truncated ZIP end record');
  if (u16(end + 4) !== 0 || u16(end + 6) !== 0 || u16(end + 8) !== u16(end + 10))
    fail('unsupported', 'Multidisk ZIP unsupported');
  const count = u16(end + 10),
    centralSize = u32(end + 12),
    centralOffset = u32(end + 16);
  if (
    count === 0xffff ||
    centralSize === 0xffffffff ||
    centralOffset === 0xffffffff ||
    (end >= 20 && u32(end - 20) === 0x07064b50)
  )
    fail('unsupported', 'ZIP64 unsupported');
  if (count > bound.entries || centralSize > bound.metadata)
    fail('limit', 'Central directory bound exceeded');
  if (centralOffset + centralSize !== end)
    fail('corrupt', 'Central directory does not end at EOCD');
  need(centralOffset, centralSize);
  function extra(at: number, size: number): void {
    const stop = at + size;
    while (at < stop) {
      if (at + 4 > stop) fail('corrupt', 'Malformed extra field');
      const tag = u16(at),
        size = u16(at + 2);
      if (tag === 1) fail('unsupported', 'ZIP64 extra field unsupported');
      at += 4 + size;
      if (at > stop) fail('corrupt', 'Truncated extra field');
    }
  }
  function flags(value: number, method: number, version: number): void {
    if (method !== 0) fail('unsupported', 'Only stored-method ZIP supported');
    if (value & 1) fail('unsupported', 'Encrypted ZIP unsupported');
    if ((value & ~0x0808) !== 0 || version > 20)
      fail('unsupported', 'Unsupported ZIP flags or version');
  }
  interface Parsed {
    name: string;
    encoded: Uint8Array;
    flags: number;
    crc: number;
    size: number;
    offset: number;
    dataStart: number;
    end: number;
  }
  const records: Parsed[] = [];
  const names = new Set<string>();
  cursor = centralOffset;
  for (let i = 0; i < count; i++) {
    check(options.signal);
    need(cursor, 46);
    if (u32(cursor) !== 0x02014b50) fail('corrupt', 'Invalid central directory signature');
    const flag = u16(cursor + 8),
      method = u16(cursor + 10);
    flags(flag, method, u16(cursor + 6));
    const crc = u32(cursor + 16),
      compressed = u32(cursor + 20),
      size = u32(cursor + 24);
    const nameLength = u16(cursor + 28),
      extraLength = u16(cursor + 30),
      commentLength = u16(cursor + 32);
    const offset = u32(cursor + 42);
    if (compressed === 0xffffffff || size === 0xffffffff || offset === 0xffffffff)
      fail('unsupported', 'ZIP64 entry unsupported');
    if (u16(cursor + 34) !== 0) fail('unsupported', 'Multidisk entry unsupported');
    if (compressed !== size) fail('corrupt', 'Stored sizes must match');
    if (size > bound.entry || nameLength > bound.name) fail('limit', 'Entry bound exceeded');
    const next = cursor + 46 + nameLength + extraLength + commentLength;
    if (next > end) fail('corrupt', 'Central record exceeds directory');
    const encoded = bytes.subarray(cursor + 46, cursor + 46 + nameLength);
    if (!(flag & 0x800) && encoded.some((byte) => byte >= 128))
      fail('unsupported', 'Non-UTF8 non-ASCII filenames unsupported');
    let name: string;
    try {
      name = new TextDecoder('utf-8', { fatal: true }).decode(encoded);
    } catch {
      return fail('corrupt', 'Invalid UTF-8 filename');
    }
    nameBytes(name, bound);
    if (names.has(name)) fail('corrupt', 'Duplicate archive path');
    names.add(name);
    extra(cursor + 46 + nameLength, extraLength);
    records.push({ name, encoded, flags: flag, crc, size, offset, dataStart: 0, end: 0 });
    cursor = next;
  }
  if (cursor !== end) fail('corrupt', 'Central directory count or size mismatch');
  const ordered = [...records].sort((a, b) => a.offset - b.offset);
  let localEnd = 0;
  for (const record of ordered) {
    check(options.signal);
    const at = record.offset;
    if (at !== localEnd) fail('corrupt', 'Overlapping entries, gaps, or preamble unsupported');
    need(at, 30);
    if (u32(at) !== 0x04034b50) fail('corrupt', 'Invalid local header signature');
    flags(u16(at + 6), u16(at + 8), u16(at + 4));
    if (u16(at + 6) !== record.flags) fail('corrupt', 'Local flags mismatch');
    const nl = u16(at + 26),
      el = u16(at + 28);
    need(at + 30, nl + el);
    if (
      nl !== record.encoded.length ||
      record.encoded.some((byte, index) => bytes[at + 30 + index] !== byte)
    )
      fail('corrupt', 'Local filename mismatch');
    extra(at + 30 + nl, el);
    const start = at + 30 + nl + el,
      stop = start + record.size;
    if (stop > centralOffset) fail('truncated', 'Entry data exceeds local region');
    if (record.name.endsWith('/') && record.size) fail('corrupt', 'Directory contains data');
    if (!(record.flags & 8)) {
      if (
        u32(at + 14) !== record.crc ||
        u32(at + 18) !== record.size ||
        u32(at + 22) !== record.size
      )
        fail('corrupt', 'Local CRC or sizes mismatch');
      localEnd = stop;
    } else {
      if (
        (u32(at + 14) !== 0 && u32(at + 14) !== record.crc) ||
        (u32(at + 18) !== 0 && u32(at + 18) !== record.size) ||
        (u32(at + 22) !== 0 && u32(at + 22) !== record.size)
      )
        fail('corrupt', 'Descriptor local values mismatch');
      let descriptorSize = 0;
      if (
        stop + 16 <= centralOffset &&
        u32(stop) === 0x08074b50 &&
        u32(stop + 4) === record.crc &&
        u32(stop + 8) === record.size &&
        u32(stop + 12) === record.size
      )
        descriptorSize = 16;
      else if (
        stop + 12 <= centralOffset &&
        u32(stop) === record.crc &&
        u32(stop + 4) === record.size &&
        u32(stop + 8) === record.size
      )
        descriptorSize = 12;
      else fail('corrupt', 'Missing or invalid data descriptor');
      localEnd = stop + descriptorSize;
    }
    record.dataStart = start;
    record.end = stop;
  }
  if (localEnd !== centralOffset) fail('corrupt', 'Local region and central directory mismatch');
  for (const record of records) {
    check(options.signal);
    const data = bytes.subarray(record.dataStart, record.end);
    // Check incrementally to bound cancellation latency for large entries.
    let crc = 0xffffffff;
    for (let i = 0; i < data.length; i += bound.chunk) {
      check(options.signal);
      crc = updateCrc(crc, data.subarray(i, i + bound.chunk));
    }
    if ((crc ^ 0xffffffff) >>> 0 !== record.crc) fail('crc', 'Entry CRC32 mismatch');
    yield { name: record.name, data: data.slice(), crc32: record.crc };
  }
}
