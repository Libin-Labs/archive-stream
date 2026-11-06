import test from 'node:test';
import assert from 'node:assert/strict';
import { writeZip, extractZipBuffered, crc32, ArchiveStreamError } from '../dist/esm/index.js';

test('documented workflow produces required outputs', async () => {
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
});
async function zip(entries) {
  const parts = [];
  for await (const p of writeZip(entries)) parts.push(p);
  return Uint8Array.from(parts.flatMap((p) => [...p]));
}
test('empty archive and Unicode names round trip', async () => {
  const empty = [];
  for await (const f of extractZipBuffered(await zip([]))) empty.push(f);
  assert.deepEqual(empty, []);
  const files = [];
  for await (const f of extractZipBuffered(await zip([{ name: '😀.txt', data: Uint8Array.of(1) }])))
    files.push(f);
  assert.equal(files[0].name, '😀.txt');
});
test('corruption and truncated end record reject', async () => {
  const bytes = await zip([{ name: 'a', data: Uint8Array.of(1, 2) }]);
  bytes[31] ^= 1;
  await assert.rejects(
    async () => {
      for await (const f of extractZipBuffered(bytes)) void f;
    },
    { code: 'crc' },
  );
  await assert.rejects(async () => {
    for await (const f of extractZipBuffered(bytes.slice(0, -2))) void f;
  });
});
test('compression and entry limits explicitly reject', async () => {
  const b = await zip([{ name: 'a', data: Uint8Array.of(1) }]);
  const view = new DataView(b.buffer);
  view.setUint16(8, 8, true);
  const at = b.findIndex(
    (x, i) => x === 0x50 && b[i + 1] === 0x4b && b[i + 2] === 1 && b[i + 3] === 2,
  );
  view.setUint16(at + 10, 8, true);
  await assert.rejects(
    async () => {
      for await (const f of extractZipBuffered(b)) void f;
    },
    { code: 'unsupported' },
  );
  await assert.rejects(
    async () => {
      for await (const c of writeZip([{ name: 'a', data: Uint8Array.of(1, 2) }], {
        maxEntryBytes: 1,
      }))
        void c;
    },
    { code: 'limit' },
  );
});
test('traversal names and cancellation reject', async () => {
  await assert.rejects(async () => {
    for await (const c of writeZip([{ name: '../a', data: new Uint8Array() }])) void c;
  });
  const c = new AbortController();
  c.abort();
  await assert.rejects(
    async () => {
      for await (const p of writeZip([], { signal: c.signal })) void p;
    },
    { code: 'cancelled' },
  );
});
