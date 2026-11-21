import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, it } from 'node:test';
import { gunzipSync } from 'node:zlib';
import { createBasis } from '../src/client.ts';
import { FileStore, R2Store, sha256Hex, signV4 } from '../src/store/blob.ts';
import { NotInStoreError, RawIndex, RawStore, replayFetch, type LogEntry } from '../src/store/raw.ts';
import { fakeFetch, fixture, response, silent } from './helpers.ts';

const tmp = () => mkdtempSync(join(tmpdir(), 'akleg-store-'));

describe('AWS Signature Version 4', () => {
  // The examples in https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
  const keys = { accessKeyId: 'AKIAIOSFODNN7EXAMPLE', secretAccessKey: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY', region: 'us-east-1', service: 's3', date: new Date('2013-05-24T00:00:00Z') };
  it('signs a GET object request like the AWS example', () => {
    const h = signV4({ ...keys, method: 'GET', url: new URL('https://examplebucket.s3.amazonaws.com/test.txt'), headers: { range: 'bytes=0-9' }, body: new Uint8Array() });
    assert.match(h.authorization!, /SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41$/);
  });
  it('signs a list request like the AWS example', () => {
    const h = signV4({ ...keys, method: 'GET', url: new URL('https://examplebucket.s3.amazonaws.com/?max-keys=2&prefix=J'), headers: {}, body: new Uint8Array() });
    assert.match(h.authorization!, /Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7$/);
  });
});

describe('R2Store', () => {
  it('sends S3 requests to the account endpoint and pages through lists', async () => {
    const calls: Array<[string, string]> = [];
    const pages = [
      '<ListBucketResult><Contents><Key>raw/log/2026-01-01.ndjson.gz</Key></Contents><IsTruncated>true</IsTruncated><NextContinuationToken>t&amp;2</NextContinuationToken></ListBucketResult>',
      '<ListBucketResult><Contents><Key>raw/log/2026-01-02.ndjson.gz</Key></Contents><IsTruncated>false</IsTruncated></ListBucketResult>',
    ];
    const fetch = (async (url: URL, init: RequestInit) => {
      calls.push([init.method!, url.toString()]);
      assert.match((init.headers as Record<string, string>).authorization!, /^AWS4-HMAC-SHA256 Credential=key\/\d{8}\/auto\/s3\/aws4_request/);
      if (init.method === 'GET' && url.searchParams.has('list-type')) return new Response(pages.shift());
      if (init.method === 'GET') return new Response(null, { status: 404 });
      return new Response('');
    }) as typeof globalThis.fetch;
    const r2 = new R2Store({ accountId: 'acct', accessKeyId: 'key', secretAccessKey: 'secret', bucket: 'akleg', fetch });
    assert.deepEqual(await r2.list('raw/log/'), ['raw/log/2026-01-01.ndjson.gz', 'raw/log/2026-01-02.ndjson.gz']);
    assert.equal(await r2.get('raw/objects/x.gz'), null);
    await r2.put('published/latest/bills.csv', new TextEncoder().encode('a'), 'text/csv');
    assert.deepEqual(calls.map(([m]) => m), ['GET', 'GET', 'GET', 'PUT']);
    assert.equal(calls[1]![1], 'https://acct.r2.cloudflarestorage.com/akleg/?list-type=2&prefix=raw%2Flog%2F&continuation-token=t%262');
    assert.equal(calls[3]![1], 'https://acct.r2.cloudflarestorage.com/akleg/published/latest/bills.csv');
    assert.throws(() => R2Store.fromEnv({}), /Set R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET/);
    const env = { CLOUDFLARE_ACCOUNT_ID: 'cf', R2_ACCESS_KEY_ID: 'k', R2_SECRET_ACCESS_KEY: 's', R2_BUCKET: 'b' };
    assert.equal(R2Store.fromEnv(env).name, 'r2://b');
  });
});

describe('RawStore', () => {
  it('records responses once per content and replays them', async () => {
    const dir = tmp();
    const raw = new RawStore(new FileStore(join(dir, 'store')), { cacheDir: join(dir, 'cache') });
    const live = fakeFetch(() => response(fixture('members.json')));
    const sink: LogEntry[] = [];
    const basis = createBasis({ fetch: raw.recordingFetch(live, sink, '2026-01-01'), logger: silent });
    await basis.members.list({ session: 34 });
    await basis.members.list({ session: 34, query: { members: { code: 'FRB' } } });
    await raw.appendLog('2026-01-01', sink);

    const hash = sha256Hex(fixture('members.json'));
    assert.deepEqual(await raw.blobs.list('raw/'), [`raw/log/2026-01-01.ndjson.gz`, `raw/objects/${hash}.gz`]);
    assert.equal(gunzipSync((await raw.blobs.get(`raw/objects/${hash}.gz`))!).toString(), fixture('members.json'));
    assert.deepEqual(sink.map((e) => [e.legislature, e.query, e.status, e.hash]), [[34, null, 200, hash], [34, 'members;code=FRB', 200, hash]]);

    // A second store over the same directory replays without the network.
    const again = new RawStore(new FileStore(join(dir, 'store')));
    const index = new RawIndex(await again.loadLog());
    const replay = createBasis({ fetch: replayFetch(again, index.view('2026-01-01')), maxAttempts: 1 });
    assert.equal((await replay.members.list({ session: 34, query: { members: { code: 'FRB' } } })).length, 5);
    await assert.rejects(replay.members.list({ session: 33 }), NotInStoreError);
  });

  it('picks the latest response on or before a date, else the earliest', () => {
    const e = (date: string, url: string, hash: string): LogEntry => ({ date, time: `${date}T00:00:00Z`, url, query: null, range: null, status: 200, hash, legislature: 34 });
    const index = new RawIndex([e('2026-01-01', 'a', 'a1'), e('2026-01-02', 'a', 'a2'), e('2026-01-03', 'b', 'b3'), e('2026-01-04', 'a', 'a4')]);
    const hashes = (d: string) => [...index.view(d).values()].map((x) => x.hash);
    assert.deepEqual(hashes('2026-01-02'), ['a2', 'b3']); // b was first requested later: its first answer fills in
    assert.deepEqual(hashes('2026-01-03'), ['a2', 'b3']);
    assert.deepEqual(hashes('2026-01-09'), ['a4', 'b3']);
    assert.deepEqual(index.dates(), ['2026-01-01', '2026-01-02', '2026-01-03', '2026-01-04']);
    assert.notEqual(index.fingerprint('2026-01-01', 34), index.fingerprint('2026-01-02', 34));
  });
});
