// End to end: three nightly fetches from a fake API into a local raw store,
// then a build of the tables and their history.
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, it } from 'node:test';
import { build } from '../src/build/build.ts';
import { readCsvAll } from '../src/build/csv.ts';
import { defaultLegislatures, fetchRaw } from '../src/build/fetch.ts';
import { publish } from '../src/build/publish.ts';
import { FileStore } from '../src/store/blob.ts';
import { RawIndex, RawStore } from '../src/store/raw.ts';
import { curatedFromCsv } from '../src/tables/index.ts';
import { fakeFetch, response, silent } from './helpers.ts';

const curated = curatedFromCsv(
  [
    { PersonId: 'per_ann-lee-33rd', PersonFullName: 'Ann Lee', PersonFirstName: 'Ann', PersonLastName: 'Lee' },
    { PersonId: 'per_bob-roe-34th', PersonFullName: 'Bob Roe', PersonFirstName: 'Bob', PersonLastName: 'Roe' },
  ],
  [
    {
      LegislatureNumber: '34',
      PersonId: 'per_ann-lee-33rd',
      MemberCode: 'LEA',
      MemberChamber: 'H',
      MemberDistrict: '1',
    },
    {
      LegislatureNumber: '34',
      PersonId: 'per_bob-roe-34th',
      MemberCode: 'ROE',
      MemberChamber: 'H',
      MemberDistrict: '2',
    },
  ],
  [{ VoteId: 'vot_34th_H0001', VoteDescription: 'Typed by hand' }],
);

function bill(number: string, title: string) {
  return {
    Documents: [],
    PartialVeto: false,
    Vetoed: false,
    BillNumber: number,
    BillName: '',
    ShortTitle: title,
    StatusCode: '002',
    StatusText: '(H) STA',
    Flag1: ' ',
    Flag2: ' ',
    StatusDate: '2026-04-01',
    StatusAndThen: [],
    StatusSummaryCode: ' ',
    OnFloor: ' ',
    NotKnown: ' ',
    Filler: ' ',
    Lock: ' ',
    AllMeetings: [],
    Meetings: [],
    ManifestErrors: [],
    Statutes: [],
    SponsorUrl: '',
    Subjects: [],
  };
}

function vote(member: string, num: string, choice: string) {
  return {
    Vote: `${choice} `,
    VoteNum: num,
    Bill: 'HB   1',
    Member: member,
    MemberParty: 'R',
    MemberChamber: 'H',
    MemberName: 'Name',
    Title: 'HB 1\r\nThird Reading\r\n',
    VoteDate: '2026-04-02',
  };
}

function version(letter: string) {
  return {
    VersionLetter: letter,
    Title: 'An Act',
    Name: `HB 1${letter}`,
    IntroDate: '2025-01-22',
    PassedHouse: null,
    PassedSenate: null,
    WorkOrder: '34-LS0001',
    Url: `https://x/HB0001${letter}.PDF`,
    Mime: null,
    Encoding: null,
    Data: null,
  };
}

/** The API as it looks on day 1, 2 or 3. */
function api(day: number) {
  return fakeFetch((url, init) => {
    const q = init.headers['X-Alaska-Legislature-Basis-Query'] ?? '';
    if (!url.includes('session=34') && !url.includes('/Plaintext/34')) {
      return response('{"Basis":{"Error":{"Code":"BaoSystemFault","Description":"Invalid Session Number."},}}');
    }
    if (url.includes('/Plaintext/')) return response(`\r\n00 HOUSE BILL NO. 1 ${/HB0001(\w)$/.exec(url)![1]}\r\n`);
    if (url.includes('/basis/bills') && q === 'subjects') {
      const bills = [bill('HB   1', day === 1 ? 'OLD TITLE' : 'NEW TITLE')];
      if (day === 1) bills.push(bill('HB   2', 'WITHDRAWN LATER'));
      return response(JSON.stringify({ Basis: { Bills: bills } }));
    }
    if (url.includes('/basis/bills')) {
      const number = /bill=([^,]+)/.exec(q)![1]!;
      const versions = number === 'HB 1' ? (day === 1 ? [version('A')] : [version('A'), version('B')]) : [];
      return response(JSON.stringify({ Basis: { Bills: [{ ...bill(number, 'x'), Versions: versions }] } }));
    }
    const code = /code=(\w+)/.exec(q)![1]!;
    const votes =
      code === 'LEA'
        ? [vote('LEA', 'H0001', 'Y'), ...(day >= 2 ? [vote('LEA', 'H0002', 'N')] : [])]
        : [vote('ROE', 'H0001', 'N')];
    return response(JSON.stringify({ Basis: { Members: [{ Votes: votes }] } }));
  });
}

describe('fetch checkpoints', () => {
  it('saves the log while fetching, not just at the end', async () => {
    const store = new FileStore(mkdtempSync(join(tmpdir(), 'akleg-ckpt-')));
    const inner = api(1);
    const seen: number[] = [];
    const fetch: typeof inner = Object.assign(
      async (url: string, init: Parameters<typeof inner>[1]) => {
        seen.push((await new RawStore(store).loadLog()).length);
        return inner(url, init);
      },
      { calls: inner.calls },
    );
    const summary = await fetchRaw({
      raw: new RawStore(store),
      curated,
      logger: silent,
      date: '2026-04-01',
      now: new Date('2026-04-05T12:00:00Z'),
      legislatures: [34],
      checkpointEvery: 1,
      client: { fetch, sleep: async () => {} },
    });
    assert.ok(seen.at(-1)! > 0, `log was empty until the end: ${seen.join(',')}`);
    assert.equal((await new RawStore(store).loadLog()).length, summary.requests);
  });
});

describe('fetch refetch modes', () => {
  // In 2027 the current legislature is 35, so 34 is closed once 35 has data.
  const now = new Date('2027-03-01T12:00:00Z');
  const run = (store: FileStore, date: string, refetch: 'always' | 'as-needed', legislatures: number[]) => {
    const fetch = api(1);
    return fetchRaw({
      raw: new RawStore(store),
      curated,
      logger: silent,
      date,
      now,
      legislatures,
      refetch,
      client: { fetch, sleep: async () => {} },
    }).then((summary) => ({ summary, calls: fetch.calls.length }));
  };

  it('reuses stored responses for closed legislatures as needed, and re-requests them always', async () => {
    const store = new FileStore(mkdtempSync(join(tmpdir(), 'akleg-refetch-')));
    const first = await run(store, '2027-03-01', 'as-needed', [34, 35]);
    assert.ok(first.calls > 1);

    const asNeeded = await run(store, '2027-03-02', 'as-needed', [34]);
    assert.equal(asNeeded.calls, 0);
    assert.equal(asNeeded.summary.requests, 0);

    const always = await run(store, '2027-03-03', 'always', [34]);
    assert.ok(always.calls > 0);
  });

  it('always re-requests the current legislature', async () => {
    const store = new FileStore(mkdtempSync(join(tmpdir(), 'akleg-refetch-')));
    await run(store, '2027-03-01', 'as-needed', [35]);
    const again = await run(store, '2027-03-02', 'as-needed', [35]);
    assert.ok(again.calls > 0);
  });
});

describe('fetch and build', () => {
  it('builds current tables and SCD2 history from the raw store', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'akleg-build-'));
    const store = new FileStore(join(dir, 'store'));
    const now = new Date('2026-04-05T12:00:00Z');
    const texts: number[] = [];
    for (const [day, date] of [
      [1, '2026-04-01'],
      [2, '2026-04-02'],
      [3, '2026-04-03'],
    ] as const) {
      const fetch = api(day);
      const summary = await fetchRaw({
        raw: new RawStore(store),
        curated,
        logger: silent,
        date,
        now,
        legislatures: day === 1 ? undefined : [34],
        client: { fetch, sleep: async () => {} },
      });
      if (day === 1)
        assert.deepEqual(
          summary.legislatures,
          [12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34],
        );
      texts.push(fetch.calls.filter((c) => c.url.includes('Plaintext')).length);
    }
    assert.deepEqual(texts, [1, 1, 0], 'each version is downloaded once');

    const out = join(dir, 'out');
    const summary = await build({
      raw: new RawStore(store, { cacheDir: join(dir, 'cache') }),
      curated,
      outDir: out,
      logger: silent,
    });
    assert.deepEqual(summary.dates, ['2026-04-01', '2026-04-02', '2026-04-03']);
    assert.equal(summary.builds, 2, 'day 3 had the same responses as day 2, so it was skipped');
    assert.deepEqual(summary.warnings, []);

    const read = (f: string) => readCsvAll(join(out, `${f}.csv`));
    assert.deepEqual(
      (await read('bills')).map((b) => [b.BillId, b.BillShortTitle]),
      [['bil_34th_HB0001', 'NEW TITLE']],
    );
    assert.deepEqual(
      (await read('bills_history')).map((b) => [b.BillId, b.BillShortTitle, b.ValidFrom, b.ValidTo]),
      [
        ['bil_34th_HB0001', 'OLD TITLE', '2026-04-01', '2026-04-02'],
        ['bil_34th_HB0001', 'NEW TITLE', '2026-04-02', null],
        ['bil_34th_HB0002', 'WITHDRAWN LATER', '2026-04-01', '2026-04-02'], // gone from the API on day 2
      ],
    );
    assert.deepEqual(
      (await read('votes')).map((v) => [v.VoteId, v.VoteDescription]),
      [
        ['vot_34th_H0001', 'Typed by hand'],
        ['vot_34th_H0002', ''],
      ],
    );
    assert.deepEqual(
      (await read('choices_history')).map((c) => [c.VoteId, c.MemberId, c.Choice, c.ValidFrom, c.ValidTo]),
      [
        ['vot_34th_H0001', 'mem_34th_H_ann-lee-33rd', 'Y', '2026-04-01', null],
        ['vot_34th_H0001', 'mem_34th_H_bob-roe-34th', 'N', '2026-04-01', null],
        ['vot_34th_H0002', 'mem_34th_H_ann-lee-33rd', 'N', '2026-04-02', null],
      ],
    );
    assert.deepEqual(
      (await read('bill_versions')).map((v) => [v.BillVersionId, v.BillVersionFullText]),
      [
        ['ver_34th_HB0001_A', 'HOUSE BILL NO. 1 A'],
        ['ver_34th_HB0001_B', 'HOUSE BILL NO. 1 B'],
      ],
    );
    assert.deepEqual(
      (await read('people_history')).map((p) => [p.PersonId, p.ValidFrom, p.ValidTo]),
      [
        ['per_ann-lee-33rd', '2026-04-01', null],
        ['per_bob-roe-34th', '2026-04-01', null],
      ],
    );

    // As of day 1, from the same store.
    const day1 = join(dir, 'day1');
    await build({ raw: new RawStore(store), curated, outDir: day1, date: '2026-04-01', logger: silent });
    assert.deepEqual(
      (await readCsvAll(join(day1, 'bills.csv'))).map((b) => b.BillShortTitle),
      ['OLD TITLE', 'WITHDRAWN LATER'],
    );

    // Publishing copies the files and prunes old dated sets.
    await store.put('published/2026-02-01/bills.csv', new Uint8Array([1]));
    await store.put('published/latest/old.csv', new Uint8Array([1]));
    const { deleted } = await publish({ store, dir: out, date: '2026-04-05', now, logger: silent });
    assert.deepEqual(deleted, ['published/2026-02-01/bills.csv', 'published/latest/old.csv']);
    const published = await store.list('published/');
    assert.ok(published.includes('published/2026-04-05/bills_history.csv'));
    assert.ok(published.includes('published/latest/choices.csv'));
  });

  it('picks legislatures to fetch', () => {
    const e = (leg: number) => ({
      date: 'd',
      time: 't',
      url: 'u',
      query: null,
      range: null,
      status: 200,
      hash: 'h',
      legislature: leg,
    });
    const index = new RawIndex(Array.from({ length: 23 }, (_, i) => e(12 + i)));
    assert.deepEqual(defaultLegislatures(index, 34, false), [34]);
    assert.deepEqual(defaultLegislatures(index, 35, false), [34, 35]);
    assert.equal(defaultLegislatures(index, 34, true).length, 23);
    assert.equal(defaultLegislatures(new RawIndex([]), 34, false).length, 23);
  });
});
