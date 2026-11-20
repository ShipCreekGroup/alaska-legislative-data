import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
  BasisClient,
  BasisError,
  buildQuery,
  buildRange,
  buildRequest,
  cleanBillText,
  currentLegislature,
  normalizeBillNumber,
  parseFaults,
  parseMsDate,
  textBillCode,
  urlFetchAppFetch,
} from '../src/client.ts';
import { fakeFetch, fixture, noSleep, response, silent } from './helpers.ts';

describe('building requests', () => {
  it('turns query objects into the header syntax', () => {
    assert.equal(
      buildQuery({ bills: { bill: 'HB 16' }, versions: { fulltext: 'urlonly' }, sponsors: true }),
      'bills;bill=HB 16,versions;fulltext=urlonly,sponsors',
    );
    assert.equal(buildQuery({ members: { code: 'FRB', ismajority: true }, votes: true }), 'members;code=FRB;ismajority=true,votes');
    assert.equal(buildQuery('Bills; title=*Oil*'), 'Bills; title=*Oil*');
    assert.equal(buildQuery(['subjects', 'sponsors']), 'subjects,sponsors');
    assert.equal(buildQuery({}), undefined);
    assert.equal(buildQuery(''), undefined);
  });

  it('formats ranges', () => {
    assert.equal(buildRange(10), '10');
    assert.equal(buildRange({ start: 10, end: 20 }), '10..20');
    assert.equal(buildRange({ last: 30 }), '..30');
    assert.equal(buildRange('..5'), '..5');
  });

  it('builds the URL and headers', () => {
    const r = buildRequest('bills', { session: 34, chamber: 'H', query: { subjects: true }, range: 5 });
    assert.equal(r.url, 'https://www.akleg.gov/publicservice/basis/bills?minifyresult=false&json=true&session=34&chamber=H');
    assert.deepEqual(r.headers, {
      'X-Alaska-Legislature-Basis-Version': '1.4',
      'X-Alaska-Legislature-Basis-Query': 'subjects',
      'X-Alaska-Query-ResultRange': '5',
    });
    assert.deepEqual(Object.keys(buildRequest('members').headers), ['X-Alaska-Legislature-Basis-Version']);
  });
});

describe('errors', () => {
  it('parses XML faults', () => {
    const [f] = parseFaults(fixture('err_session3.xml'));
    assert.equal(f?.Code, 'FaultException');
    assert.match(f!.Description, /Contact System Admin/);
  });

  it('parses the malformed-JSON faults', () => {
    assert.deepEqual(parseFaults(fixture('err_session35.json.txt')), [
      { Code: 'BaoSystemFault', Description: 'Invalid Session Number.' },
    ]);
  });

  it('does not retry "Invalid Session Number"', async () => {
    const fetch = fakeFetch(() => response(fixture('err_session35.json.txt')));
    const client = new BasisClient({ fetch, sleep: noSleep, logger: silent });
    const err = await client.bills({ session: 35 }).catch((e) => e);
    assert.ok(err instanceof BasisError);
    assert.equal(err.isDataUnavailable, true);
    assert.equal(fetch.calls.length, 1);
  });

  it('retries server faults, then succeeds', async () => {
    const fetch = fakeFetch((_u, _i, n) => (n < 3 ? response(fixture('err_session3.xml')) : response(fixture('members.json'))));
    const waits: number[] = [];
    const client = new BasisClient({ fetch, sleep: async (ms) => void waits.push(ms), logger: silent });
    const members = await client.members({ session: 34 });
    assert.equal(members.length, 5);
    assert.deepEqual(waits, [500, 1000]);
  });

  it('gives up after maxAttempts', async () => {
    const fetch = fakeFetch(() => response('oops', 503));
    const client = new BasisClient({ fetch, sleep: noSleep, logger: silent, maxAttempts: 2 });
    await assert.rejects(client.bills(), (e: BasisError) => e.status === 503);
    assert.equal(fetch.calls.length, 2);
  });

  it('retries network errors', async () => {
    const fetch = fakeFetch((_u, _i, n) => {
      if (n === 1) throw new TypeError('fetch failed');
      return response(fixture('committees.json'));
    });
    const client = new BasisClient({ fetch, sleep: noSleep, logger: silent });
    assert.equal((await client.committees()).length, 5);
  });
});

describe('BasisClient', () => {
  it('returns each endpoint\'s list', async () => {
    const bodies: Record<string, string> = {
      bills: fixture('bills_subjects.json'),
      committees: fixture('committees.json'),
      meetings: fixture('meetings.json'),
      members: fixture('members.json'),
      sessions: fixture('session34.json'),
    };
    const fetch = fakeFetch((url) => response(bodies[/basis\/(\w+)\?/.exec(url)![1]!]!, 200, { 'X-Alaska-Query-Count': '5' }));
    const client = new BasisClient({ fetch });
    const bills = await client.bills({ session: 34, query: { subjects: true } });
    assert.equal(bills[0]!.BillNumber, 'HB   1');
    assert.ok(Array.isArray(bills[0]!.Subjects));
    assert.equal((await client.committees())[0]!.Code, 'ADM');
    assert.equal((await client.meetings()).length, 5);
    assert.equal((await client.members())[0]!.Chamber, 'H');
    assert.equal((await client.session({ session: 34 })).Number, '34');
    assert.equal((await client.get('bills')).count, 5);
    assert.equal(fetch.calls[0]!.init.headers['X-Alaska-Legislature-Basis-Query'], 'subjects');
  });

  it('counts with HEAD', async () => {
    const fetch = fakeFetch(() => response('', 200, { 'x-alaska-query-count': '856' }));
    const client = new BasisClient({ fetch });
    assert.equal(await client.count('bills', { session: 34 }), 856);
    assert.equal(fetch.calls[0]!.init.method, 'HEAD');
  });

  it('limits concurrency', async () => {
    let active = 0;
    let peak = 0;
    const fetch = fakeFetch(async () => {
      active++;
      peak = Math.max(peak, active);
      await new Promise((r) => setTimeout(r, 5));
      active--;
      return response(fixture('members.json'));
    });
    const client = new BasisClient({ fetch, maxConcurrency: 3 });
    await Promise.all(Array.from({ length: 12 }, () => client.members()));
    assert.equal(peak, 3);
    assert.equal(fetch.calls.length, 12);
  });

  it('fetches and cleans bill text', async () => {
    const raw = '\r\n00   HOUSE BILL NO. 16  \r\n01 "An Act amending\r\n\r\n02 \x16\x10law\x11 \x17\r\n';
    const fetch = fakeFetch(() => response(raw));
    const client = new BasisClient({ fetch });
    assert.equal(await client.billText(34, 'HB 16', 'A'), '  HOUSE BILL NO. 16  \n"An Act amending\n\x16\x10law\x11 \x17');
    assert.equal(fetch.calls[0]!.url, 'https://www.akleg.gov/basis/Bill/Plaintext/34?Hsid=HB0016A');
  });

  it('uses a custom base URL and extra headers', async () => {
    const fetch = fakeFetch(() => response(fixture('members.json')));
    const client = new BasisClient({ fetch, baseUrl: 'http://proxy.test/basis/', headers: { 'user-agent': 'x' } });
    await client.members();
    assert.match(fetch.calls[0]!.url, /^http:\/\/proxy\.test\/basis\/members\?/);
    assert.equal(fetch.calls[0]!.init.headers['user-agent'], 'x');
  });
});

describe('helpers', () => {
  it('normalizes bill numbers', () => {
    assert.equal(normalizeBillNumber('HB   16 '), 'HB 16');
    assert.equal(textBillCode('HB 169'), 'HB0169');
    assert.equal(textBillCode('HB3169'), 'HB3169');
    assert.equal(textBillCode('HJR 42'), 'HJR0042');
    assert.equal(textBillCode('HSCR 1'), 'HSCR0001');
  });

  it('parses /Date()/ values', () => {
    assert.equal(parseMsDate('/Date(1737450000000)/').toISOString(), '2025-01-21T09:00:00.000Z');
  });

  it('estimates the current legislature', () => {
    for (const [year, leg] of [[2021, 32], [2022, 32], [2023, 33], [2024, 33], [2025, 34], [2026, 34], [2027, 35]]) {
      assert.equal(currentLegislature(new Date(year!, 5, 1)), leg);
    }
  });

  it('splits bill text lines like Python splitlines()', () => {
    assert.equal(cleanBillText('00 a\n01 b\r02 c\x0c03 d'), 'a\nb\nc\nd');
  });
});

describe('urlFetchAppFetch', () => {
  it('adapts UrlFetchApp', async () => {
    const seen: unknown[] = [];
    const fake = {
      fetch(url: string, params: unknown) {
        seen.push([url, params]);
        return {
          getResponseCode: () => 200,
          getContentText: () => fixture('members.json'),
          getAllHeaders: () => ({ 'X-Alaska-Query-Count': '5' }),
        };
      },
    };
    const client = new BasisClient({ fetch: urlFetchAppFetch(fake) });
    assert.equal((await client.members({ session: 34 })).length, 5);
    assert.equal(await client.count('members', { session: 34 }), 5);
    const [, params] = seen[1] as [string, { method: string }];
    assert.equal(params.method, 'get'); // UrlFetchApp has no HEAD
  });
});
