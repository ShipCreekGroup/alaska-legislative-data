// Contract tests: call the live API and check the responses still match the JSON Schema.
// Skipped unless LIVE=1 (`npm run test:live`). Run daily in CI to notice API changes early.
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { BasisClient, BasisError, currentLegislature, type Endpoint, type RequestParams } from '../src/client.ts';
import { describeErrors, validator } from './validate.ts';

const live = process.env.LIVE === '1';
const leg = currentLegislature();
const client = new BasisClient({ maxConcurrency: 3 });

const RESPONSE_TYPE: Record<Endpoint, string> = {
  bills: 'BillsResponse',
  committees: 'CommitteesResponse',
  meetings: 'MeetingsResponse',
  members: 'MembersResponse',
  sessions: 'SessionsResponse',
};

const CASES: Array<[Endpoint, RequestParams<object>]> = [
  ['bills', { session: leg, query: { subjects: true } }],
  ['bills', { session: 13, query: { subjects: true }, range: 50 }],
  ['bills', { session: 34, query: { bills: { bill: 'HB 16' }, versions: { fulltext: 'urlonly' }, sponsors: true, actions: true, fiscalnotes: true, votes: true, subjects: true } }],
  ['committees', { session: leg }],
  ['committees', { session: leg, query: { committees: { code: 'HSS' }, members: true, meetings: true, bills: true } }],
  ['meetings', { session: leg, range: 50 }],
  ['meetings', { session: leg, query: { meetings: true, media: true, minutes: { fulltext: 'urlonly' } }, range: 10 }],
  ['members', { session: leg }],
  ['members', { session: 19 }],
  ['members', { session: leg, query: { members: { chamber: 'S' }, committees: true, bills: true, votes: true }, range: 2 }],
  ['sessions', { session: leg }],
  ['sessions', { session: 13 }],
  ['sessions', { session: leg, query: { journals: { startdate: '2025-01-21', enddate: '2025-01-23' } } }],
];

describe('live API matches schema/basis.schema.json', { skip: !live && 'set LIVE=1 to run' }, () => {
  for (const [endpoint, params] of CASES) {
    it(`${endpoint} ${JSON.stringify(params)}`, async () => {
      const res = await client.get(endpoint, params);
      const v = validator(RESPONSE_TYPE[endpoint]);
      assert.ok(v({ Basis: res.data }), describeErrors(v));
    });
  }

  it('reports missing legislatures as unavailable', async () => {
    const err = await client.members({ session: leg + 1 }).catch((e) => e);
    assert.ok(err instanceof BasisError && err.isDataUnavailable, String(err));
  });

  it('counts', async () => {
    assert.ok((await client.count('members', { session: leg })) >= 60);
  });

  it('serves bill text', async () => {
    const text = await client.billText(34, 'HB 16', 'A');
    assert.match(text, /HOUSE BILL NO\. 16/);
  });
});
