import assert from 'node:assert/strict';
import { readdirSync, readFileSync } from 'node:fs';
import { describe, it } from 'node:test';
import { formatSchema, generateTypes, generateZod, readSchema } from '../scripts/gen.ts';
import type { Endpoint } from '../src/client.ts';
import * as zod from '../src/zod.ts';
import { fixture } from './helpers.ts';
import { describeErrors, validator } from './validate.ts';

const ENDPOINT: Record<string, Endpoint> = {
  bill: 'bills',
  committee: 'committees',
  meeting: 'meetings',
  member: 'members',
  session: 'sessions',
};
const RESPONSE_TYPE: Record<Endpoint, string> = {
  bills: 'BillsResponse',
  committees: 'CommitteesResponse',
  meetings: 'MeetingsResponse',
  members: 'MembersResponse',
  sessions: 'SessionsResponse',
};

const files = readdirSync(new URL('./fixtures/', import.meta.url)).filter((f) => f.endsWith('.json'));
function endpointOf(f: string): Endpoint {
  const prefix = Object.keys(ENDPOINT).find((p) => f.startsWith(p));
  assert.ok(prefix, `no endpoint for ${f}`);
  return ENDPOINT[prefix]!;
}

/** Both validators' verdicts on one body: [JSON Schema, zod]. */
function verdicts(endpoint: Endpoint, body: unknown): [boolean, boolean] {
  return [validator(RESPONSE_TYPE[endpoint])(body) as boolean, zod.validate(endpoint, body).length === 0];
}

describe('schema', () => {
  for (const f of files) {
    it(`matches recorded response ${f}`, () => {
      const v = validator(RESPONSE_TYPE[endpointOf(f)]);
      assert.ok(v(JSON.parse(fixture(f))), describeErrors(v));
    });
  }

  it('rejects unknown fields', () => {
    const v = validator('Committee');
    const c: Record<string, unknown> = JSON.parse(fixture('committees.json')).Basis.Committees[0];
    assert.ok(v(c));
    assert.ok(!v({ ...c, NewField: 1 }));
  });

  it('checks dates, times and document shapes', () => {
    const ok = (def: string, x: unknown) => validator(def)(x);
    assert.ok(ok('IsoDate', '2025-01-22'));
    assert.ok(!ok('IsoDate', '2025-02-30'));
    assert.ok(!ok('IsoDate', 'Jul -10- 1'));
    assert.ok(ok('Time', '15:15:00'));
    assert.ok(!ok('Time', '3:15 PM'));
    const doc = { Url: null, Mime: null, Encoding: null, Data: null };
    assert.ok(ok('Document', doc));
    assert.ok(ok('Document', { ...doc, Url: 'https://x' }));
    assert.ok(ok('Document', { Url: null, Mime: 'text/plain', Encoding: 'base64', Data: 'AA==' }));
    assert.ok(!ok('Document', { ...doc, Data: 'AA==' }), 'inline data without a mime type');
    const action = JSON.parse(fixture('bill_full.json')).Basis.Bills[0].Actions[0];
    assert.ok(ok('Action', { ...action, Document: { ...doc, Url: 'https://x' } }));
    assert.ok(!ok('Action', { ...action, Document: { ...doc, Extra: 1 } }));
  });

  it('is in the standard layout (run `npm run gen`)', () => {
    const committed = readFileSync(new URL('../schema/basis.schema.json', import.meta.url), 'utf8');
    assert.equal(committed, formatSchema(readSchema()));
  });

  it('src/types.gen.ts is up to date (run `npm run gen`)', async () => {
    const committed = readFileSync(new URL('../src/types.gen.ts', import.meta.url), 'utf8');
    assert.equal(committed, await generateTypes());
  });

  it('src/zod.gen.ts is up to date (run `npm run gen`)', async () => {
    const committed = readFileSync(new URL('../src/zod.gen.ts', import.meta.url), 'utf8');
    assert.equal(committed, await generateZod());
  });
});

describe('zod validators', () => {
  for (const f of files) {
    it(`accept recorded response ${f}`, () => {
      assert.deepEqual(zod.validate(endpointOf(f), JSON.parse(fixture(f))), []);
    });
  }

  it('agree with the JSON Schema on broken responses', () => {
    const bill = () => JSON.parse(fixture('bill_full.json'));
    const cases: Array<[string, Endpoint, (b: any) => unknown]> = [
      ['unknown field', 'bills', (b) => ((b.Basis.Bills[0].NewField = 1), b)],
      ['missing field', 'bills', (b) => (delete b.Basis.Bills[0].ShortTitle, b)],
      ['bad date', 'bills', (b) => ((b.Basis.Bills[0].Versions[0].IntroDate = '2025-13-01'), b)],
      ['old status date', 'bills', (b) => ((b.Basis.Bills[0].StatusDate = 'Jul -10- 1'), b)],
      ['odd vote date', 'bills', (b) => ((b.Basis.Bills[0].Votes[0].VoteDate = '-199- 0--0'), b)],
      ['bad fiscal note date', 'bills', (b) => ((b.Basis.Bills[0].FiscalNotes[0].PublishedDate = '2025-04-11'), b)],
      ['document with data but no mime', 'bills', (b) => ((b.Basis.Bills[0].Versions[0].Data = 'AA=='), b)],
      ['wrong chamber', 'bills', (b) => ((b.Basis.Bills[0].Actions[0].Chamber = 'X'), b)],
      [
        'bad meeting time',
        'meetings',
        () => {
          const m = JSON.parse(fixture('meetings.json'));
          m.Basis.Meetings[0].MeetingTime = '3:15 PM';
          return m;
        },
      ],
      [
        'minutes with an unknown field',
        'meetings',
        () => {
          const m = JSON.parse(fixture('meetings_minutes.json'));
          m.Basis.Meetings[0].Minutes.TimeStamps[0].Extra = true;
          return m;
        },
      ],
    ];
    for (const [name, endpoint, mutate] of cases) {
      const [schemaOk, zodOk] = verdicts(endpoint, mutate(bill()));
      assert.equal(zodOk, schemaOk, name);
    }
    assert.deepEqual(verdicts('bills', cases[0]![2](bill())), [false, false]);
    assert.deepEqual(verdicts('bills', cases[3]![2](bill())), [true, true]);
  });

  it('describe mismatches with a path', () => {
    const b = JSON.parse(fixture('bill_full.json'));
    b.Basis.Bills[0].NewField = 1;
    b.Basis.Bills[0].Versions[0].IntroDate = 'soon';
    assert.deepEqual(zod.validate('bills', b).sort(), [
      'Basis.Bills.*.Versions.*.IntroDate: not a valid date',
      'Basis.Bills.*: unknown fields NewField',
    ]);
  });
});
