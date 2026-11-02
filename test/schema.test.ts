import assert from 'node:assert/strict';
import { readdirSync, readFileSync } from 'node:fs';
import { describe, it } from 'node:test';
import { generateTypes } from '../scripts/gen-types.ts';
import { fixture } from './helpers.ts';
import { validator } from './validate.ts';

const RESPONSE_TYPE: Record<string, string> = {
  bill: 'BillsResponse',
  committee: 'CommitteesResponse',
  meeting: 'MeetingsResponse',
  member: 'MembersResponse',
  session: 'SessionsResponse',
};

describe('schema', () => {
  const files = readdirSync(new URL('./fixtures/', import.meta.url)).filter((f) => f.endsWith('.json'));
  for (const f of files) {
    it(`matches recorded response ${f}`, () => {
      const prefix = Object.keys(RESPONSE_TYPE).find((p) => f.startsWith(p));
      assert.ok(prefix, `no response type for ${f}`);
      const validate = validator(RESPONSE_TYPE[prefix]!);
      assert.ok(validate(JSON.parse(fixture(f))), JSON.stringify(validate.errors?.slice(0, 5), null, 2));
    });
  }

  it('rejects unknown fields', () => {
    const v = validator('Committee');
    const c: Record<string, unknown> = JSON.parse(fixture('committees.json')).Basis.Committees[0];
    assert.ok(v(c));
    assert.ok(!v({ ...c, NewField: 1 }));
  });

  it('src/types.gen.ts is up to date (run `npm run gen`)', async () => {
    const committed = readFileSync(new URL('../src/types.gen.ts', import.meta.url), 'utf8');
    assert.equal(committed, await generateTypes());
  });
});
