import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';
import { generateCurated } from '../scripts/gen.ts';
import { createBasis } from '../src/client.ts';
import { readCsvAll } from '../src/build/csv.ts';
import {
  amendmentId,
  billCode,
  billId,
  billVersionId,
  bundledCurated,
  checkCurated,
  cleanBill,
  curatedFromCsv,
  createTables,
  decodeCurated,
  encodeCurated,
  fixBillNumber,
  fixString,
  memberId,
  ordinal,
  parseAmendmentNumber,
  parseIsoDate,
  parseOrdinal,
  personId,
  slugify,
  splitVotes,
  voteId,
  type Curated,
} from '../src/tables/index.ts';
import type { Member } from '../src/types.gen.ts';
import { fakeFetch, fixture, noSleep, response, silent } from './helpers.ts';

describe('ids', () => {
  it('writes legislatures as two-digit ordinals', () => {
    const cases: Array<[number, string]> = [[1, '01st'], [2, '02nd'], [3, '03rd'], [4, '04th'], [11, '11th'], [12, '12th'], [13, '13th'], [21, '21st'], [22, '22nd'], [23, '23rd'], [34, '34th'], [99, '99th']];
    for (const [n, s] of cases) {
      assert.equal(ordinal(n), s);
      assert.equal(parseOrdinal(s), n);
    }
    assert.throws(() => ordinal(100));
    assert.throws(() => parseOrdinal('34st'));
  });

  it('makes slugs', () => {
    assert.equal(slugify('Cathy Muñoz'), 'cathy-munoz');
    assert.equal(slugify('Steve St. Clair'), 'steve-st-clair');
    assert.equal(slugify("John O'Connell"), 'john-oconnell');
    assert.equal(slugify('Elton Engstrom Jr'), 'elton-engstrom-jr');
    assert.equal(slugify('Löki Tobin'), 'loki-tobin');
    assert.equal(slugify('Colleen Sullivan-Leonard'), 'colleen-sullivan-leonard');
  });

  it('builds every kind of ID', () => {
    const person = personId('Robyn Burke', 34);
    assert.equal(person, 'per_robyn-burke-34th');
    assert.equal(memberId(34, 'H', person), 'mem_34th_H_robyn-burke-34th');
    assert.equal(billCode('HB   16'), 'HB0016');
    assert.equal(billCode('HSCR 1'), 'HSCR0001');
    assert.equal(billCode('SB1003'), 'SB1003');
    assert.equal(billCode('5B 152'), '5B152'); // garbled in the API
    assert.equal(billCode('sa 116'), 'SA116');
    assert.equal(billCode('S8 F,S'), 'S8FS');
    assert.equal(billId(34, 'HB 16'), 'bil_34th_HB0016');
    assert.equal(billVersionId(34, 'HB 16', 'A'), 'ver_34th_HB0016_A');
    assert.equal(voteId(34, 'H0396'), 'vot_34th_H0396');
    assert.equal(voteId(34, 'S 12'), 'vot_34th_S0012');
    assert.equal(amendmentId(34, 'HB 16', 'H', 11), 'amd_34th_HB0016_H011');
    assert.equal(amendmentId(34, 'HB 16', 'H', 2, 10), 'amd_34th_HB0016_H002_10');
    assert.throws(() => memberId(34, 'H', 'Robyn Burke:34'));
  });

  it('sorts as text in the natural order', () => {
    const ids = [billId(34, 'HB 100'), billId(34, 'HB 2'), billId(9, 'HB 1'), billId(34, 'HB 16')];
    assert.deepEqual([...ids].sort(), ['bil_09th_HB0001', 'bil_34th_HB0002', 'bil_34th_HB0016', 'bil_34th_HB0100']);
  });
});

describe('cleaning', () => {
  it('fixString trims, normalizes line breaks, and nulls empties', () => {
    assert.equal(fixString('  CSHB 7(JUD)\r\nThird Reading\r\n'), 'CSHB 7(JUD)\nThird Reading');
    assert.equal(fixString('   '), null);
    assert.equal(fixString(null), null);
    assert.equal(fixBillNumber('HB   16 '), 'HB 16');
  });

  it('parses amendment numbers', () => {
    assert.equal(parseAmendmentNumber('foo'), null);
    assert.deepEqual(parseAmendmentNumber('Amendment No. 1'), { number: 1, sub: null });
    assert.deepEqual(parseAmendmentNumber('Amendment No. 1 to Amendment No. 3'), { number: 3, sub: 1 });
    assert.deepEqual(parseAmendmentNumber('Amendment No. 10 to Amendment No. 2'), { number: 2, sub: 10 });
    assert.deepEqual(parseAmendmentNumber('Amendment to Amendment No. 3'), { number: 3, sub: 1 });
    assert.deepEqual(parseAmendmentNumber('CSHB 16(STA)\nSecond Reading\nAmendment No. 1'), { number: 1, sub: null });
    assert.deepEqual(parseAmendmentNumber('CSHB 263(FIN) am\nSecond Reading\nAmendment No. 2 to Amendment No. 70 as amended'), { number: 70, sub: 2 });
    assert.deepEqual(parseAmendmentNumber('X\nAmendment No. 2 to Amendment No.  2 as amended'), { number: 2, sub: 2 });
    assert.deepEqual(parseAmendmentNumber('X\nAmendment No. 4/Table'), { number: 4, sub: null });
    assert.deepEqual(parseAmendmentNumber('X\nRescind Previous Action/Amendment No. 7'), { number: 7, sub: null });
    assert.deepEqual(parseAmendmentNumber('X\nAmendment to SB 12?'), null);
    assert.deepEqual(parseAmendmentNumber('Amendment No. 3\nThird Reading'), null); // only the last line counts
  });

  it('parses dates, rejecting the odd old formats', () => {
    assert.equal(parseIsoDate('2025-01-22'), '2025-01-22');
    assert.equal(parseIsoDate('Jul -10- 1'), null);
    assert.equal(parseIsoDate('2025-02-30'), null);
    assert.equal(parseIsoDate(''), null);
  });

  it('cleans bills', () => {
    const [raw] = JSON.parse(fixture('bill_full.json')).Basis.Bills;
    const row = cleanBill(34, raw);
    assert.equal(row.BillId, 'bil_34th_HB0016');
    assert.equal(row.BillNumber, 'HB 16');
    assert.equal(row.BillName, 'CSHB 16(STA) AM S(EFD FLD H)');
    assert.equal(row.BillStatusDate, '2026-07-09');
    assert.equal(row.BillVetoed, true);
    assert.ok(row.BillSubjects.includes('ELECTIONS'));
  });

  it('splits member votes into votes and choices', () => {
    const [member] = JSON.parse(fixture('member_full.json')).Basis.Members as Member[];
    const curated = curatedFromCsv(
      [{ PersonId: 'per_robyn-burke-34th', PersonFullName: 'Robyn Frier', PersonFirstName: 'Robyn', PersonLastName: 'Frier' }],
      [{ LegislatureNumber: '34', PersonId: 'per_robyn-burke-34th', MemberCode: 'FRB', MemberFormerCodes: 'BUR', MemberChamber: 'H', MemberDistrict: '40', MemberParty: 'D', MemberIsMajority: 'TRUE' }],
    );
    const raw = member!.Votes!.map((vote) => ({ leg: 34, vote }));
    const { votes, choices, unmatched } = splitVotes(raw, new Set(['bil_34th_SB0113']), curated.members);
    assert.equal(unmatched.length, 0);
    assert.equal(votes.length, raw.length);
    assert.equal(choices.length, raw.length);
    const override = votes.find((v) => v.VoteId === 'vot_34th_H0396')!;
    assert.equal(override.BillId, 'bil_34th_SB0113');
    assert.equal(override.VoteTitle, 'HOUSE\nSB 113\nVeto Override');
    assert.equal(override.VoteAmendmentId, null);
    const c = choices.find((c) => c.VoteId === 'vot_34th_H0396')!;
    assert.deepEqual(Object.keys(c), ['VoteId', 'MemberId', 'Choice']);
    assert.equal(c.MemberId, 'mem_34th_H_robyn-burke-34th');
    assert.ok(['Y', 'N', 'E', 'A'].includes(c.Choice!));
    const amendment = votes.find((v) => v.VoteSubAmendmentNumber !== null) ?? votes.find((v) => v.VoteAmendmentNumber !== null)!;
    assert.match(amendment.VoteAmendmentId!, /^amd_34th_[A-Z]+\d{4}_H\d{3}(_\d{2})?$/);
  });

  it('reports votes by unknown member codes', () => {
    const vote = { Vote: 'Y ', VoteNum: 'H0001', Bill: null, Member: 'ZZZ', MemberParty: 'R', MemberChamber: 'H' as const, MemberName: 'X', Title: 'T', VoteDate: '2025-01-01' };
    const r = splitVotes([{ leg: 34, vote }], new Set(), []);
    assert.equal(r.votes.length, 1);
    assert.equal(r.choices.length, 0);
    assert.deepEqual(r.unmatched, [{ leg: 34, code: 'ZZZ', voteNum: 'H0001' }]);
  });
});

describe('curated data', () => {
  const fromCsv = async (): Promise<Curated> =>
    curatedFromCsv(
      await readCsvAll('data/curated/people.csv'),
      await readCsvAll('data/curated/members.csv'),
      await readCsvAll('data/curated/vote_descriptions.csv'),
    );

  it('people.csv and members.csv are consistent', async () => {
    const c = await fromCsv();
    assert.ok(c.people.length > 500);
    assert.ok(c.members.length > 2000);
    assert.ok(c.people.some((p) => p.PersonId === 'per_robyn-burke-34th' && p.PersonFullName === 'Robyn Frier'));
  });

  it('the bundled copy matches the CSVs (run `npm run gen`)', async () => {
    assert.equal(readFileSync(new URL('../src/tables/curated.gen.ts', import.meta.url), 'utf8'), await generateCurated());
    assert.deepEqual(bundledCurated(), await fromCsv());
  });

  it('round-trips the compact form', async () => {
    const c = await fromCsv();
    assert.deepEqual(decodeCurated(encodeCurated(c)), c);
  });

  it('catches problems', () => {
    const p = { PersonId: 'per_a-b-01st', PersonFullName: 'A B', PersonFirstName: 'A', PersonLastName: 'B' };
    const m = (d: string, code = 'ABC', chamber = 'H') => ({ LegislatureNumber: '1', PersonId: 'per_a-b-01st', MemberChamber: chamber, MemberDistrict: d, MemberCode: code });
    assert.throws(() => curatedFromCsv([p], [m('1'), m('2', 'ABC', 'S')]), /used twice/);
    assert.throws(() => curatedFromCsv([p], [m('1'), m('2', 'XYZ')]), /Duplicate MemberId/);
    assert.throws(() => curatedFromCsv([p, p], []), /Duplicate PersonId/);
    assert.throws(() => curatedFromCsv([{ ...p, PersonId: 'A B:1' }], []), /must look like/);
    assert.throws(() => checkCurated({ people: [], members: curatedFromCsv([p], [m('1')]).members, voteDescriptions: [] }), /unknown PersonId/);
    assert.throws(() => curatedFromCsv([p], [], [{ VoteId: '34:H:64', VoteDescription: 'x' }]), /bad VoteId/);
  });
});

describe('createTables', () => {
  const curated = curatedFromCsv(
    [
      { PersonId: 'per_ann-lee-33rd', PersonFullName: 'Ann Lee', PersonFirstName: 'Ann', PersonLastName: 'Lee' },
      { PersonId: 'per_bob-roe-34th', PersonFullName: 'Bob Roe', PersonFirstName: 'Bob', PersonLastName: 'Roe' },
    ],
    [
      { LegislatureNumber: '33', PersonId: 'per_ann-lee-33rd', MemberCode: 'LEE', MemberChamber: 'H', MemberDistrict: '1' },
      { LegislatureNumber: '34', PersonId: 'per_ann-lee-33rd', MemberCode: 'LEA', MemberFormerCodes: 'LEE', MemberChamber: 'H', MemberDistrict: '1' },
      { LegislatureNumber: '34', PersonId: 'per_bob-roe-34th', MemberCode: 'ROE', MemberChamber: 'H', MemberDistrict: '2' },
    ],
  );
  const bill = (number: string) => ({ BillNumber: number, BillName: '', ShortTitle: `TITLE ${number}`, StatusDate: '2026-04-01', Vetoed: false, Subjects: [] });
  const vote = (member: string, num: string, choice: string, title: string) => ({
    Vote: `${choice} `, VoteNum: num, Bill: 'HB   1', Member: member, MemberParty: 'R', MemberChamber: 'H', MemberName: 'X', Title: title, VoteDate: '2026-04-02',
  });
  const fetch = fakeFetch((url, init) => {
    const q = init.headers['X-Alaska-Legislature-Basis-Query'] ?? '';
    if (url.includes('Plaintext')) return response('\r\n00 HOUSE BILL NO. 1\r\n');
    if (url.includes('/basis/bills') && q === 'subjects') return response(JSON.stringify({ Basis: { Bills: [bill('HB   2'), bill('HB   1')] } }));
    if (url.includes('/basis/bills')) {
      const versions = ['A', 'B'].map((l) => ({ VersionLetter: l, Title: 'An Act', Name: `HB 1${l}`, IntroDate: '2025-01-22', PassedHouse: null, PassedSenate: null, WorkOrder: '34-LS0001', Url: `https://x/HB0001${l}.PDF`, Mime: null, Encoding: null, Data: null }));
      return response(JSON.stringify({ Basis: { Bills: [{ ...bill('HB   1'), Versions: versions }] } }));
    }
    const code = /code=(\w+)/.exec(q)![1]!;
    const votes = {
      LEA: [vote('LEA', 'H0001', 'Y', 'HB 1\r\nSecond Reading\r\nAmendment No. 10 to Amendment No. 2\r\n')],
      LEE: [vote('LEE', 'H0002', 'N', 'HB 1\r\nThird Reading\r\n')],
      ROE: [vote('ROE', 'H0001', 'N', 'HB 1\r\nSecond Reading\r\nAmendment No. 10 to Amendment No. 2\r\n')],
    }[code] ?? [];
    return response(JSON.stringify({ Basis: { Members: [{ Votes: votes }] } }));
  });
  const tables = createTables(createBasis({ fetch, sleep: noSleep, logger: silent }), { curated, logger: silent });

  it('returns people and members', async () => {
    assert.equal((await tables.people()).length, 2);
    assert.deepEqual((await tables.members({ legislature: 34 })).map((m) => m.MemberId), ['mem_34th_H_ann-lee-33rd', 'mem_34th_H_bob-roe-34th']);
    assert.equal((await tables.members()).length, 3);
  });

  it('returns bills, sorted by ID', async () => {
    assert.deepEqual((await tables.bills({ legislature: 34 })).map((b) => b.BillId), ['bil_34th_HB0001', 'bil_34th_HB0002']);
  });

  it('returns votes and choices', async () => {
    const { votes, choices } = await tables.votes({ legislature: 34 });
    assert.deepEqual(votes.map((v) => [v.VoteId, v.BillId, v.VoteAmendmentNumber, v.VoteSubAmendmentNumber, v.VoteAmendmentId]), [
      ['vot_34th_H0001', 'bil_34th_HB0001', 2, 10, 'amd_34th_HB0001_H002_10'],
      ['vot_34th_H0002', 'bil_34th_HB0001', null, null, null],
    ]);
    assert.deepEqual(choices, [
      { VoteId: 'vot_34th_H0001', MemberId: 'mem_34th_H_ann-lee-33rd', Choice: 'Y' },
      { VoteId: 'vot_34th_H0001', MemberId: 'mem_34th_H_bob-roe-34th', Choice: 'N' },
      { VoteId: 'vot_34th_H0002', MemberId: 'mem_34th_H_ann-lee-33rd', Choice: 'N' },
    ]);
    assert.deepEqual(await tables.votes({ legislature: 18 }), { votes: [], choices: [] });
  });

  it('returns bill versions with text, skipping known ones', async () => {
    const versions = await tables.billVersions({ legislature: 34, bills: ['HB 1'], skip: new Set(['ver_34th_HB0001_A']) });
    assert.deepEqual(versions.map((v) => [v.BillVersionId, v.BillId, v.BillVersionFullText]), [['ver_34th_HB0001_B', 'bil_34th_HB0001', 'HOUSE BILL NO. 1']]);
  });

  it('uses the bundled lists by default', async () => {
    const t = createTables(createBasis({ fetch }));
    assert.ok((await t.people()).length > 500);
  });
});
