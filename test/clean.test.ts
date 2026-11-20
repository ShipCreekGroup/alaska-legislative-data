import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, it } from 'node:test';
import type { Member } from '../src/types.gen.ts';
import {
  checkCurated,
  cleanBill,
  cleanMember,
  cleanPerson,
  fixBillNumber,
  fixString,
  parseAmendmentNumber,
  parseIsoDate,
  splitVotes,
  type MemberRow,
} from '../src/scrape/clean.ts';
import { formatCell, readCsvAll, writeCsv } from '../src/scrape/csv.ts';
import { fixture } from './helpers.ts';

describe('cleaning helpers', () => {
  it('fixString trims, normalizes line breaks, and nulls empties', () => {
    assert.equal(fixString('  CSHB 7(JUD)\r\nThird Reading\r\n'), 'CSHB 7(JUD)\nThird Reading');
    assert.equal(fixString('   '), null);
    assert.equal(fixString(null), null);
    assert.equal(fixBillNumber('HB   16 '), 'HB 16');
  });

  it('parses amendment numbers (same cases as the Python version)', () => {
    assert.equal(parseAmendmentNumber('foo'), null);
    assert.equal(parseAmendmentNumber('Amendment No. 1'), '1.0');
    assert.equal(parseAmendmentNumber('Amendment No. 2'), '2.0');
    assert.equal(parseAmendmentNumber('Amendment No. 1 to Amendment No. 3'), '3.1');
    assert.equal(parseAmendmentNumber('Amendment No. 2 to Amendment No. 3'), '3.2');
    assert.equal(parseAmendmentNumber('Amendment to Amendment No. 3'), '3.1');
    assert.equal(parseAmendmentNumber('CSHB 16(STA)\nSecond Reading\nAmendment No. 1'), '1.0');
  });

  it('parses dates, rejecting the odd old formats', () => {
    assert.equal(parseIsoDate('2025-01-22'), '2025-01-22');
    assert.equal(parseIsoDate('Jul -10- 1'), null);
    assert.equal(parseIsoDate('2025-02-30'), null);
    assert.equal(parseIsoDate(''), null);
  });
});

describe('API objects to rows', () => {
  it('cleans bills', () => {
    const [raw] = JSON.parse(fixture('bill_full.json')).Basis.Bills;
    const row = cleanBill(34, raw);
    assert.equal(row.BillId, '34:HB 16');
    assert.equal(row.BillNumber, 'HB 16');
    assert.equal(row.BillName, 'CSHB 16(STA) AM S(EFD FLD H)');
    assert.equal(row.BillStatusDate, '2026-07-09');
    assert.equal(row.BillVetoed, true);
    assert.ok(row.BillSubjects.includes('ELECTIONS'));
  });

  it('splits member votes into votes and choices', () => {
    const [member] = JSON.parse(fixture('member_full.json')).Basis.Members as Member[];
    const members: MemberRow[] = [
      cleanMember({
        LegislatureNumber: '34', PersonId: 'Robyn Burke:34', MemberCode: 'FRB', MemberFormerCodes: 'BUR',
        MemberChamber: 'H', MemberDistrict: '40', MemberParty: 'D', MemberIsMajority: 'TRUE',
      }),
    ];
    const raw = member!.Votes!.map((vote) => ({ leg: 34, vote }));
    const { votes, choices, unmatched } = splitVotes(raw, new Set(['34:SB 113']), members);
    assert.equal(unmatched.length, 0);
    assert.equal(votes.length, raw.length);
    assert.equal(choices.length, raw.length);
    const override = votes.find((v) => v.VoteId === '34:H:396')!;
    assert.equal(override.BillId, '34:SB 113');
    assert.equal(override.VoteTitle, 'HOUSE\nSB 113\nVeto Override');
    const c = choices.find((c) => c.VoteId === '34:H:396')!;
    assert.equal(c.ChoiceId, '34:H:396:H:40:Robyn Burke:34');
    assert.equal(c.MemberId, '34:H:40:Robyn Burke:34');
    assert.ok(['Y', 'N', 'E', 'A'].includes(c.Choice!));
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
  it('bundled people.csv and members.csv are consistent', async () => {
    const people = (await readCsvAll('data/curated/people.csv')).map(cleanPerson);
    const members = (await readCsvAll('data/curated/members.csv')).map(cleanMember);
    assert.ok(people.length > 500);
    assert.ok(members.length > 2000);
    checkCurated(people, members);
  });

  it('catches a member code used twice in one legislature', () => {
    const p = cleanPerson({ PersonId: 'A B:1', PersonFullName: 'A B', PersonFirstName: 'A', PersonLastName: 'B' });
    const m = (d: string) => cleanMember({ LegislatureNumber: '1', PersonId: 'A B:1', MemberChamber: 'H', MemberDistrict: d, MemberCode: 'ABC' });
    assert.throws(() => checkCurated([p], [m('1'), m('2')]), /used twice/);
  });
});

describe('csv', () => {
  it('quotes like DuckDB', () => {
    assert.equal(formatCell(null), '');
    assert.equal(formatCell(''), '""');
    assert.equal(formatCell('a,b'), '"a,b"');
    assert.equal(formatCell('say "hi"'), '"say ""hi"""');
    assert.equal(formatCell('two\nlines'), '"two\nlines"');
    assert.equal(formatCell(true), 'true');
    assert.equal(formatCell(['A', 'B & C']), '"[A, B & C]"');
    assert.equal(formatCell([]), '[]');
  });

  it('round-trips awkward values', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'akleg-'));
    const path = join(dir, 't.csv');
    const rows = [
      { a: 'plain', b: null, c: '' },
      { a: 'quote " and, comma', b: 'multi\r\nline\n', c: '\x16\x10control\x11 \x17' },
      { a: 'ñ unicode', b: '"', c: ',' },
    ];
    await writeCsv(path, ['a', 'b', 'c'], rows);
    assert.deepEqual(await readCsvAll(path), rows);
    writeFileSync(path, 'x,y\r\n1,"a""b"\r\n2,\r\n');
    assert.deepEqual(await readCsvAll(path), [{ x: '1', y: 'a"b' }, { x: '2', y: null }]);
    assert.ok(readFileSync(path, 'utf8').length > 0);
  });
});
