// End-to-end test of `scrape` against a fake API and a small baseline.
import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, it } from 'node:test';
import { createBasis } from '../src/client.ts';
import { readCsvAll } from '../src/scrape/csv.ts';
import { curatedFromCsv } from '../src/tables/index.ts';
import { legislaturesToRefresh, scrape } from '../src/scrape/scrape.ts';
import { fakeFetch, noSleep, response, silent } from './helpers.ts';

function bill(number: string, extra: Record<string, unknown> = {}) {
  return {
    Documents: [], PartialVeto: false, Vetoed: false, BillNumber: number, BillName: '', ShortTitle: `TITLE ${number}`,
    StatusCode: '002', StatusText: '(H) STA', Flag1: ' ', Flag2: ' ', StatusDate: '2026-04-01', StatusAndThen: [],
    StatusSummaryCode: ' ', OnFloor: ' ', NotKnown: ' ', Filler: ' ', Lock: ' ', AllMeetings: [], Meetings: [],
    ManifestErrors: [], Statutes: [], SponsorUrl: '', ...extra,
  };
}

function vote(member: string, num: string, choice: string, billNumber: string | null) {
  return {
    Vote: `${choice} `, VoteNum: num, Bill: billNumber, Member: member, MemberParty: 'R', MemberChamber: 'H',
    MemberName: 'Name      ', Title: `${billNumber ?? 'CONFIRM'}\r\nThird Reading\r\nAmendment No. 2\r\n`, VoteDate: '2026-04-02',
  };
}

// The baseline is in the format published before the new IDs, to check it's converted.
function setup() {
  const root = mkdtempSync(join(tmpdir(), 'akleg-scrape-'));
  const baseline = join(root, 'baseline');
  mkdirSync(baseline);
  writeFileSync(
    join(baseline, 'bills.csv'),
    'BillId,LegislatureNumber,BillNumber,BillName,BillShortTitle,BillStatusDate,BillSubjects,BillVetoed\n' +
      '33:HB 5,33,HB 5,,OLD BILL,2024-05-01,"[FUNDS, TAXATION]",false\n' +
      '34:HB 1,34,HB 1,,STALE TITLE,2025-01-22,[],false\n',
  );
  writeFileSync(
    join(baseline, 'votes.csv'),
    'VoteId,BillId,VoteNumber,VoteDate,VoteTitle,LegislatureNumber,VoteChamber,VoteDescription,VoteBillAmendmentNumber\n' +
      '33:H:9,33:HB 5,9,2024-04-01,"HB 5\nThird Reading\nAmendment No. 4",33,H,"",4.0\n' +
      '34:H:1,34:HB 1,1,2026-04-02,old,34,H,Hand-written note,\n',
  );
  writeFileSync(
    join(baseline, 'choices.csv'),
    'ChoiceId,VoteId,MemberId,Choice\n33:H:9:H:1:Ann Lee:33,33:H:9,33:H:1:Ann Lee:33,Y\n34:H:1:H:1:Ann Lee:33,34:H:1,34:H:1:Ann Lee:33,N\n',
  );
  writeFileSync(
    join(baseline, 'bill_versions.csv'),
    'BillVersionId,BillId,BillVersionLetter,BillVersionTitle,BillVersionName,BillVersionIntroDate,BillVersionPassedHouse,BillVersionPassedSenate,BillVersionWorkOrder,BillVersionPdfUrl,BillVersionFullText\n' +
      '34:HB 1:A,34:HB 1,A,"""An Act"" ",HB 1,2025-01-22,,,34-LS0001,https://x/HB0001A.PDF,"  HOUSE BILL NO. 1\n""An Act"""\n',
  );
  return { root, baseline, out: join(root, 'out') };
}

const curated = curatedFromCsv(
  [
    { PersonId: 'per_ann-lee-33rd', PersonFullName: 'Ann Lee', PersonFirstName: 'Ann', PersonLastName: 'Lee' },
    { PersonId: 'per_bob-roe-34th', PersonFullName: 'Bob Roe', PersonFirstName: 'Bob', PersonLastName: 'Roe' },
  ],
  [
    { LegislatureNumber: '33', PersonId: 'per_ann-lee-33rd', MemberCode: 'LEE', MemberChamber: 'H', MemberDistrict: '1', MemberParty: 'R', MemberIsMajority: 'TRUE' },
    { LegislatureNumber: '34', PersonId: 'per_ann-lee-33rd', MemberCode: 'LEA', MemberFormerCodes: 'LEE', MemberChamber: 'H', MemberDistrict: '1', MemberParty: 'R', MemberIsMajority: 'FALSE' },
    { LegislatureNumber: '34', PersonId: 'per_bob-roe-34th', MemberCode: 'ROE', MemberChamber: 'H', MemberDistrict: '2', MemberParty: 'D', MemberIsMajority: 'TRUE' },
  ],
);

describe('scrape', () => {
  it('picks which legislatures to refresh', () => {
    const all = new Set([12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34]);
    assert.deepEqual(legislaturesToRefresh(all, 34), [34]);
    assert.deepEqual(legislaturesToRefresh(all, 35), [34, 35]);
    all.delete(20);
    assert.deepEqual(legislaturesToRefresh(all, 34), [20, 34]);
    assert.equal(legislaturesToRefresh(new Set(), 34).length, 23);
  });

  it('refreshes the latest legislature and keeps the rest of the baseline', async () => {
    const dirs = setup();
    const fetch = fakeFetch((url, init) => {
      const q = init.headers['X-Alaska-Legislature-Basis-Query'] ?? '';
      if (url.includes('/Plaintext/34?Hsid=HB0001B')) return response('\r\n00 HOUSE BILL NO. 1 B\r\n01 text\r\n');
      if (url.includes('/basis/bills') && q === 'subjects') {
        return response(JSON.stringify({ Basis: { Bills: [bill('HB   1', { ShortTitle: 'FRESH TITLE', Subjects: ['ELECTIONS'] }), bill('HB   2')] } }));
      }
      if (url.includes('/basis/bills') && q.startsWith('bills;bill=')) {
        const number = /bill=([^,]+)/.exec(q)![1]!;
        const versions = number === 'HB 1'
          ? ['A', 'B'].map((l) => ({ VersionLetter: l, Title: 'An Act', Name: `HB 1${l}`, IntroDate: '2025-01-22', PassedHouse: null, PassedSenate: null, WorkOrder: '34-LS0001', Url: `https://x/HB0001${l}.PDF`, Mime: null, Encoding: null, Data: null }))
          : [];
        return response(JSON.stringify({ Basis: { Bills: [bill(number, { Versions: versions })] } }));
      }
      if (url.includes('/basis/members')) {
        const code = /code=(\w+)/.exec(q)![1]!;
        const votes = {
          LEA: [vote('LEA', 'H0001', 'Y', 'HB   1'), vote('LEA', 'H0002', 'N', null)],
          LEE: [vote('LEE', 'H0003', 'E', 'HB   2')], // cast under the former code
          ROE: [vote('ROE', 'H0001', 'N', 'HB   1')],
        }[code] ?? [];
        return response(JSON.stringify({ Basis: { Members: [{ Votes: votes }] } }));
      }
      throw new Error(`unexpected request ${url} ${q}`);
    });
    const basis = createBasis({ fetch, sleep: noSleep, logger: silent });
    const summary = await scrape({
      basis, logger: silent, outDir: dirs.out, curated, baselineDir: dirs.baseline, legislatures: [34],
    });
    assert.deepEqual(summary.refreshed, [34]);
    assert.deepEqual(summary.warnings, []);

    const bills = await readCsvAll(join(dirs.out, 'bills.csv'));
    assert.deepEqual(bills.map((b) => [b.BillId, b.BillShortTitle, b.BillSubjects]), [
      ['bil_33rd_HB0005', 'OLD BILL', '[FUNDS, TAXATION]'],
      ['bil_34th_HB0001', 'FRESH TITLE', '[ELECTIONS]'],
      ['bil_34th_HB0002', 'TITLE HB   2', '[]'], // inner spaces in titles are kept
    ]);

    const votes = await readCsvAll(join(dirs.out, 'votes.csv'));
    assert.deepEqual(Object.keys(votes[0]!), [
      'VoteId', 'BillId', 'VoteNumber', 'VoteDate', 'VoteTitle', 'LegislatureNumber', 'VoteChamber', 'VoteDescription',
      'VoteAmendmentNumber', 'VoteSubAmendmentNumber', 'VoteAmendmentId',
    ]);
    assert.deepEqual(votes.map((v) => [v.VoteId, v.BillId, v.VoteDescription, v.VoteAmendmentNumber, v.VoteAmendmentId]), [
      ['vot_33rd_H0009', 'bil_33rd_HB0005', '', '4', 'amd_33rd_HB0005_H004'], // converted from the old format
      ['vot_34th_H0001', 'bil_34th_HB0001', 'Hand-written note', '2', 'amd_34th_HB0001_H002'],
      ['vot_34th_H0002', null, '', '2', null],
      ['vot_34th_H0003', 'bil_34th_HB0002', '', '2', 'amd_34th_HB0002_H002'],
    ]);

    const choices = await readCsvAll(join(dirs.out, 'choices.csv'));
    assert.deepEqual(choices.map((c) => [c.VoteId, c.MemberId, c.Choice]), [
      ['vot_33rd_H0009', 'mem_33rd_H_ann-lee-33rd', 'Y'],
      ['vot_34th_H0001', 'mem_34th_H_ann-lee-33rd', 'Y'],
      ['vot_34th_H0001', 'mem_34th_H_bob-roe-34th', 'N'],
      ['vot_34th_H0002', 'mem_34th_H_ann-lee-33rd', 'N'],
      ['vot_34th_H0003', 'mem_34th_H_ann-lee-33rd', 'E'],
    ]);

    const versions = await readCsvAll(join(dirs.out, 'bill_versions.csv'));
    assert.equal(versions.length, 2);
    assert.equal(versions[0]!.BillVersionFullText, '  HOUSE BILL NO. 1\n"An Act"'); // copied from baseline
    assert.equal(versions[0]!.BillVersionId, 'ver_34th_HB0001_A');
    assert.equal(versions[0]!.BillId, 'bil_34th_HB0001');
    assert.equal(versions[1]!.BillVersionId, 'ver_34th_HB0001_B');
    assert.equal(versions[1]!.BillVersionFullText, 'HOUSE BILL NO. 1 B\ntext');
    assert.equal(versions[1]!.BillVersionPassedHouse, null);

    const members = await readCsvAll(join(dirs.out, 'members.csv'));
    assert.deepEqual(members[1], {
      MemberId: 'mem_34th_H_ann-lee-33rd', LegislatureNumber: '34', PersonId: 'per_ann-lee-33rd', MemberChamber: 'H', MemberDistrict: '1',
      MemberIsMajority: 'false', MemberParty: 'R', MemberCode: 'LEA', MemberFormerCodes: 'LEE',
    });
    // Only one text download: version A was already in the baseline.
    assert.equal(fetch.calls.filter((c) => c.url.includes('Plaintext')).length, 1);

    // A release in the new format works as the next baseline and gives the same files.
    const again = join(dirs.root, 'again');
    await scrape({ basis, logger: silent, outDir: again, curated, baselineDir: dirs.out, legislatures: [34] });
    for (const f of ['people', 'members', 'bills', 'votes', 'choices', 'bill_versions']) {
      assert.equal(readFileSync(join(again, `${f}.csv`), 'utf8'), readFileSync(join(dirs.out, `${f}.csv`), 'utf8'), f);
    }
  });

  it('keeps baseline rows for a legislature the API has no data for yet', async () => {
    const dirs = setup();
    const fetch = fakeFetch((url) => {
      if (url.includes('session=35')) return response('{"Basis":{"Error":{"Code":"BaoSystemFault","Description":"Invalid Session Number."},}}');
      throw new Error(`unexpected request ${url}`);
    });
    const basis = createBasis({ fetch, sleep: noSleep, logger: silent });
    const summary = await scrape({
      basis, logger: silent, outDir: dirs.out, curated, baselineDir: dirs.baseline, legislatures: [35],
    });
    assert.equal(summary.warnings.length, 1);
    assert.equal(summary.rows.bills, 2);
    assert.equal(summary.rows.votes, 2);
    assert.equal(summary.rows.bill_versions, 1);
  });
});
