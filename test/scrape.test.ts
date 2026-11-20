// End-to-end test of `scrape` against a fake API and a small baseline.
import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, it } from 'node:test';
import { BasisClient } from '../src/client.ts';
import { readCsvAll } from '../src/scrape/csv.ts';
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

function setup() {
  const root = mkdtempSync(join(tmpdir(), 'akleg-scrape-'));
  const curated = join(root, 'curated');
  const baseline = join(root, 'baseline');
  mkdirSync(curated);
  mkdirSync(baseline);
  writeFileSync(
    join(curated, 'people.csv'),
    'PersonId,PersonFullName,PersonFirstName,PersonLastName,PersonMiddleName,PersonNickName,PersonSuffix\n' +
      'Ann Lee:33,Ann Lee,Ann,Lee,,,\nBob Roe:34,Bob Roe,Bob,Roe,,,\n',
  );
  writeFileSync(
    join(curated, 'members.csv'),
    'LegislatureNumber,PersonId,MemberCode,MemberFormerCodes,MemberChamber,MemberDistrict,MemberParty,MemberIsMajority,MemberIsActive\n' +
      '33,Ann Lee:33,LEE,,H,1,R,TRUE,TRUE\n' +
      '34,Ann Lee:33,LEA,LEE,H,1,R,FALSE,TRUE\n' +
      '34,Bob Roe:34,ROE,,H,2,D,TRUE,TRUE\n',
  );
  writeFileSync(
    join(baseline, 'bills.csv'),
    'BillId,LegislatureNumber,BillNumber,BillName,BillShortTitle,BillStatusDate,BillSubjects,BillVetoed\n' +
      '33:HB 5,33,HB 5,,OLD BILL,2024-05-01,"[FUNDS, TAXATION]",false\n' +
      '34:HB 1,34,HB 1,,STALE TITLE,2025-01-22,[],false\n',
  );
  writeFileSync(
    join(baseline, 'votes.csv'),
    'VoteId,BillId,VoteNumber,VoteDate,VoteTitle,LegislatureNumber,VoteChamber,VoteDescription,VoteBillAmendmentNumber\n' +
      '33:H:9,33:HB 5,9,2024-04-01,"HB 5\nThird Reading",33,H,"",\n' +
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
  return { root, curated, baseline, out: join(root, 'out') };
}

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
    const client = new BasisClient({ fetch, sleep: noSleep, logger: silent });
    const summary = await scrape({
      client, logger: silent, outDir: dirs.out, curatedDir: dirs.curated, baselineDir: dirs.baseline, legislatures: [34],
    });
    assert.deepEqual(summary.refreshed, [34]);
    assert.deepEqual(summary.warnings, []);

    const bills = await readCsvAll(join(dirs.out, 'bills.csv'));
    assert.deepEqual(bills.map((b) => [b.BillId, b.BillShortTitle, b.BillSubjects]), [
      ['33:HB 5', 'OLD BILL', '[FUNDS, TAXATION]'],
      ['34:HB 1', 'FRESH TITLE', '[ELECTIONS]'],
      ['34:HB 2', 'TITLE HB   2', '[]'], // inner spaces in titles are kept
    ]);

    const votes = await readCsvAll(join(dirs.out, 'votes.csv'));
    assert.deepEqual(votes.map((v) => [v.VoteId, v.BillId, v.VoteDescription, v.VoteBillAmendmentNumber]), [
      ['33:H:9', '33:HB 5', '', null],
      ['34:H:1', '34:HB 1', 'Hand-written note', '2.0'],
      ['34:H:2', null, '', '2.0'],
      ['34:H:3', '34:HB 2', '', '2.0'],
    ]);

    const choices = await readCsvAll(join(dirs.out, 'choices.csv'));
    assert.deepEqual(choices.map((c) => [c.ChoiceId, c.Choice]), [
      ['33:H:9:H:1:Ann Lee:33', 'Y'],
      ['34:H:1:H:1:Ann Lee:33', 'Y'],
      ['34:H:1:H:2:Bob Roe:34', 'N'],
      ['34:H:2:H:1:Ann Lee:33', 'N'],
      ['34:H:3:H:1:Ann Lee:33', 'E'],
    ]);

    const versions = await readCsvAll(join(dirs.out, 'bill_versions.csv'));
    assert.equal(versions.length, 2);
    assert.equal(versions[0]!.BillVersionFullText, '  HOUSE BILL NO. 1\n"An Act"'); // copied from baseline
    assert.equal(versions[1]!.BillVersionId, '34:HB 1:B');
    assert.equal(versions[1]!.BillVersionFullText, 'HOUSE BILL NO. 1 B\ntext');
    assert.equal(versions[1]!.BillVersionPassedHouse, null);

    const members = await readCsvAll(join(dirs.out, 'members.csv'));
    assert.deepEqual(members[1], {
      MemberId: '34:H:1:Ann Lee:33', LegislatureNumber: '34', PersonId: 'Ann Lee:33', MemberChamber: 'H', MemberDistrict: '1',
      MemberIsMajority: 'false', MemberParty: 'R', MemberCode: 'LEA', MemberFormerCodes: 'LEE',
    });
    // Only one text download: version A was already in the baseline.
    assert.equal(fetch.calls.filter((c) => c.url.includes('Plaintext')).length, 1);
  });

  it('keeps baseline rows for a legislature the API has no data for yet', async () => {
    const dirs = setup();
    const fetch = fakeFetch((url) => {
      if (url.includes('session=35')) return response('{"Basis":{"Error":{"Code":"BaoSystemFault","Description":"Invalid Session Number."},}}');
      throw new Error(`unexpected request ${url}`);
    });
    const client = new BasisClient({ fetch, sleep: noSleep, logger: silent });
    const summary = await scrape({
      client, logger: silent, outDir: dirs.out, curatedDir: dirs.curated, baselineDir: dirs.baseline, legislatures: [35],
    });
    assert.equal(summary.warnings.length, 1);
    assert.equal(summary.rows.bills, 2);
    assert.equal(summary.rows.votes, 2);
    assert.equal(summary.rows.bill_versions, 1);
  });
});
