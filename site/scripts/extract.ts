// Turns ak_leg.duckdb into one JSON document per page of the site:
//   meta.json, legislatures/<n>.json, bills/<BillId>.json, bill-versions/<BillVersionId>.json,
//   votes/<VoteId>.json, members/<MemberId>.json, people/<PersonId>.json, people/index.json
// File names are encodeURIComponent(id). The site reads these from disk (DATA_DIR) or from R2.
//
// Usage: node scripts/extract.ts <path/to/ak_leg.duckdb> <out dir> [--updated YYYY-MM-DD]
import { DuckDBInstance } from '@duckdb/node-api';
import { mkdir, writeFile, rm } from 'node:fs/promises';
import { join } from 'node:path';

const [dbPath, outDir] = process.argv.slice(2);
if (!dbPath || !outDir) {
  console.error('Usage: node scripts/extract.ts <ak_leg.duckdb> <out dir> [--updated YYYY-MM-DD]');
  process.exit(1);
}
const updatedArg = process.argv.indexOf('--updated');
const updated = updatedArg > 0 ? process.argv[updatedArg + 1] : new Date().toISOString().slice(0, 10);

const db = await DuckDBInstance.create(dbPath, { access_mode: 'READ_ONLY' });
const conn = await db.connect();

async function rows(sql: string): Promise<unknown[][]> {
  const reader = await conn.runAndReadAll(sql);
  return reader.getRowsJS() as unknown[][];
}

async function hasColumn(table: string, column: string): Promise<boolean> {
  const r = await rows(
    `SELECT count(*) FROM duckdb_columns() WHERE table_name = '${table}' AND column_name = '${column}'`,
  );
  return Number(r[0]![0]) > 0;
}

// Newer exports split the amendment number into three columns.
const newAmendments = await hasColumn('votes', 'VoteAmendmentId');
const amendmentFields = newAmendments
  ? `amendment: v.VoteAmendmentNumber, subAmendment: v.VoteSubAmendmentNumber, amendmentId: v.VoteAmendmentId`
  : `amendment: v.VoteBillAmendmentNumber::DOUBLE, subAmendment: NULL::INTEGER, amendmentId: NULL::VARCHAR`;

await conn.run(`
  CREATE TEMP TABLE tally AS
  SELECT VoteId,
    count(*) FILTER (WHERE Choice = 'Y') AS y, count(*) FILTER (WHERE Choice = 'N') AS n,
    count(*) FILTER (WHERE Choice = 'E') AS e, count(*) FILTER (WHERE Choice = 'A') AS a
  FROM choices GROUP BY 1;
  CREATE TEMP TABLE who AS
  SELECT m.MemberId, m.LegislatureNumber, m.PersonId, m.MemberChamber, m.MemberDistrict, m.MemberParty,
    m.MemberIsMajority, m.MemberCode, p.PersonFullName, p.PersonFirstName, p.PersonLastName
  FROM members m JOIN people p USING (PersonId);
  CREATE TEMP TABLE vsum AS
  SELECT v.VoteId, v.BillId, v.LegislatureNumber, v.VoteChamber, v.VoteNumber, v.VoteDate, v.VoteTitle, v.VoteDescription,
    b.BillNumber, b.BillShortTitle, ${amendmentFields}, t.y, t.n, t.e, t.a
  FROM votes v LEFT JOIN bills b USING (BillId) LEFT JOIN tally t USING (VoteId);
`);

let written = 0;
async function writeDocs(kind: string, sql: string) {
  const dir = join(outDir, kind);
  await mkdir(dir, { recursive: true });
  const result = await rows(sql);
  const queue = [...result];
  const workers = Array.from({ length: 32 }, async () => {
    for (let r = queue.pop(); r; r = queue.pop()) {
      await writeFile(join(dir, encodeURIComponent(String(r[0])) + '.json'), String(r[1]));
      written++;
    }
  });
  await Promise.all(workers);
  console.error(`${kind}: ${result.length} documents`);
}

await rm(outDir, { recursive: true, force: true });
await mkdir(outDir, { recursive: true });

// --- legislatures ---------------------------------------------------------
await writeDocs(
  'legislatures',
  `
  WITH b AS (
    SELECT LegislatureNumber, list({id: BillId, number: BillNumber, title: BillShortTitle, statusDate: BillStatusDate,
      vetoed: BillVetoed, subjects: BillSubjects} ORDER BY BillNumber) AS bills
    FROM bills GROUP BY 1),
  v AS (
    SELECT LegislatureNumber, list({id: VoteId, billId: BillId, billNumber: BillNumber, billTitle: BillShortTitle,
      date: VoteDate, chamber: VoteChamber, number: VoteNumber, title: VoteTitle, description: VoteDescription,
      y: y, n: n, e: e, a: a} ORDER BY VoteDate DESC, VoteNumber DESC) AS votes
    FROM vsum GROUP BY 1),
  m AS (
    SELECT LegislatureNumber, list({id: MemberId, personId: PersonId, name: PersonFullName, lastName: PersonLastName,
      chamber: MemberChamber, district: MemberDistrict, party: MemberParty, majority: MemberIsMajority, code: MemberCode}
      ORDER BY MemberChamber, PersonLastName) AS members
    FROM who GROUP BY 1),
  legs AS (SELECT DISTINCT LegislatureNumber FROM members UNION SELECT DISTINCT LegislatureNumber FROM bills)
  SELECT l.LegislatureNumber, to_json({number: l.LegislatureNumber,
      bills: coalesce(b.bills, []), votes: coalesce(v.votes, []), members: coalesce(m.members, [])})::VARCHAR
  FROM legs l LEFT JOIN b USING (LegislatureNumber) LEFT JOIN v USING (LegislatureNumber) LEFT JOIN m USING (LegislatureNumber)
`,
);

// --- bills ----------------------------------------------------------------
await writeDocs(
  'bills',
  `
  WITH ver AS (
    SELECT BillId, list({id: BillVersionId, letter: BillVersionLetter, title: BillVersionTitle, name: BillVersionName,
      introDate: BillVersionIntroDate, passedHouse: BillVersionPassedHouse, passedSenate: BillVersionPassedSenate,
      pdfUrl: BillVersionPdfUrl, hasText: BillVersionFullText IS NOT NULL} ORDER BY BillVersionIntroDate, BillVersionLetter) AS versions
    FROM billVersions GROUP BY 1),
  vt AS (
    SELECT BillId, list({id: VoteId, date: VoteDate, chamber: VoteChamber, number: VoteNumber, title: VoteTitle,
      description: VoteDescription, amendment: amendment, subAmendment: subAmendment, y: y, n: n, e: e, a: a}
      ORDER BY VoteDate, VoteNumber) AS votes
    FROM vsum WHERE BillId IS NOT NULL GROUP BY 1)
  SELECT b.BillId, to_json({id: b.BillId, legislature: b.LegislatureNumber, number: b.BillNumber, name: b.BillName,
      title: b.BillShortTitle, statusDate: b.BillStatusDate, subjects: b.BillSubjects, vetoed: b.BillVetoed,
      versions: coalesce(ver.versions, []), votes: coalesce(vt.votes, [])})::VARCHAR
  FROM bills b LEFT JOIN ver USING (BillId) LEFT JOIN vt USING (BillId)
`,
);

// --- bill versions --------------------------------------------------------
await writeDocs(
  'bill-versions',
  `
  SELECT bv.BillVersionId, to_json({id: bv.BillVersionId, billId: bv.BillId, legislature: b.LegislatureNumber,
      billNumber: b.BillNumber, billTitle: b.BillShortTitle, letter: bv.BillVersionLetter, title: bv.BillVersionTitle,
      name: bv.BillVersionName, introDate: bv.BillVersionIntroDate, passedHouse: bv.BillVersionPassedHouse,
      passedSenate: bv.BillVersionPassedSenate, workOrder: bv.BillVersionWorkOrder, pdfUrl: bv.BillVersionPdfUrl,
      fullText: bv.BillVersionFullText})::VARCHAR
  FROM billVersions bv JOIN bills b USING (BillId)
`,
);

// --- votes ----------------------------------------------------------------
await writeDocs(
  'votes',
  `
  WITH c AS (
    SELECT c.VoteId, list({memberId: w.MemberId, personId: w.PersonId, name: w.PersonFullName, lastName: w.PersonLastName,
      party: w.MemberParty, district: w.MemberDistrict, majority: w.MemberIsMajority, choice: c.Choice}
      ORDER BY w.PersonLastName) AS choices
    FROM choices c JOIN who w USING (MemberId) GROUP BY 1)
  SELECT v.VoteId, to_json({id: v.VoteId, legislature: v.LegislatureNumber, chamber: v.VoteChamber, number: v.VoteNumber,
      date: v.VoteDate, title: v.VoteTitle, description: v.VoteDescription, billId: v.BillId, billNumber: v.BillNumber,
      billTitle: v.BillShortTitle, amendment: v.amendment, subAmendment: v.subAmendment, amendmentId: v.amendmentId,
      y: v.y, n: v.n, e: v.e, a: v.a, choices: coalesce(c.choices, [])})::VARCHAR
  FROM vsum v LEFT JOIN c USING (VoteId)
`,
);

// --- members (one legislator's term) ----------------------------------------
await writeDocs(
  'members',
  `
  WITH c AS (
    SELECT c.MemberId,
      list({id: v.VoteId, date: v.VoteDate, number: v.VoteNumber, title: v.VoteTitle, billId: v.BillId,
        billNumber: v.BillNumber, billTitle: v.BillShortTitle, choice: c.Choice, y: v.y, n: v.n}
        ORDER BY v.VoteDate DESC, v.VoteNumber DESC) AS votes,
      count(*) FILTER (WHERE c.Choice = 'Y') AS y, count(*) FILTER (WHERE c.Choice = 'N') AS n,
      count(*) FILTER (WHERE c.Choice = 'E') AS e, count(*) FILTER (WHERE c.Choice = 'A') AS a
    FROM choices c JOIN vsum v USING (VoteId) GROUP BY 1),
  terms AS (
    SELECT PersonId, list({id: MemberId, legislature: LegislatureNumber, chamber: MemberChamber, district: MemberDistrict,
      party: MemberParty} ORDER BY LegislatureNumber) AS terms
    FROM who GROUP BY 1)
  SELECT w.MemberId, to_json({id: w.MemberId, legislature: w.LegislatureNumber, personId: w.PersonId, name: w.PersonFullName,
      firstName: w.PersonFirstName, lastName: w.PersonLastName, chamber: w.MemberChamber, district: w.MemberDistrict,
      party: w.MemberParty, majority: w.MemberIsMajority, code: w.MemberCode, otherTerms: t.terms,
      y: coalesce(c.y, 0), n: coalesce(c.n, 0), e: coalesce(c.e, 0), a: coalesce(c.a, 0), votes: coalesce(c.votes, [])})::VARCHAR
  FROM who w LEFT JOIN c USING (MemberId) LEFT JOIN terms t USING (PersonId)
`,
);

// --- people ---------------------------------------------------------------
await writeDocs(
  'people',
  `
  WITH per AS (
    SELECT MemberId, count(*) FILTER (WHERE Choice = 'Y') AS y, count(*) FILTER (WHERE Choice = 'N') AS n,
      count(*) FILTER (WHERE Choice IN ('E', 'A')) AS missed
    FROM choices GROUP BY 1),
  terms AS (
    SELECT w.PersonId, list({id: w.MemberId, legislature: w.LegislatureNumber, chamber: w.MemberChamber,
      district: w.MemberDistrict, party: w.MemberParty, majority: w.MemberIsMajority, code: w.MemberCode,
      y: coalesce(per.y, 0), n: coalesce(per.n, 0), missed: coalesce(per.missed, 0)} ORDER BY w.LegislatureNumber, w.MemberChamber) AS terms
    FROM who w LEFT JOIN per USING (MemberId) GROUP BY 1)
  SELECT p.PersonId, to_json({id: p.PersonId, name: p.PersonFullName, firstName: p.PersonFirstName, lastName: p.PersonLastName,
      middleName: p.PersonMiddleName, nickName: p.PersonNickName, suffix: p.PersonSuffix, terms: coalesce(t.terms, [])})::VARCHAR
  FROM people p LEFT JOIN terms t USING (PersonId)
`,
);

// --- indexes --------------------------------------------------------------
const people = await rows(`
  SELECT to_json(list({id: p.PersonId, name: p.PersonFullName, lastName: p.PersonLastName,
    first: (SELECT min(LegislatureNumber) FROM members m WHERE m.PersonId = p.PersonId),
    last: (SELECT max(LegislatureNumber) FROM members m WHERE m.PersonId = p.PersonId)} ORDER BY p.PersonLastName, p.PersonFirstName))::VARCHAR
  FROM people p`);
await writeFile(join(outDir, 'people', 'index.json'), String(people[0]![0]));

const meta = await rows(`
  SELECT to_json({updated: '${updated}',
    bills: (SELECT count(*) FROM bills), votes: (SELECT count(*) FROM votes), choices: (SELECT count(*) FROM choices),
    people: (SELECT count(*) FROM people), versions: (SELECT count(*) FROM billVersions),
    legislatures: (SELECT list({number: n, bills: nb, votes: nv, members: nm} ORDER BY n DESC) FROM (
      SELECT l.n, (SELECT count(*) FROM bills WHERE LegislatureNumber = l.n) nb,
        (SELECT count(*) FROM votes WHERE LegislatureNumber = l.n) nv,
        (SELECT count(*) FROM members WHERE LegislatureNumber = l.n) nm
      FROM (SELECT DISTINCT LegislatureNumber n FROM members UNION SELECT DISTINCT LegislatureNumber FROM bills) l))})::VARCHAR`);
await writeFile(join(outDir, 'meta.json'), String(meta[0]![0]));

console.error(`Wrote ${written} documents to ${outDir}`);
