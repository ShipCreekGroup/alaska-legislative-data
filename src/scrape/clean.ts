// Turns raw BASIS API objects into the rows of the published tables.
// Ported from the original Python scraper (python/alaska_legislative_data/_parse.py etc).
import type { Bill, BillVersion, Vote } from '../types.gen.ts';
import type { Row } from './csv.ts';

// ---------------------------------------------------------------------------
// table definitions (column order of the published CSVs)
// ---------------------------------------------------------------------------

export const COLUMNS = {
  people: ['PersonId', 'PersonFullName', 'PersonFirstName', 'PersonLastName', 'PersonMiddleName', 'PersonNickName', 'PersonSuffix'],
  members: ['MemberId', 'LegislatureNumber', 'PersonId', 'MemberChamber', 'MemberDistrict', 'MemberIsMajority', 'MemberParty', 'MemberCode', 'MemberFormerCodes'],
  bills: ['BillId', 'LegislatureNumber', 'BillNumber', 'BillName', 'BillShortTitle', 'BillStatusDate', 'BillSubjects', 'BillVetoed'],
  votes: ['VoteId', 'BillId', 'VoteNumber', 'VoteDate', 'VoteTitle', 'LegislatureNumber', 'VoteChamber', 'VoteDescription', 'VoteBillAmendmentNumber'],
  choices: ['ChoiceId', 'VoteId', 'MemberId', 'Choice'],
  bill_versions: [
    'BillVersionId', 'BillId', 'BillVersionLetter', 'BillVersionTitle', 'BillVersionName', 'BillVersionIntroDate',
    'BillVersionPassedHouse', 'BillVersionPassedSenate', 'BillVersionWorkOrder', 'BillVersionPdfUrl', 'BillVersionFullText',
  ],
} as const;

export interface PersonRow {
  PersonId: string;
  PersonFullName: string;
  PersonFirstName: string;
  PersonLastName: string;
  PersonMiddleName: string | null;
  PersonNickName: string | null;
  PersonSuffix: string | null;
}
export interface MemberRow {
  MemberId: string;
  LegislatureNumber: number;
  PersonId: string;
  MemberChamber: string;
  MemberDistrict: string | null;
  MemberIsMajority: boolean | null;
  MemberParty: string | null;
  MemberCode: string | null;
  MemberFormerCodes: string | null;
}
export interface BillRow {
  BillId: string;
  LegislatureNumber: number;
  BillNumber: string;
  BillName: string | null;
  BillShortTitle: string | null;
  BillStatusDate: string | null;
  BillSubjects: string[];
  BillVetoed: boolean | null;
}
export interface VoteRow {
  VoteId: string;
  BillId: string | null;
  VoteNumber: number;
  VoteDate: string | null;
  VoteTitle: string | null;
  LegislatureNumber: number;
  VoteChamber: string;
  VoteDescription: string;
  VoteBillAmendmentNumber: string | null;
}
export interface ChoiceRow {
  ChoiceId: string;
  VoteId: string;
  MemberId: string;
  Choice: string | null;
}
export interface BillVersionRow {
  BillVersionId: string;
  BillId: string;
  BillVersionLetter: string;
  BillVersionTitle: string;
  BillVersionName: string;
  BillVersionIntroDate: string | null;
  BillVersionPassedHouse: string | null;
  BillVersionPassedSenate: string | null;
  BillVersionWorkOrder: string;
  BillVersionPdfUrl: string | null;
  BillVersionFullText: string | null;
}

// ---------------------------------------------------------------------------
// string helpers
// ---------------------------------------------------------------------------

/** Trim, normalize \r\n to \n, and turn '' into null. */
export function fixString(s: string | null | undefined): string | null {
  if (s === null || s === undefined) return null;
  const t = s.replace(/^[ \t\n\r\v\f]+|[ \t\n\r\v\f]+$/g, '').replace(/\r\n/g, '\n');
  return t === '' ? null : t;
}

/** 'HB   16 ' -> 'HB 16' */
export function fixBillNumber(s: string | null | undefined): string | null {
  const t = fixString(s);
  return t === null ? null : t.replace(/\s+/g, ' ');
}

/** A yyyy-mm-dd string if `s` is a valid date, else null (eg the 'Jul -10- 1' values in old legislatures). */
export function parseIsoDate(s: string | null | undefined): string | null {
  if (!s) return null;
  const m = /^\s*(\d{4})-(\d{2})-(\d{2})\s*$/.exec(s);
  if (!m) return null;
  const [y, mo, d] = [Number(m[1]), Number(m[2]), Number(m[3])];
  const dt = new Date(Date.UTC(y, mo - 1, d));
  if (dt.getUTCFullYear() !== y || dt.getUTCMonth() !== mo - 1 || dt.getUTCDate() !== d) return null;
  return `${m[1]}-${m[2]}-${m[3]}`;
}

/**
 * Amendment number of a vote title, as a sortable decimal string:
 * 'Amendment No. 2' -> '2.0', 'Amendment No. 1 to Amendment No. 3' -> '3.1',
 * 'Amendment to Amendment No. 3' -> '3.1'. null when not an amendment vote.
 */
export function parseAmendmentNumber(title: string | null): string | null {
  if (title === null) return null;
  const root = /Amendment No. (\d+)$/.exec(title)?.[1];
  if (!root) return null;
  let sub = /Amendment No. (\d+) to Amendment No. \d+$/.exec(title)?.[1];
  if (!sub) sub = /Amendment to Amendment No. \d+/.test(title) ? '1' : '0';
  return `${Number(root)}.${Number(sub)}`;
}

// ---------------------------------------------------------------------------
// curated tables
// ---------------------------------------------------------------------------

function parseBool(s: string | null): boolean | null {
  if (s === null || s === '') return null;
  if (/^true$/i.test(s)) return true;
  if (/^false$/i.test(s)) return false;
  throw new Error(`Not a boolean: ${s}`);
}

export function memberId(leg: number | string, chamber: string, district: string | null, personId: string): string {
  return `${leg}:${chamber}:${district ?? ''}:${personId}`;
}

export function cleanPerson(r: Row): PersonRow {
  const req = (k: string) => {
    const v = r[k];
    if (!v) throw new Error(`people.csv: missing ${k} in ${JSON.stringify(r)}`);
    return v;
  };
  return {
    PersonId: req('PersonId'),
    PersonFullName: req('PersonFullName'),
    PersonFirstName: req('PersonFirstName'),
    PersonLastName: req('PersonLastName'),
    PersonMiddleName: r.PersonMiddleName || null,
    PersonNickName: r.PersonNickName || null,
    PersonSuffix: r.PersonSuffix || null,
  };
}

export function cleanMember(r: Row): MemberRow {
  const leg = Number(r.LegislatureNumber);
  if (!Number.isInteger(leg) || !r.PersonId || !r.MemberChamber) {
    throw new Error(`members.csv: bad row ${JSON.stringify(r)}`);
  }
  const district = r.MemberDistrict || null;
  return {
    MemberId: memberId(leg, r.MemberChamber, district, r.PersonId),
    LegislatureNumber: leg,
    PersonId: r.PersonId,
    MemberChamber: r.MemberChamber,
    MemberDistrict: district,
    MemberIsMajority: parseBool(r.MemberIsMajority ?? null),
    MemberParty: r.MemberParty || null,
    MemberCode: r.MemberCode || null,
    MemberFormerCodes: r.MemberFormerCodes || null,
  };
}

/** Checks the curated tables against each other. Throws on problems. */
export function checkCurated(people: PersonRow[], members: MemberRow[]): void {
  const personIds = new Set<string>();
  for (const p of people) {
    if (personIds.has(p.PersonId)) throw new Error(`Duplicate PersonId ${p.PersonId}`);
    if (!p.PersonId.includes(':')) throw new Error(`PersonId ${p.PersonId} must look like 'Name:leg'`);
    personIds.add(p.PersonId);
  }
  const ids = new Set<string>();
  const codes = new Set<string>();
  for (const m of members) {
    if (ids.has(m.MemberId)) throw new Error(`Duplicate MemberId ${m.MemberId}`);
    ids.add(m.MemberId);
    if (!personIds.has(m.PersonId)) throw new Error(`Member ${m.MemberId} has unknown PersonId`);
    for (const code of memberCodes(m)) {
      const key = `${m.LegislatureNumber}:${code}`;
      if (codes.has(key)) throw new Error(`Member code ${code} used twice in legislature ${m.LegislatureNumber}`);
      codes.add(key);
    }
  }
}

/** A member's current code plus any earlier ones (eg after a name change). */
export function memberCodes(m: MemberRow): string[] {
  const out: string[] = [];
  if (m.MemberCode) out.push(m.MemberCode);
  if (m.MemberFormerCodes) out.push(...m.MemberFormerCodes.split(';').filter(Boolean));
  return out;
}

// ---------------------------------------------------------------------------
// API -> rows
// ---------------------------------------------------------------------------

export function cleanBill(leg: number, b: Bill): BillRow {
  const BillNumber = fixBillNumber(b.BillNumber);
  if (!BillNumber) throw new Error(`Bill without a number in legislature ${leg}`);
  return {
    BillId: `${leg}:${BillNumber}`,
    LegislatureNumber: leg,
    BillNumber,
    BillName: fixString(b.BillName),
    BillShortTitle: fixString(b.ShortTitle),
    BillStatusDate: parseIsoDate(b.StatusDate),
    BillSubjects: b.Subjects ?? [],
    BillVetoed: typeof b.Vetoed === 'boolean' ? b.Vetoed : null,
  };
}

export function cleanBillVersion(leg: number, billNumber: string, v: BillVersion, fullText: string | null): BillVersionRow {
  const billId = `${leg}:${fixBillNumber(billNumber)}`;
  return {
    BillVersionId: `${billId}:${v.VersionLetter}`,
    BillId: billId,
    BillVersionLetter: v.VersionLetter,
    BillVersionTitle: v.Title,
    BillVersionName: v.Name,
    BillVersionIntroDate: parseIsoDate(v.IntroDate),
    BillVersionPassedHouse: parseIsoDate(v.PassedHouse),
    BillVersionPassedSenate: parseIsoDate(v.PassedSenate),
    BillVersionWorkOrder: v.WorkOrder,
    BillVersionPdfUrl: v.Url,
    BillVersionFullText: fullText,
  };
}

export interface SplitVotesResult {
  votes: VoteRow[];
  choices: ChoiceRow[];
  /** Raw choices whose member code isn't in the curated members table. */
  unmatched: Array<{ leg: number; code: string; voteNum: string }>;
}

/**
 * Split raw per-member vote records into one row per roll call (votes) and one
 * row per member per roll call (choices).
 * @param raw votes from `members?query=members;code=XXX,votes`, tagged with their legislature
 * @param billIds every BillId known, so votes can be linked to bills
 * @param members the curated members, to resolve member codes to MemberIds
 */
export function splitVotes(
  raw: Array<{ leg: number; vote: Vote }>,
  billIds: Set<string>,
  members: MemberRow[],
): SplitVotesResult {
  const memberByCode = new Map<string, string>();
  for (const m of members) for (const code of memberCodes(m)) memberByCode.set(`${m.LegislatureNumber}:${code}`, m.MemberId);

  const votes = new Map<string, VoteRow>();
  const choices = new Map<string, ChoiceRow>();
  const unmatched: SplitVotesResult['unmatched'] = [];

  for (const { leg, vote } of raw) {
    const voteNum = fixString(vote.VoteNum);
    if (!voteNum) throw new Error(`Vote without VoteNum: ${JSON.stringify(vote)}`);
    const chamber = voteNum[0]!;
    const number = Number(voteNum.slice(1));
    const voteId = `${leg}:${chamber}:${number}`;
    const billNumber = fixBillNumber(vote.Bill);
    const billId = billNumber && billIds.has(`${leg}:${billNumber}`) ? `${leg}:${billNumber}` : null;
    const title = fixString(vote.Title);
    if (!votes.has(voteId)) {
      votes.set(voteId, {
        VoteId: voteId,
        BillId: billId,
        VoteNumber: number,
        VoteDate: vote.VoteDate === '-199- 0--0' ? null : parseIsoDate(vote.VoteDate),
        VoteTitle: title,
        LegislatureNumber: leg,
        VoteChamber: chamber,
        VoteDescription: '',
        VoteBillAmendmentNumber: parseAmendmentNumber(title),
      });
    }
    const code = fixString(vote.Member);
    const mId = code ? memberByCode.get(`${leg}:${code}`) : undefined;
    if (!mId) {
      unmatched.push({ leg, code: code ?? '', voteNum });
      continue;
    }
    const choiceId = `${voteId}:${mId.replace(/^\d+:/, '')}`;
    choices.set(choiceId, { ChoiceId: choiceId, VoteId: voteId, MemberId: mId, Choice: fixString(vote.Vote) });
  }
  return { votes: [...votes.values()], choices: [...choices.values()], unmatched };
}

// ---------------------------------------------------------------------------
// reading published rows back (baseline CSVs)
// ---------------------------------------------------------------------------

export function billFromCsv(r: Row): BillRow {
  const subjects = r.BillSubjects ?? '[]';
  const inner = subjects.replace(/^\[|\]$/g, '');
  return {
    BillId: r.BillId!,
    LegislatureNumber: Number(r.LegislatureNumber),
    BillNumber: r.BillNumber!,
    BillName: r.BillName ?? null,
    BillShortTitle: r.BillShortTitle ?? null,
    BillStatusDate: r.BillStatusDate ?? null,
    BillSubjects: inner === '' ? [] : inner.split(', '),
    BillVetoed: r.BillVetoed == null ? null : r.BillVetoed === 'true',
  };
}

export function voteFromCsv(r: Row): VoteRow {
  return {
    VoteId: r.VoteId!,
    BillId: r.BillId ?? null,
    VoteNumber: Number(r.VoteNumber),
    VoteDate: r.VoteDate ?? null,
    VoteTitle: r.VoteTitle ?? null,
    LegislatureNumber: Number(r.LegislatureNumber),
    VoteChamber: r.VoteChamber!,
    VoteDescription: r.VoteDescription ?? '',
    VoteBillAmendmentNumber: r.VoteBillAmendmentNumber ?? null,
  };
}

export function choiceFromCsv(r: Row): ChoiceRow {
  return { ChoiceId: r.ChoiceId!, VoteId: r.VoteId!, MemberId: r.MemberId!, Choice: r.Choice ?? null };
}

/** Legislature number from an id like '34:H:12' or '34:HB 16'. */
export function legOf(id: string): number {
  return Number(id.slice(0, id.indexOf(':')));
}

export function compareBy<T>(...keys: Array<(x: T) => string | number>) {
  return (a: T, b: T): number => {
    for (const k of keys) {
      const x = k(a);
      const y = k(b);
      if (x < y) return -1;
      if (x > y) return 1;
    }
    return 0;
  };
}
