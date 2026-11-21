// Turns raw BASIS API objects into rows of the published tables.
import type { Bill, BillVersion, Vote } from '../types.gen.ts';
import { amendmentId, billId, billVersionId, rollCall, voteId } from './ids.ts';
import type { BillRow, BillVersionRow, ChoiceRow, MemberRow, VoteRow } from './rows.ts';

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

export interface AmendmentNumber {
  number: number;
  /** The sub-amendment number, or null for an amendment that isn't to another one. */
  sub: number | null;
}

/**
 * The amendment a vote is about, from the last line of its title:
 * 'Amendment No. 2' -> { number: 2, sub: null },
 * 'Amendment No. 10 to Amendment No. 2' -> { number: 2, sub: 10 },
 * 'Amendment to Amendment No. 3' -> { number: 3, sub: 1 }.
 * Text around it is allowed, eg 'Amendment No. 2 as amended', 'Amendment No. 4/Table',
 * 'Rescind Previous Action/Amendment No. 7'. null when the vote isn't on a numbered amendment.
 */
export function parseAmendmentNumber(title: string | null): AmendmentNumber | null {
  if (title === null) return null;
  const last = title.split('\n').at(-1)!;
  const toAmendment = /Amendment No\.\s*(\d+)\s+to\s+Amendment No\.\s*(\d+)/i.exec(last);
  if (toAmendment) return { number: Number(toAmendment[2]), sub: Number(toAmendment[1]) };
  const unnumbered = /Amendment to Amendment No\.\s*(\d+)/i.exec(last);
  if (unnumbered) return { number: Number(unnumbered[1]), sub: 1 };
  const plain = /Amendment No\.\s*(\d+)/i.exec(last);
  return plain ? { number: Number(plain[1]), sub: null } : null;
}

/** A member's current code plus any earlier ones (eg after a name change). */
export function memberCodes(m: Pick<MemberRow, 'MemberCode' | 'MemberFormerCodes'>): string[] {
  const out: string[] = [];
  if (m.MemberCode) out.push(m.MemberCode);
  if (m.MemberFormerCodes) out.push(...m.MemberFormerCodes.split(';').filter(Boolean));
  return out;
}

export function cleanBill(leg: number, b: Bill): BillRow {
  const BillNumber = fixBillNumber(b.BillNumber);
  if (!BillNumber) throw new Error(`Bill without a number in legislature ${leg}`);
  return {
    BillId: billId(leg, BillNumber),
    LegislatureNumber: leg,
    BillNumber,
    BillName: fixString(b.BillName),
    BillShortTitle: fixString(b.ShortTitle),
    BillStatusDate: parseIsoDate(b.StatusDate),
    BillSubjects: b.Subjects ?? [],
    BillVetoed: typeof b.Vetoed === 'boolean' ? b.Vetoed : null,
  };
}

export function cleanBillVersion(
  leg: number,
  billNumber: string,
  v: BillVersion,
  fullText: string | null,
): BillVersionRow {
  return {
    BillVersionId: billVersionId(leg, billNumber, v.VersionLetter),
    BillId: billId(leg, billNumber),
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
  for (const m of members)
    for (const code of memberCodes(m)) memberByCode.set(`${m.LegislatureNumber}:${code}`, m.MemberId);

  const votes = new Map<string, VoteRow>();
  const choices = new Map<string, ChoiceRow>();
  const unmatched: SplitVotesResult['unmatched'] = [];

  for (const { leg, vote } of raw) {
    const voteNum = fixString(vote.VoteNum);
    if (!voteNum) throw new Error(`Vote without VoteNum: ${JSON.stringify(vote)}`);
    const id = voteId(leg, voteNum);
    if (!votes.has(id)) votes.set(id, cleanVote(leg, voteNum, vote, billIds));
    const code = fixString(vote.Member);
    const mId = code ? memberByCode.get(`${leg}:${code}`) : undefined;
    if (!mId) {
      unmatched.push({ leg, code: code ?? '', voteNum });
      continue;
    }
    choices.set(`${id} ${mId}`, { VoteId: id, MemberId: mId, Choice: fixString(vote.Vote) });
  }
  return { votes: [...votes.values()], choices: [...choices.values()], unmatched };
}

function cleanVote(leg: number, voteNum: string, vote: Vote, billIds: Set<string>): VoteRow {
  const rc = rollCall(voteNum);
  const chamber = rc[0] as 'H' | 'S';
  const billNumber = fixBillNumber(vote.Bill);
  const bill = billNumber ? billId(leg, billNumber) : null;
  const title = fixString(vote.Title);
  const amendment = parseAmendmentNumber(title);
  return {
    VoteId: voteId(leg, voteNum),
    BillId: bill && billIds.has(bill) ? bill : null,
    VoteNumber: Number(rc.slice(1)),
    VoteDate: parseIsoDate(vote.VoteDate),
    VoteTitle: title,
    LegislatureNumber: leg,
    VoteChamber: chamber,
    VoteDescription: '',
    VoteAmendmentNumber: amendment?.number ?? null,
    VoteSubAmendmentNumber: amendment?.sub ?? null,
    VoteAmendmentId: amendment && billNumber ? voteAmendmentId(leg, billNumber, chamber, amendment) : null,
  };
}

/** A vote's AmendmentId; null when the numbers don't fit the ID format (amendments 1-999, sub-amendments 1-99). */
export function voteAmendmentId(
  leg: number,
  billNumber: string,
  chamber: 'H' | 'S',
  a: AmendmentNumber,
): string | null {
  const fits = a.number >= 1 && a.number <= 999 && (a.sub === null || (a.sub >= 1 && a.sub <= 99));
  return fits ? amendmentId(leg, billNumber, chamber, a.number, a.sub) : null;
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
