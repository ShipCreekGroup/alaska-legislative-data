// Converts IDs from the format published before the new ID system
// ('Robyn Burke:34', '34:HB 16', '34:H:396', ...). Used to read a previous
// release's CSVs as the baseline for an incremental scrape.
import { billId, billVersionId, memberId, personId, voteId } from '../tables/ids.ts';

/** People whose old ID can't be converted mechanically. */
const PERSON_FIXES: Record<string, string> = {
  // A duplicate of 'Tammie Wilson:26' with the legislature missing.
  'Tammie Wilson:': 'Tammie Wilson:26',
};

export function isLegacyId(id: string): boolean {
  return !/^[a-z]{3}_/.test(id);
}

/** 'Robyn Burke:34' -> 'per_robyn-burke-34th' */
export function legacyPersonId(old: string): string {
  const fixed = PERSON_FIXES[old] ?? old;
  const i = fixed.lastIndexOf(':');
  const leg = Number(fixed.slice(i + 1));
  if (i < 1 || !Number.isInteger(leg) || leg < 1) throw new Error(`Not an old PersonId: ${old}`);
  return personId(fixed.slice(0, i), leg);
}

/** '34:H:40:Robyn Burke:34' -> 'mem_34th_H_robyn-burke-34th' */
export function legacyMemberId(old: string): string {
  const m = /^(\d+):([HS]):[^:]*:(.+)$/.exec(old);
  if (!m) throw new Error(`Not an old MemberId: ${old}`);
  return memberId(Number(m[1]), m[2]!, legacyPersonId(m[3]!));
}

/** '34:HB 16' -> 'bil_34th_HB0016' */
export function legacyBillId(old: string): string {
  const m = /^(\d+):(.+)$/.exec(old);
  if (!m) throw new Error(`Not an old BillId: ${old}`);
  return billId(Number(m[1]), m[2]!);
}

/** '34:HB 16:A' -> 'ver_34th_HB0016_A' */
export function legacyBillVersionId(old: string): string {
  const m = /^(\d+):(.+):([A-Z]+)$/.exec(old);
  if (!m) throw new Error(`Not an old BillVersionId: ${old}`);
  return billVersionId(Number(m[1]), m[2]!, m[3]!);
}

/** '34:H:396' -> 'vot_34th_H0396' */
export function legacyVoteId(old: string): string {
  const m = /^(\d+):([HS]):(\d+)$/.exec(old);
  if (!m) throw new Error(`Not an old VoteId: ${old}`);
  return voteId(Number(m[1]), `${m[2]}${m[3]}`);
}
