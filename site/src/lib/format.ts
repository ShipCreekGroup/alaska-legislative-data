import type { Chamber, Choice, Tally } from './types';

export const SITE_NAME = 'Alaska Legislative Data';
/** Public address of the site; used for canonical links, sitemaps and structured data. */
export const SITE_URL = (import.meta.env.VITE_SITE_URL as string | undefined) ?? 'https://akleg.shipcreekgroup.com';

export const fmt = (n: number) => n.toLocaleString('en-US');

export function legYears(n: number): string {
  const start = 1959 + (n - 1) * 2;
  return `${start}–${start + 1}`;
}

export function ordinal(n: number): string {
  const s = n % 100;
  if (s > 10 && s < 14) return `${n}th`;
  return n + ({ 1: 'st', 2: 'nd', 3: 'rd' }[n % 10] ?? 'th');
}

export const legName = (n: number) => `${ordinal(n)} Legislature`;
export const chamberName = (c: Chamber | string) => (c === 'H' ? 'House' : 'Senate');
export const memberTitle = (c: Chamber | string) => (c === 'H' ? 'Representative' : 'Senator');
export const memberTitleShort = (c: Chamber | string) => (c === 'H' ? 'Rep.' : 'Sen.');

const PARTY: Record<string, string> = {
  D: 'Democrat',
  R: 'Republican',
  I: 'Independent',
  L: 'Libertarian',
  N: 'Nonpartisan',
};
export const partyName = (p: string | null | undefined) => (p ? (PARTY[p] ?? p) : 'Party unknown');
export const partyPlural = (p: string | null | undefined) =>
  p === 'N' ? 'Nonpartisan members' : p === 'I' ? 'Independents' : p ? partyName(p) + 's' : 'Party unknown';

export const CHOICE: Record<Choice, string> = { Y: 'Yea', N: 'Nay', E: 'Excused', A: 'Absent' };

export function date(d: string | null | undefined, style: 'long' | 'short' = 'long'): string {
  if (!d) return 'Date unknown';
  const dt = new Date(d + 'T12:00:00Z');
  return dt.toLocaleDateString('en-US', {
    timeZone: 'UTC',
    month: style === 'long' ? 'long' : 'short',
    day: 'numeric',
    year: 'numeric',
  });
}

/** "Third Reading Final Passage" from a multi-line roll-call title. */
export function voteMotion(title: string | null | undefined): string {
  if (!title) return 'Roll-call vote';
  const lines = title
    .split('\n')
    .map((l) => l.trim())
    .filter(Boolean);
  return lines.length > 1 ? lines.slice(1).join(' · ') : lines[0]!;
}
export function voteVersion(title: string | null | undefined): string | null {
  if (!title) return null;
  const lines = title
    .split('\n')
    .map((l) => l.trim())
    .filter(Boolean);
  return lines.length > 1 ? lines[0]! : null;
}

export const tallyText = (t: Tally) => `${t.y}–${t.n}`;

/** Collapse whitespace and cut to a length suitable for a meta description. */
export function clip(s: string, n = 155): string {
  const flat = s.replace(/\s+/g, ' ').trim();
  return flat.length <= n ? flat : flat.slice(0, n - 1).replace(/\s+\S*$/, '') + '…';
}

export const akleg = {
  bill: (leg: number, number: string) =>
    `https://www.akleg.gov/basis/Bill/Detail/${leg}?Root=${encodeURIComponent(number.replace(/\s+/g, ''))}`,
  legislator: (leg: number, code: string) =>
    `https://www.akleg.gov/basis/Member/Detail/${leg}?code=${encodeURIComponent(code)}`,
};
export const legFull = (n: number) => `${ordinal(n)} Alaska State Legislature`;
