import {
  chamberName,
  date,
  fmt,
  legFull,
  legName,
  legYears,
  memberTitle,
  partyName,
  partyPlural,
  voteMotion,
  voteVersion,
} from './format';
import type { Bill, BillVersion, Choice, Member, Person, Vote } from './types';

export function billSummary(b: Bill): string {
  const parts = [
    `${b.number} is a ${b.number.includes('R') && !b.number.startsWith('HB') && !b.number.startsWith('SB') ? 'resolution' : 'bill'} in the ${legFull(b.legislature)} (${legYears(b.legislature)})${b.title ? `, titled “${b.title}”` : ''}.`,
  ];
  if (b.versions.length) parts.push(`It has ${b.versions.length} version${b.versions.length === 1 ? '' : 's'}.`);
  parts.push(
    b.votes.length
      ? `Legislators took ${b.votes.length} recorded floor vote${b.votes.length === 1 ? '' : 's'} on it, the last on ${date(b.votes[b.votes.length - 1]!.date)}.`
      : 'No floor votes on it are recorded.',
  );
  if (b.vetoed) parts.push('It was vetoed by the governor.');
  if (b.statusDate) parts.push(`Its status was last updated on ${date(b.statusDate)}.`);
  return parts.join(' ');
}

export function versionSummary(v: BillVersion): string {
  const s = [
    `This is version ${v.letter} of ${v.billNumber}${v.billTitle ? ` (“${v.billTitle}”)` : ''} in the ${legFull(v.legislature)}, introduced ${date(v.introDate)}.`,
  ];
  if (v.passedHouse) s.push(`It passed the House on ${date(v.passedHouse)}.`);
  if (v.passedSenate) s.push(`It passed the Senate on ${date(v.passedSenate)}.`);
  return s.join(' ');
}

export function byParty(v: Vote) {
  const m = new Map<string, Record<Choice, number>>();
  for (const c of v.choices) {
    const k = c.party ?? '?';
    const r = m.get(k) ?? { Y: 0, N: 0, E: 0, A: 0 };
    r[c.choice]++;
    m.set(k, r);
  }
  return [...m.entries()].sort((a, b) => b[1].Y + b[1].N - (a[1].Y + a[1].N));
}

export function voteSummary(v: Vote): string {
  const what = v.billNumber
    ? `${voteVersion(v.title) ?? v.billNumber} (${v.billNumber}${v.billTitle ? `, “${v.billTitle}”` : ''})`
    : 'a non-bill matter';
  const s = [
    `On ${date(v.date)}, the Alaska ${chamberName(v.chamber)} voted ${v.y} to ${v.n} on “${voteMotion(v.title)}” for ${what}.`,
  ];
  const parties = byParty(v)
    .filter(([p]) => p !== '?')
    .map(([p, r]) => `${partyPlural(p)} voted ${r.Y}–${r.N}`);
  if (parties.length) s.push(parties.join('; ') + '.');
  if (v.e + v.a) s.push(`${v.e + v.a} member${v.e + v.a === 1 ? ' was' : 's were'} absent or excused.`);
  return s.join(' ');
}

export function memberSummary(m: Omit<Member, 'votes'>): string {
  const total = m.y + m.n + m.e + m.a;
  const s = [
    `${memberTitle(m.chamber)} ${m.name} (${partyName(m.party)}) represented ${chamberName(m.chamber)} District ${m.district ?? 'unknown'} in the ${legFull(m.legislature)} (${legYears(m.legislature)})${m.majority === null ? '' : `, as a member of the ${m.majority ? 'majority' : 'minority'} caucus`}.`,
  ];
  if (total)
    s.push(
      `${m.lastName} voted yea ${fmt(m.y)} times and nay ${fmt(m.n)} times, and missed ${fmt(m.e + m.a)} of ${fmt(total)} roll-call votes (${((100 * (m.e + m.a)) / total).toFixed(1)}%).`,
    );
  return s.join(' ');
}

export function personSummary(p: Person): string {
  const t = p.terms;
  if (!t.length) return `${p.name} served in the Alaska State Legislature.`;
  const first = t[0]!,
    last = t[t.length - 1]!;
  const chambers = [...new Set(t.map((x) => chamberName(x.chamber)))].join(' and ');
  const parties = [...new Set(t.map((x) => x.party).filter(Boolean))].map(partyName).join(', then ');
  const y = t.reduce((a, x) => a + (x.y ?? 0), 0),
    n = t.reduce((a, x) => a + (x.n ?? 0), 0),
    missed = t.reduce((a, x) => a + (x.missed ?? 0), 0);
  const s = [
    `${p.name} served ${t.length} term${t.length === 1 ? '' : 's'} in the Alaska ${chambers}, from the ${legName(first.legislature)} (${legYears(first.legislature).slice(0, 4)}) to the ${legName(last.legislature)} (${legYears(last.legislature).slice(5)})${parties ? `, as a ${parties}` : ''}.`,
  ];
  if (y + n) s.push(`Across those terms they cast ${fmt(y)} yea and ${fmt(n)} nay votes and missed ${fmt(missed)}.`);
  return s.join(' ');
}
