// The hand-kept lists. People and legislators can't be tracked across
// legislatures from the API alone (member codes change, names repeat), so
// data/curated/people.csv and members.csv are edited by hand, as is
// vote_descriptions.csv, the one hand-typed column of the votes table. They
// are bundled into this package in a compact form (src/tables/curated.gen.ts,
// written by `pnpm gen`).
import { CURATED } from './curated.gen.ts';
import { isPersonId, memberId } from './ids.ts';
import { memberCodes } from './clean.ts';
import type { MemberRow, PersonRow } from './rows.ts';

export interface VoteDescription {
  VoteId: string;
  VoteDescription: string;
}

export interface Curated {
  people: PersonRow[];
  members: MemberRow[];
  voteDescriptions: VoteDescription[];
}

/** A CSV row: column name to value, with null for an empty field. */
export type CsvRow = Record<string, string | null>;

/** [PersonId without 'per_', full, first, last, middle, nick, suffix], trailing nulls dropped. */
export type PersonTuple = [string, string, string, string, (string | null)?, (string | null)?, (string | null)?];
/** [legislature, index into people, chamber, district, party, is majority (1/0), code, former codes], trailing nulls dropped. */
export type MemberTuple = [number, number, 'H' | 'S', (string | null)?, (string | null)?, (0 | 1 | null)?, (string | null)?, (string | null)?];

export interface CompactCurated {
  people: PersonTuple[];
  members: MemberTuple[];
  /** [VoteId, VoteDescription] */
  voteDescriptions: Array<[string, string]>;
}

let cache: Curated | undefined;

/** The people and members lists bundled with this package. */
export function bundledCurated(): Curated {
  cache ??= decodeCurated(CURATED);
  return {
    people: cache.people.map((p) => ({ ...p })),
    members: cache.members.map((m) => ({ ...m })),
    voteDescriptions: cache.voteDescriptions.map((d) => ({ ...d })),
  };
}

/** Parse the rows of people.csv, members.csv and vote_descriptions.csv, and check them. */
export function curatedFromCsv(peopleRows: CsvRow[], memberRows: CsvRow[], descriptionRows: CsvRow[] = []): Curated {
  const curated = {
    people: peopleRows.map(personFromCsv),
    members: memberRows.map(memberFromCsv),
    voteDescriptions: descriptionRows.map((r) => {
      if (!r.VoteId || !r.VoteDescription) throw new Error(`vote_descriptions.csv: bad row ${JSON.stringify(r)}`);
      return { VoteId: r.VoteId, VoteDescription: r.VoteDescription };
    }),
  };
  checkCurated(curated);
  return curated;
}

function personFromCsv(r: CsvRow): PersonRow {
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

function memberFromCsv(r: CsvRow): MemberRow {
  const leg = Number(r.LegislatureNumber);
  const chamber = r.MemberChamber;
  if (!Number.isInteger(leg) || !r.PersonId || (chamber !== 'H' && chamber !== 'S')) {
    throw new Error(`members.csv: bad row ${JSON.stringify(r)}`);
  }
  return {
    MemberId: memberId(leg, chamber, r.PersonId),
    LegislatureNumber: leg,
    PersonId: r.PersonId,
    MemberChamber: chamber,
    MemberDistrict: r.MemberDistrict || null,
    MemberIsMajority: parseBool(r.MemberIsMajority ?? null),
    MemberParty: r.MemberParty || null,
    MemberCode: r.MemberCode || null,
    MemberFormerCodes: r.MemberFormerCodes || null,
  };
}

function parseBool(s: string | null): boolean | null {
  if (s === null || s === '') return null;
  if (/^true$/i.test(s)) return true;
  if (/^false$/i.test(s)) return false;
  throw new Error(`Not a boolean: ${s}`);
}

/** Checks the curated tables against each other. Throws on problems. */
export function checkCurated({ people, members, voteDescriptions }: Curated): void {
  const voteIds = new Set<string>();
  for (const d of voteDescriptions) {
    if (!/^vot_\d{2}(st|nd|rd|th)_[HS]\d{4}$/.test(d.VoteId)) throw new Error(`vote_descriptions.csv: bad VoteId ${d.VoteId}`);
    if (voteIds.has(d.VoteId)) throw new Error(`vote_descriptions.csv: ${d.VoteId} listed twice`);
    voteIds.add(d.VoteId);
  }
  const personIds = new Set<string>();
  for (const p of people) {
    if (!isPersonId(p.PersonId)) throw new Error(`PersonId ${p.PersonId} must look like 'per_robyn-burke-34th'`);
    if (personIds.has(p.PersonId)) throw new Error(`Duplicate PersonId ${p.PersonId}`);
    personIds.add(p.PersonId);
  }
  const ids = new Set<string>();
  const codes = new Set<string>();
  for (const m of members) {
    if (!personIds.has(m.PersonId)) throw new Error(`Member ${m.MemberId} has unknown PersonId`);
    if (ids.has(m.MemberId)) throw new Error(`Duplicate MemberId ${m.MemberId}`);
    ids.add(m.MemberId);
    for (const code of memberCodes(m)) {
      const key = `${m.LegislatureNumber}:${code}`;
      if (codes.has(key)) throw new Error(`Member code ${code} used twice in legislature ${m.LegislatureNumber}`);
      codes.add(key);
    }
  }
}

export function encodeCurated({ people, members, voteDescriptions }: Curated): CompactCurated {
  const index = new Map(people.map((p, i) => [p.PersonId, i]));
  const trim = <T extends unknown[]>(t: T): T => {
    while (t.length && t[t.length - 1] === null) t.pop();
    return t;
  };
  return {
    people: people.map(
      (p) =>
        trim([p.PersonId.slice(4), p.PersonFullName, p.PersonFirstName, p.PersonLastName, p.PersonMiddleName, p.PersonNickName, p.PersonSuffix]) as PersonTuple,
    ),
    members: members.map((m) => {
      const i = index.get(m.PersonId);
      if (i === undefined) throw new Error(`Member ${m.MemberId} has unknown PersonId`);
      const majority = m.MemberIsMajority === null ? null : m.MemberIsMajority ? 1 : 0;
      return trim([m.LegislatureNumber, i, m.MemberChamber, m.MemberDistrict, m.MemberParty, majority, m.MemberCode, m.MemberFormerCodes]) as MemberTuple;
    }),
    voteDescriptions: voteDescriptions.map((d) => [d.VoteId, d.VoteDescription]),
  };
}

export function decodeCurated(c: CompactCurated): Curated {
  const people: PersonRow[] = c.people.map(([slug, full, first, last, middle, nick, suffix]) => ({
    PersonId: `per_${slug}`,
    PersonFullName: full,
    PersonFirstName: first,
    PersonLastName: last,
    PersonMiddleName: middle ?? null,
    PersonNickName: nick ?? null,
    PersonSuffix: suffix ?? null,
  }));
  const members: MemberRow[] = c.members.map(([leg, i, chamber, district, party, majority, code, former]) => {
    const person = people[i]?.PersonId;
    if (!person) throw new Error(`Bundled member points at missing person ${i}`);
    return {
      MemberId: memberId(leg, chamber, person),
      LegislatureNumber: leg,
      PersonId: person,
      MemberChamber: chamber,
      MemberDistrict: district ?? null,
      MemberIsMajority: majority === undefined || majority === null ? null : majority === 1,
      MemberParty: party ?? null,
      MemberCode: code ?? null,
      MemberFormerCodes: former ?? null,
    };
  });
  return { people, members, voteDescriptions: c.voteDescriptions.map(([VoteId, VoteDescription]) => ({ VoteId, VoteDescription })) };
}
