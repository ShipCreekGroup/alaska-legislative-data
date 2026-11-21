// IDs of the published tables. Every ID is a 3-letter type prefix plus fields
// joined by `_`. Fields never contain `_` (slugs use `-`), have no spaces, and
// are zero-padded so IDs sort correctly as text:
//
//   PersonId       per_robyn-burke-34th          stored in people.csv; never changes
//   MemberId       mem_34th_H_robyn-burke-34th   legislature, chamber, person
//   BillId         bil_34th_HB0016               legislature, bill code
//   BillVersionId  ver_34th_HB0016_A             legislature, bill code, version letter
//   VoteId         vot_34th_H0396                legislature, roll-call number
//   AmendmentId    amd_34th_HB0016_H011          legislature, bill code, chamber + amendment number
//                  amd_34th_HB0016_H002_10       ... + sub-amendment (No. 10 to No. 2)

/** 34 -> '34th', 1 -> '01st', 22 -> '22nd'. Two digits last until the 100th Legislature. */
export function ordinal(legislature: number): string {
  if (!Number.isInteger(legislature) || legislature < 1 || legislature > 99) {
    throw new Error(`Legislature must be 1-99: ${legislature}`);
  }
  const teen = legislature % 100 >= 11 && legislature % 100 <= 13;
  const suffix = teen ? 'th' : (({ 1: 'st', 2: 'nd', 3: 'rd' } as Record<number, string>)[legislature % 10] ?? 'th');
  return `${String(legislature).padStart(2, '0')}${suffix}`;
}

/** '34th' -> 34 */
export function parseOrdinal(s: string): number {
  const m = /^(\d{2})(st|nd|rd|th)$/.exec(s);
  if (!m || ordinal(Number(m[1])) !== s) throw new Error(`Not a legislature ordinal: ${s}`);
  return Number(m[1]);
}

/** ASCII, lowercase, hyphens: 'Cathy Muñoz' -> 'cathy-munoz', 'Steve St. Clair' -> 'steve-st-clair'. */
export function slugify(name: string): string {
  const s = name
    .normalize('NFKD')
    .replace(/[̀-ͯ]/g, '')
    .toLowerCase()
    .replace(/[.'’]/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
  if (!s) throw new Error(`Can't make a slug from ${JSON.stringify(name)}`);
  return s;
}

/** A new person's ID, from the name they first served under and their first legislature. */
export function personId(name: string, firstLegislature: number): string {
  return `per_${slugify(name)}-${ordinal(firstLegislature)}`;
}

const PERSON_ID = /^per_([a-z0-9]+(?:-[a-z0-9]+)*-\d{2}(?:st|nd|rd|th))$/;

export function isPersonId(id: string): boolean {
  return PERSON_ID.test(id);
}

export function memberId(legislature: number, chamber: string, person: string): string {
  const m = PERSON_ID.exec(person);
  if (!m) throw new Error(`Not a PersonId: ${person}`);
  if (chamber !== 'H' && chamber !== 'S') throw new Error(`Not a chamber: ${chamber}`);
  return `mem_${ordinal(legislature)}_${chamber}_${m[1]}`;
}

/**
 * 'HB 16' -> 'HB0016', the Legislature's own padded form (as in its PDF links).
 * About 100 bills in the 13th-17th legislatures have garbled numbers in the
 * API, eg '5B 152' or 'S8 126'. Those keep their letters and digits as they
 * are ('5B152'), which can't clash with a well-formed code.
 */
export function billCode(billNumber: string): string {
  const m = /^\s*([A-Z]{2,4})\s*0*(\d{1,4})\s*$/.exec(billNumber);
  if (m) return `${m[1]}${m[2]!.padStart(4, '0')}`;
  const fallback = billNumber.toUpperCase().replace(/[^A-Z0-9]/g, '');
  if (!fallback) throw new Error(`Can't make a bill code from ${JSON.stringify(billNumber)}`);
  return fallback;
}

export function billId(legislature: number, billNumber: string): string {
  return `bil_${ordinal(legislature)}_${billCode(billNumber)}`;
}

export function billVersionId(legislature: number, billNumber: string, letter: string): string {
  if (!/^[A-Z]{1,2}$/.test(letter)) throw new Error(`Not a version letter: ${letter}`);
  return `ver_${ordinal(legislature)}_${billCode(billNumber)}_${letter}`;
}

/** 'H0396', 'H 396' or 'H396' -> 'H0396' */
export function rollCall(voteNum: string): string {
  const m = /^\s*([HS])\s*0*(\d+)\s*$/.exec(voteNum);
  if (!m || m[2]!.length > 4) throw new Error(`Can't parse roll-call number: ${voteNum}`);
  return `${m[1]}${m[2]!.padStart(4, '0')}`;
}

export function voteId(legislature: number, voteNum: string): string {
  return `vot_${ordinal(legislature)}_${rollCall(voteNum)}`;
}

export function amendmentId(
  legislature: number,
  billNumber: string,
  chamber: string,
  amendment: number,
  sub: number | null = null,
): string {
  if (chamber !== 'H' && chamber !== 'S') throw new Error(`Not a chamber: ${chamber}`);
  if (!Number.isInteger(amendment) || amendment < 1 || amendment > 999)
    throw new Error(`Bad amendment number ${amendment}`);
  let id = `amd_${ordinal(legislature)}_${billCode(billNumber)}_${chamber}${String(amendment).padStart(3, '0')}`;
  if (sub !== null) {
    if (!Number.isInteger(sub) || sub < 1 || sub > 99) throw new Error(`Bad sub-amendment number ${sub}`);
    id += `_${String(sub).padStart(2, '0')}`;
  }
  return id;
}
