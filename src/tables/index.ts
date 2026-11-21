// The cleaned layer: rows of the published tables (people, members, bills,
// votes, choices, bill_versions), fetched through a raw client.
//
//   import { createBasis } from 'alaska-legislative-api';
//   import { createTables } from 'alaska-legislative-api/tables';
//   const tables = createTables(createBasis());
//   const bills = await tables.bills({ legislature: 34 });
import type { Basis, Logger } from '../client.ts';
import type { Vote } from '../types.gen.ts';
import { cleanBill, cleanBillVersion, compareBy, memberCodes, splitVotes } from './clean.ts';
import { bundledCurated, checkCurated, type Curated } from './curated.ts';
import { billId } from './ids.ts';
import type { BillRow, BillVersionRow, ChoiceRow, MemberRow, PersonRow, VoteRow } from './rows.ts';

export * from './clean.ts';
export * from './curated.ts';
export * from './ids.ts';
export * from './rows.ts';

/** The API has no bills before the 12th legislature. */
export const FIRST_BILL_LEGISLATURE = 12;
/** The API has no roll-call votes before the 19th legislature. */
export const FIRST_VOTE_LEGISLATURE = 19;

/** Members whose vote requests always fail on the server. */
const KNOWN_FAILING_MEMBERS = new Set(['18:GRS', '18:HUD', '18:DVG']);

export interface TablesOptions {
  /** People, members and vote descriptions to use instead of the bundled lists, eg read from edited CSVs. */
  curated?: Curated;
  /** Problems in the data (eg votes by unknown member codes) go to `logger.warn`. Default: `console.warn`. */
  logger?: Pick<Logger, 'warn'>;
  /** Requests in flight at once when one table needs many (votes, bill versions). Default 20. */
  concurrency?: number;
}

export interface LegislatureParams {
  /** eg 34 for 2025-2026 */
  legislature: number;
}

export interface VotesParams extends LegislatureParams {
  /** The legislature's bills, to link votes to them. Fetched if not given. */
  bills?: BillRow[];
}

export interface VotesResult {
  votes: VoteRow[];
  choices: ChoiceRow[];
}

export interface BillVersionsParams extends LegislatureParams {
  /** Bill numbers to check, eg ['HB 16']. Default: every bill in the legislature. */
  bills?: string[];
  /** Download each version's plain text. Default true. A function decides per version. */
  text?: boolean | ((version: { legislature: number; bill: string; version: string; id: string }) => boolean);
  /** BillVersionIds to leave out, eg ones you already have. Their text isn't downloaded. */
  skip?: Set<string>;
}

export interface Tables {
  people(): Promise<PersonRow[]>;
  /** Every legislature's members, or one legislature's. */
  members(params?: Partial<LegislatureParams>): Promise<MemberRow[]>;
  bills(params: LegislatureParams): Promise<BillRow[]>;
  /**
   * Roll calls and each member's choice. Fetched member by member, so this
   * takes one request per member code.
   */
  votes(params: VotesParams): Promise<VotesResult>;
  /** One request per bill, plus one per version for the text. */
  billVersions(params: BillVersionsParams): Promise<BillVersionRow[]>;
}

/**
 * Throws `BasisError` with `isDataUnavailable` for a legislature the API has
 * no data for yet.
 */
export function createTables(basis: Basis, options: TablesOptions = {}): Tables {
  const curated = options.curated ?? bundledCurated();
  if (options.curated) checkCurated(curated);
  const warn = (msg: string) => (options.logger ?? console).warn(msg);
  const concurrency = options.concurrency ?? 20;
  const descriptions = new Map(curated.voteDescriptions.map((d) => [d.VoteId, d.VoteDescription]));

  const bills = async ({ legislature }: LegislatureParams): Promise<BillRow[]> => {
    const raw = await basis.bills.list({ session: legislature, query: { subjects: true } });
    const byId = new Map<string, BillRow>();
    for (const b of raw) {
      const row = cleanBill(legislature, b);
      if (byId.has(row.BillId)) warn(`Duplicate bill ${row.BillId}; keeping the first`);
      else byId.set(row.BillId, row);
    }
    return [...byId.values()].sort(compareBy((b) => b.BillId));
  };

  return {
    people: async () => curated.people.map((p) => ({ ...p })),

    members: async ({ legislature } = {}) =>
      curated.members
        .filter((m) => legislature === undefined || m.LegislatureNumber === legislature)
        .map((m) => ({ ...m })),

    bills,

    async votes({ legislature, bills: given }) {
      if (legislature < FIRST_VOTE_LEGISLATURE) return { votes: [], choices: [] };
      const billRows = given ?? (await bills({ legislature }));
      const members = curated.members.filter((m) => m.LegislatureNumber === legislature);
      const codes = [...new Set(members.flatMap(memberCodes))];
      const perCode = await mapLimit(codes, concurrency, async (code) => {
        try {
          const ms = await basis.members.list({ session: legislature, query: { members: { code }, votes: true } });
          return ms.flatMap((m) => m.Votes ?? []).map((vote: Vote) => ({ leg: legislature, vote }));
        } catch (e) {
          if (KNOWN_FAILING_MEMBERS.has(`${legislature}:${code}`)) {
            warn(`Skipping votes for ${legislature}:${code} (known server error)`);
            return [];
          }
          throw e;
        }
      });
      const split = splitVotes(perCode.flat(), new Set(billRows.map((b) => b.BillId)), members);
      for (const v of split.votes) v.VoteDescription = descriptions.get(v.VoteId) ?? '';
      for (const u of new Set(split.unmatched.map((u) => `${u.leg}:${u.code}`))) {
        warn(`Votes by member code ${u} have no matching member; dropped`);
      }
      return {
        votes: split.votes.sort(compareBy((v) => v.VoteId)),
        choices: split.choices.sort(
          compareBy(
            (c) => c.VoteId,
            (c) => c.MemberId,
          ),
        ),
      };
    },

    async billVersions({ legislature, bills: numbers, text = true, skip = new Set() }) {
      const billNumbers = numbers ?? (await bills({ legislature })).map((b) => b.BillNumber);
      const perBill = await mapLimit(billNumbers, concurrency, async (number) => {
        const detail = await basis.bills.list({
          session: legislature,
          query: { bills: { bill: number }, versions: { fulltext: 'urlonly' } },
        });
        const found = new Map<string, BillVersionRow>();
        for (const bill of detail) {
          if (billId(legislature, bill.BillNumber) !== billId(legislature, number)) continue;
          for (const v of bill.Versions ?? []) {
            const row = cleanBillVersion(legislature, number, v, null);
            if (skip.has(row.BillVersionId) || found.has(row.BillVersionId)) continue;
            const wanted =
              typeof text === 'function'
                ? text({ legislature, bill: number, version: v.VersionLetter, id: row.BillVersionId })
                : text;
            if (wanted) {
              try {
                row.BillVersionFullText = await basis.billText.get({
                  session: legislature,
                  bill: number,
                  version: v.VersionLetter,
                });
              } catch (e) {
                warn(`No text for ${row.BillVersionId}: ${String(e)}`);
              }
            }
            found.set(row.BillVersionId, row);
          }
        }
        return [...found.values()];
      });
      return perBill.flat().sort(compareBy((v) => v.BillVersionId));
    },
  };
}

/** Like Promise.all(items.map(f)), with at most `limit` pending at once. Order is preserved. */
export async function mapLimit<T, R>(items: T[], limit: number, f: (x: T) => Promise<R>): Promise<R[]> {
  const out: R[] = [];
  let next = 0;
  const worker = async () => {
    while (next < items.length) {
      const i = next++;
      out[i] = await f(items[i]!);
    }
  };
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
  return out;
}
