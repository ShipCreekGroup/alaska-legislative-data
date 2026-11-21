// `akleg build`: rebuild the published tables and their history from the raw
// store and the curated lists. Nothing is read from a previous release, so a
// fix to the cleaning code corrects every past version too.
//
// For each fetch date, each legislature fetched that day is rebuilt from the
// responses as of that date and compared with the versions so far (see
// HistoryTable). A legislature whose responses didn't change that day is
// skipped, since its tables can't have changed either.
import { mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { BasisError, createBasis, type Logger } from '../client.ts';
import { NotInStoreError, RawIndex, replayFetch, type RawStore } from '../store/raw.ts';
import {
  COLUMNS,
  FIRST_BILL_LEGISLATURE,
  FIRST_VOTE_LEGISLATURE,
  bundledCurated,
  createTables,
  parseOrdinal,
  type Curated,
  type TableName,
} from '../tables/index.ts';
import { FIRST_VERSION_LEGISLATURE } from './fetch.ts';
import { HistoryTable } from './history.ts';

export interface BuildOptions {
  raw: RawStore;
  outDir: string;
  curated?: Curated;
  /** Build as of this date (yyyy-mm-dd). Default: the latest fetch. */
  date?: string;
  logger?: Logger;
}

export interface BuildSummary {
  /** Fetch dates the history covers. */
  dates: string[];
  /** Legislature builds done (the rest were skipped as unchanged). */
  builds: number;
  rows: Record<TableName, { current: number; history: number }>;
  warnings: string[];
}

const quiet: Logger = { debug() {}, info: console.error, warn: console.error, error: console.error };

/** The legislature in an ID like 'bil_34th_HB0016', or in a choice key 'vot_34th_H0001 mem_...'. */
function legislatureOfKey(key: string): number | null {
  const part = key.split('_')[1];
  return part && /^\d{2}(st|nd|rd|th)$/.test(part) ? parseOrdinal(part) : null;
}

export async function build(opts: BuildOptions): Promise<BuildSummary> {
  const log = opts.logger ?? quiet;
  const warnings = new Set<string>();
  const curated = opts.curated ?? bundledCurated();
  const index = new RawIndex(await opts.raw.loadLog());
  const dates = index.dates().filter((d) => !opts.date || d <= opts.date);
  if (!dates.length) throw new Error(`The raw store ${opts.raw.blobs.name} has no fetches${opts.date ? ` on or before ${opts.date}` : ''}`);
  mkdirSync(opts.outDir, { recursive: true });
  const tmp = mkdtempSync(join(tmpdir(), 'akleg-build-'));

  const key = (cols: string[]) => (row: object) => cols.map((c) => (row as Record<string, string>)[c]).join(' ');
  const tables: Record<TableName, HistoryTable> = {
    people: new HistoryTable('people', COLUMNS.people, key(['PersonId']), () => null, tmp),
    members: new HistoryTable('members', COLUMNS.members, key(['MemberId']), legislatureOfKey, tmp),
    bills: new HistoryTable('bills', COLUMNS.bills, key(['BillId']), legislatureOfKey, tmp),
    votes: new HistoryTable('votes', COLUMNS.votes, key(['VoteId']), legislatureOfKey, tmp),
    choices: new HistoryTable('choices', COLUMNS.choices, key(['VoteId', 'MemberId']), legislatureOfKey, tmp),
    bill_versions: new HistoryTable('bill_versions', COLUMNS.bill_versions, key(['BillVersionId']), legislatureOfKey, tmp),
  };

  try {
    // The curated lists are the same on every date.
    tables.people.apply(dates[0]!, null, curated.people);
    tables.members.apply(dates[0]!, null, curated.members);

    const last = new Map<number, string>();
    let builds = 0;
    for (const date of dates) {
      const view = index.view(date);
      for (const leg of index.legislaturesOn(date).filter((l) => l >= FIRST_BILL_LEGISLATURE)) {
        const fingerprint = index.fingerprint(date, leg);
        if (last.get(leg) === fingerprint) continue;
        last.set(leg, fingerprint);
        const isLast = date === dates.at(-1);
        const replay = createTables(createBasis({ fetch: replayFetch(opts.raw, view), maxAttempts: 1, logger: quiet }), {
          curated,
          logger: { warn: (m: string) => void (isLast && warnings.add(m)) },
        });
        let bills, votes, versions;
        try {
          bills = await replay.bills({ legislature: leg });
          votes = leg >= FIRST_VOTE_LEGISLATURE ? await replay.votes({ legislature: leg, bills }) : { votes: [], choices: [] };
          versions =
            leg >= FIRST_VERSION_LEGISLATURE ? await replay.billVersions({ legislature: leg, bills: bills.map((b) => b.BillNumber) }) : [];
        } catch (e) {
          if (e instanceof BasisError && e.isDataUnavailable) continue;
          if (e instanceof NotInStoreError) {
            // eg a fetch that stopped part way: keep the earlier versions.
            warnings.add(`Legislature ${leg} on ${date}: ${e.message}; skipped`);
            last.delete(leg);
            continue;
          }
          throw e;
        }
        const changes = [
          tables.bills.apply(date, leg, bills),
          tables.votes.apply(date, leg, votes.votes),
          tables.choices.apply(date, leg, votes.choices),
          tables.bill_versions.apply(date, leg, versions),
        ];
        builds++;
        const added = changes.reduce((n, c) => n + c.added, 0);
        const closed = changes.reduce((n, c) => n + c.closed, 0);
        log.info(`${date} legislature ${leg}: ${added} new row versions, ${closed} rows gone`);
      }
    }

    const rows = {} as BuildSummary['rows'];
    for (const [name, t] of Object.entries(tables) as Array<[TableName, HistoryTable]>) rows[name] = await t.write(opts.outDir);
    for (const w of warnings) log.warn(w);
    return { dates, builds, rows, warnings: [...warnings] };
  } finally {
    for (const t of Object.values(tables)) t.close();
    rmSync(tmp, { recursive: true, force: true });
  }
}
