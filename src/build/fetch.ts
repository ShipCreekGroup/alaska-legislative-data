// `akleg fetch`: download legislatures from the API into the raw store.
//
// The same tables code that `akleg build` replays makes the requests, so
// every response a build needs is recorded. Bill text is downloaded once per
// version; every other response is re-requested on each fetch.
import { BasisError, billTextUrl, createBasis, currentLegislature, type ClientOptions, type Logger } from '../client.ts';
import { RawIndex, replayFetch, type LogEntry, type RawStore } from '../store/raw.ts';
import { FIRST_BILL_LEGISLATURE, FIRST_VOTE_LEGISLATURE, createTables, type Curated } from '../tables/index.ts';

/** Bill text is only kept from the 26th legislature on. */
export const FIRST_VERSION_LEGISLATURE = 26;

export interface FetchOptions {
  raw: RawStore;
  /** Legislatures to fetch. Default: the current one, the latest with data, and any never fetched. */
  legislatures?: number[];
  /** Fetch every legislature (the one-time full fill takes hours, mostly bill text). */
  all?: boolean;
  /** Don't download bill text for new versions. */
  skipText?: boolean;
  /** Also re-check bills that had no versions yet in legislatures not being fetched (slow). */
  recheckMissingVersions?: boolean;
  curated?: Curated;
  /** Options for the live client, eg `fetch` in tests. */
  client?: ClientOptions;
  logger?: Logger;
  /** The run's date, yyyy-mm-dd. Default today (UTC). */
  date?: string;
  now?: Date;
}

export interface FetchSummary {
  date: string;
  legislatures: number[];
  requests: number;
  warnings: string[];
}

const quiet: Logger = { debug() {}, info: console.error, warn: console.error, error: console.error };

export async function fetchRaw(opts: FetchOptions): Promise<FetchSummary> {
  const log = opts.logger ?? quiet;
  const date = opts.date ?? (opts.now ?? new Date()).toISOString().slice(0, 10);
  const warnings: string[] = [];
  const warn = (m: string) => {
    warnings.push(m);
    log.warn(m);
  };
  const index = new RawIndex(await opts.raw.loadLog());
  const current = currentLegislature(opts.now);
  const legislatures = opts.legislatures ?? defaultLegislatures(index, current, opts.all ?? false);
  log.info(`Fetching legislatures ${legislatures.join(', ')} into ${opts.raw.blobs.name}`);

  const sink: LogEntry[] = [];
  const baseFetch = opts.client?.fetch ?? ((url, init) => fetch(url, init));
  const basis = createBasis({ logger: log, ...opts.client, fetch: opts.raw.recordingFetch(baseFetch, sink, date) });
  const tables = createTables(basis, { curated: opts.curated, logger: { warn } });
  const haveText = index.textUrls();
  const wantText = ({ legislature, bill, version }: { legislature: number; bill: string; version: string }) =>
    !opts.skipText && !haveText.has(billTextUrl({ session: legislature, bill, version }));

  try {
    for (const leg of legislatures) {
      let bills;
      try {
        bills = await tables.bills({ legislature: leg });
      } catch (e) {
        if (e instanceof BasisError && e.isDataUnavailable) {
          log.info(`Legislature ${leg}: no data from the API yet`);
          continue;
        }
        throw e;
      }
      log.info(`Legislature ${leg}: ${bills.length} bills`);
      if (leg >= FIRST_VOTE_LEGISLATURE) {
        const { votes } = await tables.votes({ legislature: leg, bills });
        log.info(`Legislature ${leg}: ${votes.length} votes`);
      }
      if (leg >= FIRST_VERSION_LEGISLATURE) {
        const versions = await tables.billVersions({ legislature: leg, bills: bills.map((b) => b.BillNumber), text: wantText });
        log.info(`Legislature ${leg}: ${versions.length} bill versions`);
      }
    }

    if (opts.recheckMissingVersions) {
      // For legislatures not fetched now: ask again about bills whose last answer had no versions.
      const replay = createTables(createBasis({ fetch: replayFetch(opts.raw, index.view(date)), maxAttempts: 1, logger: quiet }), {
        curated: opts.curated,
        logger: { warn() {} },
      });
      for (const leg of index.legislatures().filter((l) => l >= FIRST_VERSION_LEGISLATURE && !legislatures.includes(l))) {
        const bills = await replay.bills({ legislature: leg });
        const withVersions = new Set((await replay.billVersions({ legislature: leg, text: false })).map((v) => v.BillId));
        const missing = bills.filter((b) => !withVersions.has(b.BillId)).map((b) => b.BillNumber);
        log.info(`Legislature ${leg}: re-checking ${missing.length} bills with no versions`);
        await tables.billVersions({ legislature: leg, bills: missing, text: wantText });
      }
    }
  } finally {
    // Keep what was fetched even if the run fails part way.
    await opts.raw.appendLog(date, sink);
  }
  log.info(`Recorded ${sink.length} responses for ${date}`);
  return { date, legislatures, requests: sink.length, warnings };
}

/** The current legislature, the latest one with data, and any never fetched (all of them on the first run). */
export function defaultLegislatures(index: RawIndex, current: number, all: boolean): number[] {
  const have = new Set(index.legislatures());
  const out: number[] = [];
  const latest = have.size ? Math.max(...have) : undefined;
  for (let l = FIRST_BILL_LEGISLATURE; l <= current; l++) {
    if (all || !have.has(l) || l === latest || l === current) out.push(l);
  }
  return out;
}
