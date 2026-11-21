// Writes the published tables (CSV files), using the tables layer.
//
// The scrape is incremental: pass the previous release's CSVs as `baselineDir`
// and only the latest legislature (plus any missing ones) is re-downloaded.
// A baseline in the format published before the new IDs is converted as it's
// read. Without a baseline, every legislature is scraped (slow: hours, mostly
// bill text).
import { existsSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { BasisError, currentLegislature, type Basis, type Logger } from '../client.ts';
import {
  COLUMNS,
  FIRST_BILL_LEGISLATURE,
  FIRST_VOTE_LEGISLATURE,
  compareBy,
  createTables,
  parseAmendmentNumber,
  parseOrdinal,
  voteAmendmentId,
  type BillRow,
  type ChoiceRow,
  type Curated,
  type VoteRow,
} from '../tables/index.ts';
import { CsvWriter, readCsv, readCsvAll, writeCsv, type Row } from './csv.ts';
import { isLegacyId, legacyBillId, legacyBillVersionId, legacyMemberId, legacyVoteId } from './legacy.ts';

export { FIRST_BILL_LEGISLATURE, FIRST_VOTE_LEGISLATURE };
/** Bill text is only scraped from the 26th legislature on. */
export const FIRST_VERSION_LEGISLATURE = 26;

export interface ScrapeOptions {
  basis: Basis;
  outDir: string;
  /** People and members to use instead of the bundled lists. */
  curated?: Curated;
  /** Directory with the previous release's CSVs. */
  baselineDir?: string;
  /** Legislatures to re-download. Default: the latest one in the baseline plus any missing. */
  legislatures?: number[];
  /** Skip downloading bill text. New versions get a null BillVersionFullText. */
  skipText?: boolean;
  /** Also re-check bills that have no versions yet in older legislatures (slow). */
  recheckMissingVersions?: boolean;
  logger?: Logger;
  now?: Date;
}

export interface ScrapeSummary {
  refreshed: number[];
  rows: Record<keyof typeof COLUMNS, number>;
  warnings: string[];
}

const quietLogger: Logger = { debug() {}, info: console.error, warn: console.error, error: console.error };

export async function scrape(opts: ScrapeOptions): Promise<ScrapeSummary> {
  const log = opts.logger ?? quietLogger;
  const { basis, outDir } = opts;
  const warnings: string[] = [];
  const warn = (msg: string) => {
    warnings.push(msg);
    log.warn(msg);
  };
  const tables = createTables(basis, { curated: opts.curated, logger: { warn } });
  mkdirSync(outDir, { recursive: true });
  const baseline = opts.baselineDir;
  if (baseline && !existsSync(join(baseline, 'bills.csv'))) {
    throw new Error(`Baseline directory ${baseline} has no bills.csv`);
  }
  const current = currentLegislature(opts.now);

  // --- curated people and members -----------------------------------------
  const rows = {} as ScrapeSummary['rows'];
  rows.people = await writeCsv(join(outDir, 'people.csv'), COLUMNS.people, await tables.people());
  rows.members = await writeCsv(join(outDir, 'members.csv'), COLUMNS.members, await tables.members());

  // --- which legislatures to refresh --------------------------------------
  const baseBills = baseline ? (await readCsvAll(join(baseline, 'bills.csv'))).map(billFromCsv) : [];
  const refresh = opts.legislatures
    ? [...opts.legislatures].sort((a, b) => a - b)
    : legislaturesToRefresh(new Set(baseBills.map((b) => b.LegislatureNumber)), current);
  const refreshSet = new Set(refresh);
  log.info(`Refreshing legislatures: ${refresh.join(', ')}`);

  // --- bills ---------------------------------------------------------------
  const bills: BillRow[] = baseBills.filter((b) => !refreshSet.has(b.LegislatureNumber));
  const billsByLeg = new Map<number, BillRow[]>();
  for (const leg of refresh.filter((l) => l >= FIRST_BILL_LEGISLATURE)) {
    const fresh = await unlessUnavailable(() => tables.bills({ legislature: leg }));
    if (fresh === undefined) {
      warn(`Legislature ${leg}: no data from the API yet; keeping baseline rows`);
      bills.push(...baseBills.filter((b) => b.LegislatureNumber === leg));
      continue;
    }
    log.info(`Legislature ${leg}: ${fresh.length} bills`);
    billsByLeg.set(leg, fresh);
    bills.push(...fresh);
  }
  bills.sort(compareBy((b) => b.BillId));
  rows.bills = await writeCsv(join(outDir, 'bills.csv'), COLUMNS.bills, bills);

  // --- votes and choices ---------------------------------------------------
  const voteLegs = [...billsByLeg.keys()].filter((l) => l >= FIRST_VOTE_LEGISLATURE);
  const voteLegSet = new Set(voteLegs);
  const baseVotes: VoteRow[] = baseline ? (await readCsvAll(join(baseline, 'votes.csv'))).map(voteFromCsv) : [];
  const descriptions = new Map(baseVotes.filter((v) => v.VoteDescription).map((v) => [v.VoteId, v.VoteDescription]));
  const votes = baseVotes.filter((v) => !voteLegSet.has(v.LegislatureNumber));
  const choices: ChoiceRow[] = [];
  if (baseline) {
    for await (const r of readCsv(join(baseline, 'choices.csv'))) {
      const c = choiceFromCsv(r);
      if (!voteLegSet.has(legOf(c.VoteId))) choices.push(c);
    }
  }
  for (const leg of voteLegs) {
    const fresh = await tables.votes({ legislature: leg, bills: billsByLeg.get(leg) });
    for (const v of fresh.votes) v.VoteDescription = descriptions.get(v.VoteId) ?? '';
    log.info(`Legislature ${leg}: ${fresh.votes.length} votes, ${fresh.choices.length} choices`);
    votes.push(...fresh.votes);
    choices.push(...fresh.choices);
  }
  votes.sort(compareBy((v) => v.VoteId));
  choices.sort(compareBy((c) => c.VoteId, (c) => c.MemberId));
  rows.votes = await writeCsv(join(outDir, 'votes.csv'), COLUMNS.votes, votes);
  rows.choices = await writeCsv(join(outDir, 'choices.csv'), COLUMNS.choices, choices);

  // --- bill versions (with full text) --------------------------------------
  // Existing versions are copied from the baseline as-is; the file is large, so stream it.
  const out = new CsvWriter(join(outDir, 'bill_versions.csv'), COLUMNS.bill_versions);
  const haveVersion = new Set<string>();
  const billsWithVersions = new Set<string>();
  if (baseline) {
    for await (const r of readCsv(join(baseline, 'bill_versions.csv'))) {
      const row = versionFromCsv(r);
      haveVersion.add(row.BillVersionId!);
      billsWithVersions.add(row.BillId!);
      await out.write(row);
    }
  }
  const toCheck = bills.filter(
    (b) =>
      b.LegislatureNumber >= FIRST_VERSION_LEGISLATURE &&
      (billsByLeg.has(b.LegislatureNumber) || (opts.recheckMissingVersions && !billsWithVersions.has(b.BillId))),
  );
  log.info(`Checking ${toCheck.length} bills for new versions`);
  let added = 0;
  for (const leg of new Set(toCheck.map((b) => b.LegislatureNumber))) {
    const found = await tables.billVersions({
      legislature: leg,
      bills: toCheck.filter((b) => b.LegislatureNumber === leg).map((b) => b.BillNumber),
      text: !opts.skipText,
      skip: haveVersion,
    });
    for (const row of found) await out.write(row);
    added += found.length;
  }
  await out.close();
  log.info(`Added ${added} new bill versions`);
  rows.bill_versions = out.rows;

  return { refreshed: refresh, rows, warnings };
}

/** Missing legislatures, plus the latest one we have and the current one (which may have changed). */
export function legislaturesToRefresh(have: Set<number>, current: number): number[] {
  const out: number[] = [];
  const latest = have.size ? Math.max(...have) : undefined;
  for (let l = FIRST_BILL_LEGISLATURE; l <= current; l++) if (!have.has(l) || l === latest || l === current) out.push(l);
  return out;
}

async function unlessUnavailable<T>(f: () => Promise<T>): Promise<T | undefined> {
  try {
    return await f();
  } catch (e) {
    if (e instanceof BasisError && e.isDataUnavailable) return undefined;
    throw e;
  }
}

// ---------------------------------------------------------------------------
// reading the baseline (previous release's CSVs, old or new IDs)
// ---------------------------------------------------------------------------

/** 'vot_34th_H0396' -> 34 */
function legOf(id: string): number {
  return parseOrdinal(id.split('_')[1]!);
}

function upgrade(id: string, convert: (old: string) => string): string {
  return isLegacyId(id) ? convert(id) : id;
}

export function billFromCsv(r: Row): BillRow {
  const inner = (r.BillSubjects ?? '[]').replace(/^\[|\]$/g, '');
  return {
    BillId: upgrade(r.BillId!, legacyBillId),
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
  const row: VoteRow = {
    VoteId: upgrade(r.VoteId!, legacyVoteId),
    BillId: r.BillId ? upgrade(r.BillId, legacyBillId) : null,
    VoteNumber: Number(r.VoteNumber),
    VoteDate: r.VoteDate ?? null,
    VoteTitle: r.VoteTitle ?? null,
    LegislatureNumber: Number(r.LegislatureNumber),
    VoteChamber: r.VoteChamber as 'H' | 'S',
    VoteDescription: r.VoteDescription ?? '',
    VoteAmendmentNumber: numOrNull(r.VoteAmendmentNumber),
    VoteSubAmendmentNumber: numOrNull(r.VoteSubAmendmentNumber),
    VoteAmendmentId: r.VoteAmendmentId ?? null,
  };
  if ('VoteBillAmendmentNumber' in r) {
    // Old format: work the new amendment columns out from the title.
    const a = parseAmendmentNumber(row.VoteTitle);
    row.VoteAmendmentNumber = a?.number ?? null;
    row.VoteSubAmendmentNumber = a?.sub ?? null;
    row.VoteAmendmentId =
      a && row.BillId ? voteAmendmentId(row.LegislatureNumber, row.BillId.split('_')[2]!, row.VoteChamber, a) : null;
  }
  return row;
}

function numOrNull(s: string | null | undefined): number | null {
  return s == null || s === '' ? null : Number(s);
}

export function choiceFromCsv(r: Row): ChoiceRow {
  return {
    VoteId: upgrade(r.VoteId!, legacyVoteId),
    MemberId: upgrade(r.MemberId!, legacyMemberId),
    Choice: r.Choice ?? null,
  };
}

function versionFromCsv(r: Row): Row {
  return { ...r, BillVersionId: upgrade(r.BillVersionId!, legacyBillVersionId), BillId: upgrade(r.BillId!, legacyBillId) };
}
