// Builds the published tables (CSV files) from the BASIS API.
//
// The scrape is incremental: pass the previous release's CSVs as `baselineDir`
// and only the latest legislature (plus any missing ones) is re-downloaded.
// Without a baseline, every legislature is scraped (slow: hours, mostly bill text).
import { existsSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { BasisClient, BasisError, currentLegislature, type Logger } from '../client.ts';
import type { Bill, Vote } from '../types.gen.ts';
import { CsvWriter, readCsv, readCsvAll, writeCsv } from './csv.ts';
import {
  COLUMNS,
  billFromCsv,
  checkCurated,
  choiceFromCsv,
  cleanBill,
  cleanBillVersion,
  cleanMember,
  cleanPerson,
  compareBy,
  legOf,
  memberCodes,
  splitVotes,
  voteFromCsv,
  type BillRow,
  type BillVersionRow,
  type ChoiceRow,
  type MemberRow,
  type VoteRow,
} from './clean.ts';

/** The API has no bills before the 12th legislature. */
export const FIRST_BILL_LEGISLATURE = 12;
/** The API has no roll-call votes before the 19th legislature. */
export const FIRST_VOTE_LEGISLATURE = 19;
/** Bill text is only scraped from the 26th legislature on. */
export const FIRST_VERSION_LEGISLATURE = 26;

/** Members whose vote requests always fail on the server. */
const KNOWN_FAILING_MEMBERS = new Set(['18:GRS', '18:HUD', '18:DVG']);

export interface ScrapeOptions {
  client: BasisClient;
  outDir: string;
  /** Directory with the curated people.csv and members.csv. */
  curatedDir: string;
  /** Directory with the previous release's CSVs. */
  baselineDir?: string;
  /** Legislatures to re-download. Default: the latest one in the baseline plus any missing. */
  legislatures?: number[];
  /** Skip downloading bill text. New versions get a null BillVersionFullText. */
  skipText?: boolean;
  /** Also re-check bills that have no versions yet in older legislatures (slow, like the old Python scraper). */
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
  const { client, outDir } = opts;
  const warnings: string[] = [];
  const warn = (msg: string) => {
    warnings.push(msg);
    log.warn(msg);
  };
  mkdirSync(outDir, { recursive: true });
  const baseline = opts.baselineDir;
  if (baseline && !existsSync(join(baseline, 'bills.csv'))) {
    throw new Error(`Baseline directory ${baseline} has no bills.csv`);
  }
  const current = currentLegislature(opts.now);

  // --- curated people and members -----------------------------------------
  const people = (await readCsvAll(join(opts.curatedDir, 'people.csv'))).map(cleanPerson);
  const members = (await readCsvAll(join(opts.curatedDir, 'members.csv'))).map(cleanMember);
  checkCurated(people, members);
  const rows = {} as ScrapeSummary['rows'];
  rows.people = await writeCsv(join(outDir, 'people.csv'), COLUMNS.people, people);
  rows.members = await writeCsv(join(outDir, 'members.csv'), COLUMNS.members, members);

  // --- which legislatures to refresh --------------------------------------
  const baseBills = baseline ? (await readCsvAll(join(baseline, 'bills.csv'))).map(billFromCsv) : [];
  const refresh = opts.legislatures
    ? [...opts.legislatures].sort((a, b) => a - b)
    : legislaturesToRefresh(new Set(baseBills.map((b) => b.LegislatureNumber)), current);
  const refreshSet = new Set(refresh);
  log.info(`Refreshing legislatures: ${refresh.join(', ')}`);

  // --- bills ---------------------------------------------------------------
  const bills: BillRow[] = baseBills.filter((b) => !refreshSet.has(b.LegislatureNumber));
  const available = new Set<number>();
  for (const leg of refresh.filter((l) => l >= FIRST_BILL_LEGISLATURE)) {
    const raw = await unlessUnavailable(() => client.bills({ session: leg, query: { subjects: true } }));
    if (raw === undefined) {
      warn(`Legislature ${leg}: no data from the API yet; keeping baseline rows`);
      bills.push(...baseBills.filter((b) => b.LegislatureNumber === leg));
      continue;
    }
    available.add(leg);
    const byId = new Map<string, BillRow>();
    for (const b of raw) {
      const row = cleanBill(leg, b);
      if (byId.has(row.BillId)) warn(`Duplicate bill ${row.BillId}; keeping the first`);
      else byId.set(row.BillId, row);
    }
    log.info(`Legislature ${leg}: ${byId.size} bills`);
    bills.push(...byId.values());
  }
  bills.sort(compareBy((b) => b.LegislatureNumber, (b) => b.BillNumber));
  rows.bills = await writeCsv(join(outDir, 'bills.csv'), COLUMNS.bills, bills);
  const billIds = new Set(bills.map((b) => b.BillId));

  // --- votes and choices ---------------------------------------------------
  const voteLegs = refresh.filter((l) => l >= FIRST_VOTE_LEGISLATURE && available.has(l));
  const voteLegSet = new Set(voteLegs);
  const baseVotes: VoteRow[] = baseline ? (await readCsvAll(join(baseline, 'votes.csv'))).map(voteFromCsv) : [];
  const descriptions = new Map(baseVotes.filter((v) => v.VoteDescription).map((v) => [v.VoteId, v.VoteDescription]));
  const votes = baseVotes.filter((v) => !voteLegSet.has(v.LegislatureNumber));
  const choices: ChoiceRow[] = [];
  if (baseline) {
    for await (const r of readCsv(join(baseline, 'choices.csv'))) {
      if (!voteLegSet.has(legOf(r.VoteId!))) choices.push(choiceFromCsv(r));
    }
  }
  for (const leg of voteLegs) {
    const raw = await scrapeVotes(client, leg, members, warn);
    const split = splitVotes(raw, billIds, members);
    for (const u of dedupe(split.unmatched.map((u) => `${u.leg}:${u.code}`))) {
      warn(`Votes by member code ${u} have no matching row in members.csv; dropped`);
    }
    for (const v of split.votes) v.VoteDescription = descriptions.get(v.VoteId) ?? '';
    log.info(`Legislature ${leg}: ${split.votes.length} votes, ${split.choices.length} choices`);
    votes.push(...split.votes);
    choices.push(...split.choices);
  }
  votes.sort(compareBy((v) => v.LegislatureNumber, (v) => v.VoteChamber, (v) => v.VoteNumber));
  choices.sort(compareBy((c) => c.ChoiceId));
  rows.votes = await writeCsv(join(outDir, 'votes.csv'), COLUMNS.votes, votes);
  rows.choices = await writeCsv(join(outDir, 'choices.csv'), COLUMNS.choices, choices);

  // --- bill versions (with full text) --------------------------------------
  // Existing versions are copied from the baseline as-is; the file is large, so stream it.
  const out = new CsvWriter(join(outDir, 'bill_versions.csv'), COLUMNS.bill_versions);
  const haveVersion = new Set<string>();
  const billsWithVersions = new Set<string>();
  if (baseline) {
    for await (const r of readCsv(join(baseline, 'bill_versions.csv'))) {
      haveVersion.add(r.BillVersionId!);
      billsWithVersions.add(r.BillId!);
      await out.write(r);
    }
  }
  const toCheck = bills.filter(
    (b) =>
      b.LegislatureNumber >= FIRST_VERSION_LEGISLATURE &&
      (available.has(b.LegislatureNumber) || (opts.recheckMissingVersions && !billsWithVersions.has(b.BillId))),
  );
  log.info(`Checking ${toCheck.length} bills for new versions`);
  let added = 0;
  await mapLimit(toCheck, 25, async (b) => {
    const detail = await client.bills({
      session: b.LegislatureNumber,
      query: { bills: { bill: b.BillNumber }, versions: { fulltext: 'urlonly' } },
    });
    const found: BillVersionRow[] = [];
    for (const bill of detail as Bill[]) {
      for (const v of bill.Versions ?? []) {
        const row = cleanBillVersion(b.LegislatureNumber, bill.BillNumber, v, null);
        if (haveVersion.has(row.BillVersionId) || found.some((f) => f.BillVersionId === row.BillVersionId)) continue;
        if (!opts.skipText) {
          try {
            row.BillVersionFullText = await client.billText(b.LegislatureNumber, b.BillNumber, v.VersionLetter);
          } catch (e) {
            warn(`No text for ${row.BillVersionId}: ${String(e)}`);
          }
        }
        found.push(row);
        await out.write({ ...row });
        added++;
      }
    }
  });
  await out.close();
  log.info(`Added ${added} new bill versions`);
  rows.bill_versions = out.rows;

  return { refreshed: refresh, rows, warnings };
}

/** Missing legislatures, plus the latest one we have and the current one (which may have changed). */
export function legislaturesToRefresh(have: Set<number>, current: number): number[] {
  const latest = have.size ? Math.max(...have) : undefined;
  return range(FIRST_BILL_LEGISLATURE, current).filter((l) => !have.has(l) || l === latest || l === current);
}

/** Every roll-call choice cast in one legislature, fetched member by member. */
async function scrapeVotes(
  client: BasisClient,
  leg: number,
  members: MemberRow[],
  warn: (m: string) => void,
): Promise<Array<{ leg: number; vote: Vote }>> {
  const codes = dedupe(members.filter((m) => m.LegislatureNumber === leg).flatMap(memberCodes));
  const results = await mapLimit(codes, 20, async (code) => {
    try {
      const ms = await client.members({ session: leg, query: { members: { code }, votes: true } });
      return ms.flatMap((m) => m.Votes ?? []).map((vote) => ({ leg, vote }));
    } catch (e) {
      if (e instanceof BasisError && KNOWN_FAILING_MEMBERS.has(`${leg}:${code}`)) {
        warn(`Skipping votes for ${leg}:${code} (known server error)`);
        return [];
      }
      throw e;
    }
  });
  return results.flat();
}

async function unlessUnavailable<T>(f: () => Promise<T>): Promise<T | undefined> {
  try {
    return await f();
  } catch (e) {
    if (e instanceof BasisError && e.isDataUnavailable) return undefined;
    throw e;
  }
}

/** Like Promise.all(items.map(f)), with at most `limit` pending at once. Order is preserved. */
export async function mapLimit<T, R>(items: T[], limit: number, f: (x: T) => Promise<R>): Promise<R[]> {
  const out = new Array<R>(items.length);
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

function range(lo: number, hi: number): number[] {
  const out: number[] = [];
  for (let i = lo; i <= hi; i++) out.push(i);
  return out;
}

function dedupe<T>(xs: T[]): T[] {
  return [...new Set(xs)];
}
