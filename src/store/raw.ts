// The raw store: every API response we fetch, kept forever.
//
//   raw/objects/<sha256>.gz          one response body, gzipped, named by the hash of its content
//   raw/log/<YYYY-MM-DD>.ndjson.gz   one line per request made that day: time, URL, query, range, status, body hash
//
// Objects are written only when their content is new, so an unchanged
// response only adds a log line. Nothing is ever edited or deleted.
//
// `recordingFetch` records live requests; `replayFetch` answers the same
// requests from the store as of a date, so the tables layer can rebuild any
// day's tables without the network.
import { existsSync } from 'node:fs';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { gunzipSync, gzipSync } from 'node:zlib';
import type { FetchFn, FetchInit, FetchResponse } from '../client.ts';
import { sha256Hex, type BlobStore } from './blob.ts';

export interface LogEntry {
  /** The fetch run's date, yyyy-mm-dd. Groups requests into one day's snapshot. */
  date: string;
  /** When the response arrived, ISO 8601. */
  time: string;
  url: string;
  query: string | null;
  range: string | null;
  status: number;
  /** sha256 of the body; the object is raw/objects/<hash>.gz. */
  hash: string;
  /** The legislature the request is about, from the URL; null if none. */
  legislature: number | null;
}

/** What identifies a request: URL plus the two headers that change the answer. */
export function requestKey(e: Pick<LogEntry, 'url' | 'query' | 'range'>): string {
  return `${e.url}\n${e.query ?? ''}\n${e.range ?? ''}`;
}

export function legislatureOfUrl(url: string): number | null {
  const m = /[?&]session=(\d+)/.exec(url) ?? /\/Plaintext\/(\d+)\?/.exec(url);
  return m ? Number(m[1]) : null;
}

export function isTextUrl(url: string): boolean {
  return url.includes('/Plaintext/');
}

export interface RawStoreOptions {
  /** Local directory to keep copies of objects in. Objects never change, so this is always safe to reuse. */
  cacheDir?: string;
  /** Bodies kept in memory between reads, in characters. Default 400 million. */
  memoryBudget?: number;
}

export class RawStore {
  readonly blobs: BlobStore;
  private cacheDir: string | undefined;
  private known = new Set<string>();
  private memo = new Map<string, string>();
  private memoSize = 0;
  private memoryBudget: number;

  constructor(blobs: BlobStore, options: RawStoreOptions = {}) {
    this.blobs = blobs;
    this.cacheDir = options.cacheDir;
    this.memoryBudget = options.memoryBudget ?? 400e6;
  }

  static objectKey(hash: string): string {
    return `raw/objects/${hash}.gz`;
  }

  static logKey(date: string): string {
    return `raw/log/${date}.ndjson.gz`;
  }

  /** Every log entry, oldest first. */
  async loadLog(): Promise<LogEntry[]> {
    const keys = (await this.blobs.list('raw/log/')).filter((k) => k.endsWith('.ndjson.gz'));
    const out: LogEntry[] = [];
    for (const key of keys) out.push(...(await this.readLog(key)));
    for (const e of out) if (e.status < 400) this.known.add(e.hash);
    return out.sort((a, b) => (a.time < b.time ? -1 : a.time > b.time ? 1 : 0));
  }

  private async readLog(key: string): Promise<LogEntry[]> {
    const gz = await this.blobs.get(key);
    if (!gz) return [];
    return gunzipSync(gz)
      .toString('utf8')
      .split('\n')
      .filter(Boolean)
      .map((l) => JSON.parse(l) as LogEntry);
  }

  /** Add entries to a day's log (appending to any already there). */
  async appendLog(date: string, entries: LogEntry[]): Promise<void> {
    if (!entries.length) return;
    const key = RawStore.logKey(date);
    const all = [...(await this.readLog(key)), ...entries];
    await this.blobs.put(key, gzipSync(all.map((e) => JSON.stringify(e)).join('\n') + '\n'), 'application/gzip');
  }

  /** Store a body if it's new. Returns its hash. */
  async putBody(body: string): Promise<string> {
    const hash = sha256Hex(body);
    if (!this.known.has(hash)) {
      const gz = gzipSync(body);
      await this.blobs.put(RawStore.objectKey(hash), gz, 'application/gzip');
      await this.writeCache(hash, gz);
      this.known.add(hash);
    }
    return hash;
  }

  async getBody(hash: string): Promise<string> {
    const hit = this.memo.get(hash);
    if (hit !== undefined) return hit;
    let gz = await this.readCache(hash);
    if (!gz) {
      gz = await this.blobs.get(RawStore.objectKey(hash));
      if (!gz) throw new Error(`Raw store is missing object ${hash}`);
      await this.writeCache(hash, gz);
    }
    const body = gunzipSync(gz).toString('utf8');
    if (sha256Hex(body) !== hash) throw new Error(`Raw object ${hash} doesn't match its hash`);
    if (this.memoSize + body.length <= this.memoryBudget) {
      this.memo.set(hash, body);
      this.memoSize += body.length;
    }
    return body;
  }

  private async readCache(hash: string): Promise<Uint8Array | null> {
    if (!this.cacheDir) return null;
    const p = join(this.cacheDir, `${hash}.gz`);
    return existsSync(p) ? readFile(p) : null;
  }

  private async writeCache(hash: string, gz: Uint8Array): Promise<void> {
    if (!this.cacheDir) return;
    await mkdir(this.cacheDir, { recursive: true });
    await writeFile(join(this.cacheDir, `${hash}.gz`), gz);
  }

  /**
   * Wraps a fetch so every GET response below 500 is stored, and its log entry
   * pushed to `sink`. Server errors aren't recorded: the client retries them.
   */
  recordingFetch(inner: FetchFn, sink: LogEntry[], date: string): FetchFn {
    return async (url, init) => {
      const res = await inner(url, init);
      if (init.method !== 'GET' || res.status >= 500) return res;
      const body = await res.text();
      const hash = await this.putBody(body);
      sink.push({
        date,
        time: new Date().toISOString(),
        url,
        query: header(init, 'X-Alaska-Legislature-Basis-Query'),
        range: header(init, 'X-Alaska-Query-ResultRange'),
        status: res.status,
        hash,
        legislature: legislatureOfUrl(url),
      });
      return { status: res.status, headers: res.headers, text: async () => body };
    };
  }
}

function header(init: FetchInit, name: string): string | null {
  const lower = name.toLowerCase();
  for (const [k, v] of Object.entries(init.headers)) if (k.toLowerCase() === lower) return v;
  return null;
}

/** Thrown by a replay fetch for a request that was never recorded. */
export class NotInStoreError extends Error {
  readonly url: string;
  readonly query: string | null;
  constructor(url: string, query: string | null) {
    super(`Not in the raw store: ${url}${query ? ` (query ${query})` : ''}`);
    this.name = 'NotInStoreError';
    this.url = url;
    this.query = query;
  }
}

/** The log, indexed for building tables as of a date. */
export class RawIndex {
  readonly entries: LogEntry[];
  constructor(entries: LogEntry[]) {
    this.entries = entries;
  }

  /** Fetch run dates, oldest first. */
  dates(): string[] {
    return [...new Set(this.entries.map((e) => e.date))].sort();
  }

  /** Legislatures with requests on a date. */
  legislaturesOn(date: string): number[] {
    const legs = new Set<number>();
    for (const e of this.entries) if (e.date === date && e.legislature !== null) legs.add(e.legislature);
    return [...legs].sort((a, b) => a - b);
  }

  /** Legislatures with any requests. */
  legislatures(): number[] {
    return [...new Set(this.entries.flatMap((e) => (e.legislature === null ? [] : [e.legislature])))].sort((a, b) => a - b);
  }

  /** URLs of bill text fetched successfully. */
  textUrls(): Set<string> {
    return new Set(this.entries.filter((e) => isTextUrl(e.url) && e.status === 200).map((e) => e.url));
  }

  /**
   * The response to use for each request as of a date: the latest on or
   * before it, or, for a request first made later (eg after a new column was
   * added), the earliest one. That way a new column is filled into older
   * days instead of appearing as a change.
   */
  view(date: string): Map<string, LogEntry> {
    const out = new Map<string, LogEntry>();
    for (const e of this.entries) {
      const key = requestKey(e);
      const prev = out.get(key);
      if (!prev || e.date <= date) out.set(key, e);
    }
    return out;
  }

  /**
   * A digest of one legislature's responses on one date (bill text aside,
   * which is fetched once per version). Equal digests mean equal tables.
   */
  fingerprint(date: string, legislature: number): string {
    // The last answer to each request that day: a retried request counts once.
    const latest = new Map<string, LogEntry>();
    for (const e of this.entries) {
      if (e.date === date && e.legislature === legislature && !isTextUrl(e.url)) latest.set(requestKey(e), e);
    }
    const lines = [...latest].map(([key, e]) => `${key}\n${e.status}\n${e.hash}`);
    return sha256Hex(lines.sort().join('\n\n'));
  }
}

/** A fetch that answers from the raw store, using one `RawIndex.view()`. */
export function replayFetch(raw: RawStore, view: Map<string, LogEntry>): FetchFn {
  return async (url, init): Promise<FetchResponse> => {
    const query = header(init, 'X-Alaska-Legislature-Basis-Query');
    const entry = view.get(requestKey({ url, query, range: header(init, 'X-Alaska-Query-ResultRange') }));
    if (!entry || init.method !== 'GET') throw new NotInStoreError(url, query);
    const body = await raw.getBody(entry.hash);
    return { status: entry.status, headers: { get: () => null }, text: async () => body };
  };
}
