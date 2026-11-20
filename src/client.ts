// A dependency-free client for the Alaska Legislature's BASIS API.
//
// Works anywhere a `fetch`-like function exists (Node 18+, Deno, Bun, browsers,
// Cloudflare Workers). For Google Apps Script, pass `fetch: urlFetchAppFetch()`.
//
// This file must stay self-contained (type-only imports only) so that
// scripts/build-gas.ts can turn it into a single Apps Script file.
import type { Bill, Chamber, Committee, Fault, Meeting, Member, Session } from './types.gen.ts';

export const DEFAULT_BASE_URL = 'https://www.akleg.gov/publicservice/basis';
export const DEFAULT_TEXT_BASE_URL = 'https://www.akleg.gov/basis/Bill/Plaintext';
export const API_VERSION = '1.4';

// ---------------------------------------------------------------------------
// fetch abstraction
// ---------------------------------------------------------------------------

export interface FetchInit {
  method: 'GET' | 'HEAD';
  headers: Record<string, string>;
}

/** The subset of the web `Response` that this client uses. */
export interface FetchResponse {
  status: number;
  headers: { get(name: string): string | null };
  text(): Promise<string>;
}

/** Any function shaped like the web standard `fetch`. */
export type FetchFn = (url: string, init: FetchInit) => Promise<FetchResponse>;

export interface Logger {
  debug(...args: unknown[]): void;
  info(...args: unknown[]): void;
  warn(...args: unknown[]): void;
  error(...args: unknown[]): void;
}

export interface ClientOptions {
  /** Defaults to the global `fetch`. */
  fetch?: FetchFn;
  baseUrl?: string;
  /** Where bill version plain text is served. */
  textBaseUrl?: string;
  /** Maximum requests in flight at once. The Legislature asks that clients be gentle. Default 5. */
  maxConcurrency?: number;
  /** Attempts per request for network errors, 5xx responses and server faults. Default 4. */
  maxAttempts?: number;
  /** Used between retries. Defaults to setTimeout, or no wait where that doesn't exist. */
  sleep?: (ms: number) => Promise<void>;
  /** Defaults to warnings and errors on `console`. */
  logger?: Logger;
  /** Extra headers sent with every request. */
  headers?: Record<string, string>;
}

// ---------------------------------------------------------------------------
// queries
// ---------------------------------------------------------------------------

export type ConstraintValue = string | number | boolean;
/** `true` to just include the section, or an object of constraints (values may use `*` as a wildcard). */
export type Include<K extends string> = true | Partial<Record<K, ConstraintValue>>;

type BillFields = 'name' | 'chamber' | 'bill' | 'onfloor' | 'shorttitle' | 'statustext' | 'statuscode' | 'committeecode' | 'fulltext' | 'subject';
type MemberFields = 'building' | 'chamber' | 'comment' | 'district' | 'party' | 'phone' | 'firstname' | 'lastname' | 'ismajority' | 'code';
type CommitteeFields = 'category' | 'chamber' | 'location' | 'name' | 'title' | 'code';
type MeetingFields = 'date' | 'startdate' | 'enddate' | 'details';
type ActionFields = 'code' | 'text' | 'date' | 'startdate' | 'enddate' | 'page' | 'startpage' | 'endpage';
type FiscalNoteFields = 'chamber' | 'date' | 'startdate' | 'enddate' | 'startpage' | 'endpage';
type JournalFields = 'chamber' | 'date' | 'startdate' | 'enddate' | 'page' | 'startpage' | 'endpage' | 'fulltext';

export interface BillsQuery {
  bills?: Include<BillFields>;
  actions?: Include<ActionFields>;
  sponsors?: Include<MemberFields>;
  subjects?: Include<'text'>;
  /** Use `{ fulltext: 'urlonly' }` to get PDF links without inline content. */
  versions?: Include<'fulltext'>;
  fiscalnotes?: Include<FiscalNoteFields>;
  votes?: Include<'vote' | 'title'>;
}
export interface CommitteesQuery {
  committees?: Include<CommitteeFields>;
  members?: Include<MemberFields>;
  meetings?: Include<MeetingFields>;
  bills?: Include<BillFields>;
}
export interface MembersQuery {
  members?: Include<MemberFields>;
  committees?: Include<CommitteeFields>;
  bills?: Include<BillFields>;
  votes?: Include<'vote' | 'title'>;
}
export interface MeetingsQuery {
  meetings?: Include<MeetingFields>;
  media?: true;
  minutes?: Include<'fulltext'>;
}
export interface SessionsQuery {
  journals?: Include<JournalFields>;
  // Accepted by the server but not in the 2014 docs:
  locations?: true;
  subjects?: true;
  requestors?: true;
  stats?: true;
  housestatus?: true;
  senatestatus?: true;
}

/**
 * Which slice of results to return:
 * - `10` or `'10'`: the first 10
 * - `{ start: 10, end: 20 }` or `'10..20'`: results 10 through 20, inclusive
 * - `{ last: 30 }` or `'..30'`: the last 30
 */
export type Range = number | string | { start: number; end: number } | { last: number };

export interface RequestParams<Q> {
  /** Legislature number, eg 34 for 2025-2026. Defaults to the current one. */
  session?: number;
  chamber?: Chamber;
  /** A query object, or the raw header value(s), eg 'bills;bill=HB 16,sponsors'. */
  query?: Q | string | string[];
  /** Not usable from browsers: the server's CORS policy doesn't allow this header. */
  range?: Range;
}

export type Endpoint = 'bills' | 'committees' | 'meetings' | 'members' | 'sessions';

/** Turn a query object into the X-Alaska-Legislature-Basis-Query header value. */
export function buildQuery(query: object | string | string[] | undefined): string | undefined {
  if (query === undefined) return undefined;
  if (typeof query === 'string') return query || undefined;
  if (Array.isArray(query)) return query.length ? query.join(',') : undefined;
  const parts: string[] = [];
  for (const [include, constraints] of Object.entries(query)) {
    if (constraints === undefined || constraints === false) continue;
    let part = include;
    if (constraints !== true) {
      for (const [k, v] of Object.entries(constraints as Record<string, ConstraintValue>)) {
        if (v === undefined) continue;
        part += `;${k}=${String(v)}`;
      }
    }
    parts.push(part);
  }
  return parts.length ? parts.join(',') : undefined;
}

export function buildRange(range: Range | undefined): string | undefined {
  if (range === undefined) return undefined;
  if (typeof range === 'number') return String(range);
  if (typeof range === 'string') return range;
  if ('last' in range) return `..${range.last}`;
  return `${range.start}..${range.end}`;
}

/** Build the URL and headers for a request, without sending it. */
export function buildRequest(
  endpoint: Endpoint,
  params: RequestParams<object> = {},
  baseUrl: string = DEFAULT_BASE_URL,
): { url: string; headers: Record<string, string> } {
  let url = `${baseUrl.replace(/\/$/, '')}/${endpoint}?minifyresult=false&json=true`;
  if (params.session !== undefined) url += `&session=${encodeURIComponent(String(params.session))}`;
  if (params.chamber !== undefined) url += `&chamber=${encodeURIComponent(params.chamber)}`;
  const headers: Record<string, string> = { 'X-Alaska-Legislature-Basis-Version': API_VERSION };
  const query = buildQuery(params.query);
  if (query) headers['X-Alaska-Legislature-Basis-Query'] = query;
  const range = buildRange(params.range);
  if (range) headers['X-Alaska-Query-ResultRange'] = range;
  return { url, headers };
}

// ---------------------------------------------------------------------------
// errors
// ---------------------------------------------------------------------------

/** The server answered with an error, either an HTTP error status or a BASIS <Error> fault. */
export class BasisError extends Error {
  url: string;
  status: number;
  faults: Fault[];
  body: string;
  constructor(message: string, url: string, status: number, faults: Fault[], body: string) {
    super(message);
    this.name = 'BasisError';
    this.url = url;
    this.status = status;
    this.faults = faults;
    this.body = body;
  }
  /** True when the server says the requested legislature has no data, eg legislatures before the 10th. */
  get isDataUnavailable(): boolean {
    return this.faults.some((f) => /Invalid Session Number/i.test(f.Description));
  }
}

/**
 * Pull Error (or Warning) faults out of a BASIS error body. These come back
 * either as XML (`<Error><Code>..</Code><Description>..</Description></Error>`)
 * or as malformed JSON (`{"Basis":{"Error":{"Code":"..","Description":".."},}}`).
 */
export function parseFaults(body: string, kind: 'Error' | 'Warning' = 'Error'): Fault[] {
  const out: Fault[] = [];
  const xml = new RegExp(`<${kind}\\b[^>]*>([\\s\\S]*?)</${kind}>`, 'g');
  let m: RegExpExecArray | null;
  while ((m = xml.exec(body))) {
    const inner = m[1]!;
    const code = /<Code>([\s\S]*?)<\/Code>/.exec(inner)?.[1] ?? '';
    const description = /<Description>([\s\S]*?)<\/Description>/.exec(inner)?.[1] ?? '';
    out.push({ Code: decodeXml(code.trim()), Description: decodeXml(description.trim()) });
  }
  const json = new RegExp(`"${kind}"\\s*:\\s*\\{\\s*"Code"\\s*:\\s*("(?:[^"\\\\]|\\\\.)*")\\s*,\\s*"Description"\\s*:\\s*("(?:[^"\\\\]|\\\\.)*")`, 'g');
  while ((m = json.exec(body))) {
    out.push({ Code: JSON.parse(m[1]!), Description: JSON.parse(m[2]!) });
  }
  return out;
}

function decodeXml(s: string): string {
  return s
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&amp;/g, '&');
}

// ---------------------------------------------------------------------------
// client
// ---------------------------------------------------------------------------

export interface RawResponse {
  /** The parsed `Basis` object, eg `{ Bills: [...] }`. */
  data: Record<string, unknown>;
  /** The X-Alaska-Query-Count header, if sent. */
  count: number | null;
  url: string;
  headers: Record<string, string>;
}

const consoleLogger: Logger = {
  debug() {},
  info() {},
  warn: (...a) => (typeof console !== 'undefined' ? console.warn(...a) : undefined),
  error: (...a) => (typeof console !== 'undefined' ? console.error(...a) : undefined),
};

function defaultSleep(ms: number): Promise<void> {
  const st = (globalThis as { setTimeout?: (f: () => void, ms: number) => unknown }).setTimeout;
  return new Promise((resolve) => (st ? st(resolve, ms) : resolve()));
}

function defaultFetch(): FetchFn {
  const f = (globalThis as { fetch?: unknown }).fetch;
  if (typeof f !== 'function') {
    return () =>
      Promise.reject(
        new Error('No global fetch found. Pass `fetch` in the client options (eg `urlFetchAppFetch()` in Google Apps Script).'),
      );
  }
  return (url, init) => (f as FetchFn)(url, init);
}

export class BasisClient {
  readonly baseUrl: string;
  readonly textBaseUrl: string;
  readonly maxAttempts: number;
  readonly maxConcurrency: number;
  private fetchFn: FetchFn;
  private sleep: (ms: number) => Promise<void>;
  private logger: Logger;
  private extraHeaders: Record<string, string>;
  private active = 0;
  private waiting: Array<() => void> = [];

  constructor(options: ClientOptions = {}) {
    this.baseUrl = (options.baseUrl ?? DEFAULT_BASE_URL).replace(/\/$/, '');
    this.textBaseUrl = (options.textBaseUrl ?? DEFAULT_TEXT_BASE_URL).replace(/\/$/, '');
    this.fetchFn = options.fetch ?? defaultFetch();
    this.maxConcurrency = options.maxConcurrency ?? 5;
    this.maxAttempts = options.maxAttempts ?? 4;
    this.sleep = options.sleep ?? defaultSleep;
    this.logger = options.logger ?? consoleLogger;
    this.extraHeaders = options.headers ?? {};
  }

  /** Bills. Use `query` to add sponsors, versions, votes, etc. */
  async bills(params: RequestParams<BillsQuery> = {}): Promise<Bill[]> {
    return (await this.get('bills', params)).data.Bills as Bill[];
  }

  async committees(params: RequestParams<CommitteesQuery> = {}): Promise<Committee[]> {
    return (await this.get('committees', params)).data.Committees as Committee[];
  }

  async meetings(params: RequestParams<MeetingsQuery> = {}): Promise<Meeting[]> {
    return (await this.get('meetings', params)).data.Meetings as Meeting[];
  }

  /** Legislators. Add `query: { votes: true }` to get every roll-call choice they made. */
  async members(params: RequestParams<MembersQuery> = {}): Promise<Member[]> {
    return (await this.get('members', params)).data.Members as Member[];
  }

  /** One legislature's details (the API calls it a session). */
  async session(params: RequestParams<SessionsQuery> = {}): Promise<Session> {
    return (await this.get('sessions', params)).data.Session as Session;
  }

  /** Number of results a query would return, without downloading them. */
  async count(endpoint: Endpoint, params: RequestParams<object> = {}): Promise<number> {
    const { url, headers } = buildRequest(endpoint, params, this.baseUrl);
    return this.withRetries(url, async () => {
      const res = await this.send(url, { method: 'HEAD', headers: { ...this.extraHeaders, ...headers } });
      if (res.status >= 400) throw new BasisError(`HTTP ${res.status} from ${url}`, url, res.status, [], '');
      const n = res.headers.get('x-alaska-query-count');
      if (n === null || Number.isNaN(Number(n))) {
        throw new BasisError('Response had no X-Alaska-Query-Count header', url, res.status, [], '');
      }
      return Number(n);
    });
  }

  /** Send a GET request and return the parsed `Basis` object plus metadata. */
  async get(endpoint: Endpoint, params: RequestParams<object> = {}): Promise<RawResponse> {
    const { url, headers } = buildRequest(endpoint, params, this.baseUrl);
    return this.withRetries(url, async () => {
      const res = await this.send(url, { method: 'GET', headers: { ...this.extraHeaders, ...headers } });
      const body = await res.text();
      if (res.status >= 400) {
        throw new BasisError(`HTTP ${res.status} from ${url}`, url, res.status, parseFaults(body), body);
      }
      let parsed: { Basis?: Record<string, unknown> };
      try {
        parsed = JSON.parse(body);
      } catch {
        // Faults come back as XML, even with json=true.
        const faults = parseFaults(body);
        const msg = faults.length
          ? faults.map((f) => `${f.Code}: ${f.Description}`).join('; ')
          : `Response was not JSON: ${body.slice(0, 200)}`;
        throw new BasisError(msg, url, res.status, faults, body);
      }
      if (!parsed || typeof parsed.Basis !== 'object' || parsed.Basis === null) {
        throw new BasisError('Response had no Basis object', url, res.status, [], body);
      }
      const err = (parsed.Basis as { Error?: Fault | Fault[] }).Error;
      if (err) {
        const faults = Array.isArray(err) ? err : [err];
        throw new BasisError(faults.map((f) => `${f.Code}: ${f.Description}`).join('; '), url, res.status, faults, body);
      }
      const count = res.headers.get('x-alaska-query-count');
      return { data: parsed.Basis, count: count === null ? null : Number(count), url, headers };
    });
  }

  /**
   * The plain text of one bill version, with line numbers removed.
   * Control characters marking added (\x16\x10 ... \x11\x17) and other sections are kept.
   * @param legislature eg 34
   * @param bill eg 'HB 16' (any spacing)
   * @param version eg 'A'
   */
  async billText(legislature: number, bill: string, version: string): Promise<string> {
    return cleanBillText(await this.billTextRaw(legislature, bill, version));
  }

  /** The plain text of one bill version exactly as served. */
  async billTextRaw(legislature: number, bill: string, version: string): Promise<string> {
    const url = `${this.textBaseUrl}/${legislature}?Hsid=${textBillCode(bill)}${version}`;
    return this.withRetries(url, async () => {
      const res = await this.send(url, { method: 'GET', headers: { ...this.extraHeaders } });
      const body = await res.text();
      if (res.status >= 400) throw new BasisError(`HTTP ${res.status} from ${url}`, url, res.status, [], body);
      return body;
    });
  }

  private async withRetries<T>(url: string, attempt: () => Promise<T>): Promise<T> {
    let lastError: unknown;
    for (let i = 0; i < this.maxAttempts; i++) {
      try {
        return await attempt();
      } catch (e) {
        lastError = e;
        if (!isRetryable(e) || i === this.maxAttempts - 1) break;
        const wait = 500 * 2 ** i;
        this.logger.warn(`Retrying ${url} in ${wait}ms (attempt ${i + 2}/${this.maxAttempts}): ${String(e)}`);
        await this.sleep(wait);
      }
    }
    throw lastError;
  }

  private async send(url: string, init: FetchInit): Promise<FetchResponse> {
    await this.acquire();
    try {
      this.logger.debug(init.method, url, init.headers);
      return await this.fetchFn(url, init);
    } finally {
      this.release();
    }
  }

  private acquire(): Promise<void> {
    if (this.active < this.maxConcurrency) {
      this.active++;
      return Promise.resolve();
    }
    return new Promise((resolve) => this.waiting.push(resolve));
  }

  private release(): void {
    const next = this.waiting.shift();
    if (next) next();
    else this.active--;
  }
}

function isRetryable(e: unknown): boolean {
  if (e instanceof BasisError) {
    if (e.isDataUnavailable) return false;
    if (e.status >= 500 || e.status === 429) return true;
    // Under load the server sometimes answers a valid query with one of these; a retry usually works.
    return e.faults.some((f) => f.Code === 'FaultException' || f.Code === 'XmlSchemaValidationException');
  }
  return true; // network errors
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/** 'HB   16' -> 'HB 16' */
export function normalizeBillNumber(bill: string): string {
  return bill.trim().replace(/\s+/g, ' ');
}

/** 'HB 16' -> 'HB0016', the form used in bill text and PDF URLs. */
export function textBillCode(bill: string): string {
  const m = /([A-Z]{2,4})\s*0*(\d+)/.exec(bill.trim().toUpperCase());
  if (!m) throw new Error(`Can't parse bill number: ${bill}`);
  return `${m[1]}${m[2]!.padStart(4, '0')}`;
}

/** '/Date(1737450000000)/' -> Date */
export function parseMsDate(s: string): Date {
  const m = /^\/Date\((-?\d+)\)\/$/.exec(s);
  if (!m) throw new Error(`Not a /Date(...)/ value: ${s}`);
  return new Date(Number(m[1]));
}

/** Remove the 3-character line-number prefix and blank lines from bill plain text. */
export function cleanBillText(raw: string): string {
  // Same line breaks as Python's str.splitlines(), which the original scraper used.
  return raw
    .split(/\r\n|[\n\r\v\f\x1c\x1d\x1e\x85\u2028\u2029]/)
    .filter((line) => line !== '')
    .map((line) => line.slice(3))
    .join('\n');
}

/** Rough current legislature number: 34 for 2025-2026, 35 for 2027-2028, ... */
export function currentLegislature(date: Date = new Date()): number {
  return Math.floor((date.getFullYear() - 2023) / 2) + 33;
}

// ---------------------------------------------------------------------------
// Google Apps Script
// ---------------------------------------------------------------------------

interface UrlFetchAppLike {
  fetch(
    url: string,
    params: { method: string; headers: Record<string, string>; muteHttpExceptions: boolean; followRedirects: boolean },
  ): {
    getResponseCode(): number;
    getContentText(): string;
    getAllHeaders(): Record<string, string | string[]>;
  };
}

/**
 * A FetchFn backed by Apps Script's UrlFetchApp. UrlFetchApp can't send HEAD
 * requests, so `count()` sends a GET instead (and downloads the results).
 */
export function urlFetchAppFetch(urlFetchApp?: UrlFetchAppLike): FetchFn {
  return async (url, init) => {
    const app = urlFetchApp ?? (globalThis as { UrlFetchApp?: UrlFetchAppLike }).UrlFetchApp;
    if (!app) throw new Error('UrlFetchApp is not available');
    const res = app.fetch(url, { method: 'get', headers: init.headers, muteHttpExceptions: true, followRedirects: true });
    const all = res.getAllHeaders();
    const lower: Record<string, string> = {};
    for (const k of Object.keys(all)) {
      const v = all[k]!;
      lower[k.toLowerCase()] = Array.isArray(v) ? v.join(', ') : v;
    }
    return {
      status: res.getResponseCode(),
      headers: { get: (name: string) => lower[name.toLowerCase()] ?? null },
      text: async () => res.getContentText(),
    };
  };
}
