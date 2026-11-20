// Key-value storage for the raw store and published files: a local directory
// (for development and tests) or a Cloudflare R2 bucket, through R2's
// S3-compatible API signed with AWS Signature Version 4. No SDK needed.
import { createHash, createHmac } from 'node:crypto';
import { mkdir, readdir, readFile, rm, writeFile } from 'node:fs/promises';
import { dirname, join, relative, sep } from 'node:path';

export interface BlobStore {
  /** The object's bytes, or null if there is none. */
  get(key: string): Promise<Uint8Array | null>;
  put(key: string, body: Uint8Array, contentType?: string): Promise<void>;
  /** Every key starting with `prefix`, sorted. */
  list(prefix: string): Promise<string[]>;
  delete(key: string): Promise<void>;
  /** For log messages, eg 'r2://bucket' or a path. */
  readonly name: string;
}

// ---------------------------------------------------------------------------
// local directory
// ---------------------------------------------------------------------------

export class FileStore implements BlobStore {
  readonly name: string;
  readonly dir: string;
  constructor(dir: string) {
    this.dir = dir;
    this.name = dir;
  }

  private path(key: string): string {
    if (key.split('/').some((p) => p === '..' || p === '')) throw new Error(`Bad key ${key}`);
    return join(this.dir, ...key.split('/'));
  }

  async get(key: string): Promise<Uint8Array | null> {
    try {
      return await readFile(this.path(key));
    } catch (e) {
      if ((e as NodeJS.ErrnoException).code === 'ENOENT') return null;
      throw e;
    }
  }

  async put(key: string, body: Uint8Array): Promise<void> {
    const p = this.path(key);
    await mkdir(dirname(p), { recursive: true });
    await writeFile(p, body);
  }

  async list(prefix: string): Promise<string[]> {
    const out: string[] = [];
    const walk = async (dir: string): Promise<void> => {
      let entries;
      try {
        entries = await readdir(dir, { withFileTypes: true });
      } catch (e) {
        if ((e as NodeJS.ErrnoException).code === 'ENOENT') return;
        throw e;
      }
      for (const e of entries) {
        const p = join(dir, e.name);
        if (e.isDirectory()) await walk(p);
        else {
          const key = relative(this.dir, p).split(sep).join('/');
          if (key.startsWith(prefix)) out.push(key);
        }
      }
    };
    await walk(this.dir);
    return out.sort();
  }

  async delete(key: string): Promise<void> {
    await rm(this.path(key), { force: true });
  }
}

// ---------------------------------------------------------------------------
// Cloudflare R2
// ---------------------------------------------------------------------------

export interface R2Options {
  accountId: string;
  accessKeyId: string;
  secretAccessKey: string;
  bucket: string;
  /** Defaults to https://<accountId>.r2.cloudflarestorage.com */
  endpoint?: string;
  fetch?: typeof fetch;
  now?: () => Date;
}

export class R2Store implements BlobStore {
  readonly name: string;
  private endpoint: URL;
  private fetchFn: typeof fetch;
  private now: () => Date;
  private opts: R2Options;

  constructor(opts: R2Options) {
    this.opts = opts;
    this.name = `r2://${opts.bucket}`;
    this.endpoint = new URL(opts.endpoint ?? `https://${opts.accountId}.r2.cloudflarestorage.com`);
    this.fetchFn = opts.fetch ?? fetch;
    this.now = opts.now ?? (() => new Date());
  }

  /** Reads R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY and R2_BUCKET. */
  static fromEnv(env: Record<string, string | undefined> = process.env): R2Store {
    const need = ['R2_ACCOUNT_ID', 'R2_ACCESS_KEY_ID', 'R2_SECRET_ACCESS_KEY', 'R2_BUCKET'];
    const missing = need.filter((k) => !env[k]);
    if (missing.length) throw new Error(`Set ${missing.join(', ')} to use R2`);
    return new R2Store({
      accountId: env.R2_ACCOUNT_ID!,
      accessKeyId: env.R2_ACCESS_KEY_ID!,
      secretAccessKey: env.R2_SECRET_ACCESS_KEY!,
      bucket: env.R2_BUCKET!,
    });
  }

  private async send(method: string, key: string, query: Record<string, string> = {}, body?: Uint8Array, contentType?: string) {
    const path = `/${this.opts.bucket}/${key.split('/').map(encodeRfc3986).join('/')}`;
    const url = new URL(path, this.endpoint);
    for (const [k, v] of Object.entries(query)) url.searchParams.set(k, v);
    const headers: Record<string, string> = {};
    if (contentType) headers['content-type'] = contentType;
    const signed = signV4({
      method,
      url,
      headers,
      body: body ?? new Uint8Array(),
      accessKeyId: this.opts.accessKeyId,
      secretAccessKey: this.opts.secretAccessKey,
      region: 'auto',
      service: 's3',
      date: this.now(),
    });
    for (let attempt = 1; ; attempt++) {
      let res: Response;
      try {
        res = await this.fetchFn(url, { method, headers: signed, body: body as Uint8Array<ArrayBuffer> | undefined });
      } catch (e) {
        if (attempt >= 4) throw e;
        await sleep(500 * 2 ** attempt);
        continue;
      }
      if (res.status >= 500 && attempt < 4) {
        await sleep(500 * 2 ** attempt);
        continue;
      }
      return res;
    }
  }

  async get(key: string): Promise<Uint8Array | null> {
    const res = await this.send('GET', key);
    if (res.status === 404) return null;
    if (!res.ok) throw new Error(`R2 GET ${key}: HTTP ${res.status} ${await res.text()}`);
    return new Uint8Array(await res.arrayBuffer());
  }

  async put(key: string, body: Uint8Array, contentType = 'application/octet-stream'): Promise<void> {
    const res = await this.send('PUT', key, {}, body, contentType);
    if (!res.ok) throw new Error(`R2 PUT ${key}: HTTP ${res.status} ${await res.text()}`);
  }

  async list(prefix: string): Promise<string[]> {
    const out: string[] = [];
    let token: string | undefined;
    do {
      const query: Record<string, string> = { 'list-type': '2', prefix };
      if (token) query['continuation-token'] = token;
      const res = await this.send('GET', '', query);
      const xml = await res.text();
      if (!res.ok) throw new Error(`R2 list ${prefix}: HTTP ${res.status} ${xml}`);
      for (const m of xml.matchAll(/<Key>([\s\S]*?)<\/Key>/g)) out.push(decodeXml(m[1]!));
      token = /<IsTruncated>true<\/IsTruncated>/.test(xml) ? decodeXml(/<NextContinuationToken>([\s\S]*?)<\/NextContinuationToken>/.exec(xml)?.[1] ?? '') : undefined;
    } while (token);
    return out.sort();
  }

  async delete(key: string): Promise<void> {
    const res = await this.send('DELETE', key);
    if (!res.ok && res.status !== 404) throw new Error(`R2 DELETE ${key}: HTTP ${res.status} ${await res.text()}`);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function decodeXml(s: string): string {
  return s.replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&apos;/g, "'").replace(/&amp;/g, '&');
}

function encodeRfc3986(s: string): string {
  return encodeURIComponent(s).replace(/[!'()*]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
}

export interface SignParams {
  method: string;
  url: URL;
  /** Extra headers to sign (lowercase names). */
  headers: Record<string, string>;
  body: Uint8Array;
  accessKeyId: string;
  secretAccessKey: string;
  region: string;
  service: string;
  date: Date;
}

/** AWS Signature Version 4. Returns all headers to send, including Authorization. */
export function signV4(p: SignParams): Record<string, string> {
  const amzDate = p.date.toISOString().replace(/[-:]/g, '').replace(/\.\d{3}/, '');
  const day = amzDate.slice(0, 8);
  const payloadHash = sha256Hex(p.body);
  const headers: Record<string, string> = {
    ...Object.fromEntries(Object.entries(p.headers).map(([k, v]) => [k.toLowerCase(), v.trim()])),
    host: p.url.host,
    'x-amz-content-sha256': payloadHash,
    'x-amz-date': amzDate,
  };
  const names = Object.keys(headers).sort();
  const query = [...p.url.searchParams.entries()]
    .map(([k, v]) => [encodeRfc3986(k), encodeRfc3986(v)] as const)
    .sort(([a, x], [b, y]) => (a < b ? -1 : a > b ? 1 : x < y ? -1 : x > y ? 1 : 0))
    .map(([k, v]) => `${k}=${v}`)
    .join('&');
  const canonical = [
    p.method,
    p.url.pathname,
    query,
    ...names.map((n) => `${n}:${headers[n]}`),
    '',
    names.join(';'),
    payloadHash,
  ].join('\n');
  const scope = `${day}/${p.region}/${p.service}/aws4_request`;
  const toSign = ['AWS4-HMAC-SHA256', amzDate, scope, sha256Hex(canonical)].join('\n');
  let key: Buffer = hmac(`AWS4${p.secretAccessKey}`, day);
  for (const part of [p.region, p.service, 'aws4_request']) key = hmac(key, part);
  const signature = createHmac('sha256', key).update(toSign).digest('hex');
  headers.authorization = `AWS4-HMAC-SHA256 Credential=${p.accessKeyId}/${scope}, SignedHeaders=${names.join(';')}, Signature=${signature}`;
  delete headers.host; // fetch sets it
  return headers;
}

function hmac(key: string | Buffer, data: string): Buffer {
  return createHmac('sha256', key).update(data).digest();
}

export function sha256Hex(data: string | Uint8Array): string {
  return createHash('sha256').update(data).digest('hex');
}
