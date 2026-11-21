// Reads the JSON documents written by scripts/extract.ts.
// Locally they live in DATA_DIR (default ./.data). On Cloudflare they live in the R2 bucket bound as
// SITE_DATA, under the prefix site/latest/ (see wrangler.jsonc).
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';

export type Kind = 'legislatures' | 'bills' | 'bill-versions' | 'votes' | 'members' | 'people';

const cache = new Map<string, unknown>();
const MAX = 300;

type R2 = { get(k: string): Promise<{ text(): Promise<string> } | null> };
let r2: R2 | null | undefined;

/** On Cloudflare Workers, the R2 bucket bound as SITE_DATA in wrangler.jsonc. */
async function bucket(): Promise<R2 | null> {
  if (r2 !== undefined) return r2;
  r2 = null;
  if (typeof navigator !== 'undefined' && navigator.userAgent === 'Cloudflare-Workers') {
    const mod = 'cloudflare:workers';
    const { env } = (await import(/* @vite-ignore */ mod)) as { env: { SITE_DATA?: R2 } };
    r2 = env.SITE_DATA ?? null;
  }
  return r2;
}

async function readRaw(path: string): Promise<string | null> {
  const r2 = await bucket();
  if (r2) {
    const obj = await r2.get('site/latest/' + path);
    return obj ? obj.text() : null;
  }
  const dir = process.env.DATA_DIR ?? join(process.cwd(), '.data');
  try {
    return await readFile(join(dir, path), 'utf8');
  } catch {
    return null;
  }
}

async function readJson<T>(path: string): Promise<T | null> {
  if (cache.has(path)) return cache.get(path) as T;
  const text = await readRaw(path);
  if (text === null) return null;
  const doc = JSON.parse(text) as T;
  cache.set(path, doc);
  if (cache.size > MAX) cache.delete(cache.keys().next().value!);
  return doc;
}

export function getDoc<T>(kind: Kind, id: string | number): Promise<T | null> {
  return readJson<T>(`${kind}/${encodeURIComponent(String(id))}.json`);
}
export function getMeta<T>(): Promise<T | null> {
  return readJson<T>('meta.json');
}
export function getPeopleIndex<T>(): Promise<T | null> {
  return readJson<T>('people/index.json');
}
