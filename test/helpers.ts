import { readFileSync } from 'node:fs';
import type { FetchFn, FetchInit, FetchResponse } from '../src/client.ts';

export function fixture(name: string): string {
  return readFileSync(new URL(`./fixtures/${name}`, import.meta.url), 'utf8');
}

export function response(body: string, status = 200, headers: Record<string, string> = {}): FetchResponse {
  const lower = Object.fromEntries(Object.entries(headers).map(([k, v]) => [k.toLowerCase(), v]));
  return { status, headers: { get: (n) => lower[n.toLowerCase()] ?? null }, text: async () => body };
}

export interface Call {
  url: string;
  init: FetchInit;
}

/** A fake fetch that answers with `handler` and records every call. */
export function fakeFetch(handler: (url: string, init: FetchInit, n: number) => FetchResponse | Promise<FetchResponse>): FetchFn & {
  calls: Call[];
} {
  const calls: Call[] = [];
  const f = (async (url: string, init: FetchInit) => {
    calls.push({ url, init });
    return handler(url, init, calls.length);
  }) as FetchFn & { calls: Call[] };
  f.calls = calls;
  return f;
}

export const noSleep = async () => {};
export const silent = { debug() {}, info() {}, warn() {}, error() {} };
