// `akleg publish`: copy a built export directory to the store as
// published/<date>/ and published/latest/, and delete dated sets older than
// `keepDays`. The raw store can rebuild any of them, and the history tables
// make old sets unnecessary.
import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import type { Logger } from '../client.ts';
import type { BlobStore } from '../store/blob.ts';

export interface PublishOptions {
  store: BlobStore;
  dir: string;
  /** yyyy-mm-dd. Default today (UTC). */
  date?: string;
  /** Default 30. */
  keepDays?: number;
  logger?: Pick<Logger, 'info'>;
  now?: Date;
}

const TYPES: Record<string, string> = {
  csv: 'text/csv; charset=utf-8',
  parquet: 'application/vnd.apache.parquet',
  duckdb: 'application/octet-stream',
  zip: 'application/zip',
};

export async function publish(opts: PublishOptions): Promise<{ uploaded: string[]; deleted: string[] }> {
  const now = opts.now ?? new Date();
  const date = opts.date ?? now.toISOString().slice(0, 10);
  const keepDays = opts.keepDays ?? 30;
  const log = opts.logger ?? { info: console.error };
  const files = (await readdir(opts.dir, { withFileTypes: true })).filter((f) => f.isFile()).map((f) => f.name).sort();
  const uploaded: string[] = [];
  for (const name of files) {
    const body = await readFile(join(opts.dir, name));
    const type = TYPES[name.split('.').at(-1) ?? ''] ?? 'application/octet-stream';
    for (const prefix of [`published/${date}/`, 'published/latest/']) {
      await opts.store.put(prefix + name, body, type);
      uploaded.push(prefix + name);
    }
    log.info(`Published ${name} (${(body.length / 1e6).toFixed(1)} MB)`);
  }

  const deleted: string[] = [];
  const cutoff = new Date(now.getTime() - keepDays * 86400e3).toISOString().slice(0, 10);
  for (const key of await opts.store.list('published/')) {
    const [, folder, name] = key.split('/');
    const stale = folder === 'latest' ? !files.includes(name!) : /^\d{4}-\d{2}-\d{2}$/.test(folder!) && folder! < cutoff;
    if (stale) {
      await opts.store.delete(key);
      deleted.push(key);
    }
  }
  if (deleted.length) log.info(`Deleted ${deleted.length} old published files`);
  return { uploaded, deleted };
}
