// Slowly changing dimension (type 2) history for one table: every version of
// every row, with the dates it was valid. ValidTo is the first date a version
// was no longer current; empty means it's current.
//
// Versions are kept in a temporary file (bill text is too big to hold in
// memory) and only their keys, hashes and file offsets in memory.
import { closeSync, openSync, readSync, writeSync } from 'node:fs';
import { join } from 'node:path';
import { sha256Hex } from '../store/blob.ts';
import { CsvWriter } from './csv.ts';

interface Version {
  key: string;
  validFrom: string;
  validTo: string | null;
  offset: number;
  length: number;
}

export class HistoryTable {
  readonly name: string;
  readonly columns: readonly string[];
  private keyOf: (row: object) => string;
  private legislatureOf: (key: string) => number | null;
  private fd: number;
  private size = 0;
  private versions: Version[] = [];
  /** Current version of each key: its hash and index in `versions`. */
  private open = new Map<string, { hash: string; index: number }>();

  constructor(
    name: string,
    columns: readonly string[],
    keyOf: (row: object) => string,
    legislatureOf: (key: string) => number | null,
    tmpDir: string,
  ) {
    this.name = name;
    this.columns = columns;
    this.keyOf = keyOf;
    this.legislatureOf = legislatureOf;
    this.fd = openSync(join(tmpDir, `${name}.ndjson`), 'w+');
  }

  /**
   * Compare a fresh build of one legislature's rows (or of the whole table,
   * with `legislature` null) on `date` with the current versions: unchanged
   * rows are left alone, changed rows get a new version, new rows start one,
   * and current rows missing from `rows` are closed.
   */
  apply(date: string, legislature: number | null, rows: Iterable<object>): { added: number; closed: number } {
    let added = 0;
    let closed = 0;
    const seen = new Set<string>();
    for (const row of rows) {
      const key = this.keyOf(row);
      if (seen.has(key)) throw new Error(`${this.name}: ${key} appears twice in one build`);
      seen.add(key);
      const values = this.columns.map((c) => (row as Record<string, unknown>)[c] ?? null);
      const json = JSON.stringify(values);
      const hash = sha256Hex(json);
      const cur = this.open.get(key);
      if (cur?.hash === hash) continue;
      if (cur) this.versions[cur.index]!.validTo = date;
      const buf = Buffer.from(json + '\n');
      writeSync(this.fd, buf, 0, buf.length, this.size);
      this.versions.push({ key, validFrom: date, validTo: null, offset: this.size, length: buf.length - 1 });
      this.size += buf.length;
      this.open.set(key, { hash, index: this.versions.length - 1 });
      added++;
    }
    for (const [key, cur] of this.open) {
      if (seen.has(key)) continue;
      if (legislature !== null && this.legislatureOf(key) !== legislature) continue;
      this.versions[cur.index]!.validTo = date;
      this.open.delete(key);
      closed++;
    }
    return { added, closed };
  }

  /** Write `<name>.csv` (current rows) and `<name>_history.csv` (every version), both sorted by key. */
  async write(outDir: string): Promise<{ current: number; history: number }> {
    const order = this.versions
      .map((_, i) => i)
      .sort((a, b) => {
        const x = this.versions[a]!;
        const y = this.versions[b]!;
        return x.key < y.key ? -1 : x.key > y.key ? 1 : x.validFrom < y.validFrom ? -1 : x.validFrom > y.validFrom ? 1 : 0;
      });
    const current = new CsvWriter(join(outDir, `${this.name}.csv`), this.columns);
    const history = new CsvWriter(join(outDir, `${this.name}_history.csv`), [...this.columns, 'ValidFrom', 'ValidTo']);
    for (const i of order) {
      const v = this.versions[i]!;
      const buf = Buffer.alloc(v.length);
      readSync(this.fd, buf, 0, v.length, v.offset);
      const values = JSON.parse(buf.toString('utf8')) as unknown[];
      const row: Record<string, unknown> = {};
      this.columns.forEach((c, j) => (row[c] = values[j]));
      if (v.validTo === null) await current.write(row);
      await history.write({ ...row, ValidFrom: v.validFrom, ValidTo: v.validTo });
    }
    await current.close();
    await history.close();
    return { current: current.rows, history: history.rows };
  }

  close(): void {
    closeSync(this.fd);
  }
}
