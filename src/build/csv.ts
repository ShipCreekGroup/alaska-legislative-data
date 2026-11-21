// Minimal streaming CSV reader/writer, compatible with what DuckDB writes:
// NULL is an empty unquoted field, '' is written as "", and fields containing
// a comma, quote, or line break are quoted.
import { createReadStream, createWriteStream, type WriteStream } from 'node:fs';

export type Cell = string | number | boolean | null | undefined | string[];
export type Row = Record<string, string | null>;

/** Read a CSV file with a header row. Unquoted empty fields are null; "" is ''. */
export async function* readCsv(path: string): AsyncGenerator<Row> {
  let header: string[] | null = null;
  for await (const fields of readCsvRecords(path)) {
    if (!header) {
      header = fields.map((f) => f ?? '');
      continue;
    }
    const row: Row = {};
    header.forEach((h, i) => (row[h] = fields[i] ?? null));
    yield row;
  }
}

export async function readCsvAll(path: string): Promise<Row[]> {
  const out: Row[] = [];
  for await (const r of readCsv(path)) out.push(r);
  return out;
}

async function* readCsvRecords(path: string): AsyncGenerator<Array<string | null>> {
  let fields: Array<string | null> = [];
  let field = '';
  let quoted = false; // current field was quoted
  let inQuotes = false;
  let pendingQuote = false; // saw a quote while inQuotes; could be "" or the closing quote
  let started = false; // any char seen in this record
  let skipLf = false;

  const endField = () => {
    fields.push(quoted || field !== '' ? field : null);
    field = '';
    quoted = false;
  };

  for await (const chunk of createReadStream(path, { encoding: 'utf8' })) {
    const s = chunk as string;
    for (let i = 0; i < s.length; i++) {
      const c = s[i]!;
      if (skipLf) {
        skipLf = false;
        if (c === '\n') continue;
      }
      if (pendingQuote) {
        pendingQuote = false;
        if (c === '"') {
          field += '"';
          continue;
        }
        inQuotes = false;
      }
      if (inQuotes) {
        if (c === '"') {
          pendingQuote = true;
        } else {
          // Fast path: copy everything up to the next quote at once.
          const j = s.indexOf('"', i);
          const end = j === -1 ? s.length : j;
          field += s.slice(i, end);
          i = end - 1;
        }
        continue;
      }
      started = true;
      if (c === '"') {
        inQuotes = true;
        quoted = true;
      } else if (c === ',') {
        endField();
      } else if (c === '\n' || c === '\r') {
        endField();
        yield fields;
        fields = [];
        started = false;
        if (c === '\r') skipLf = true;
      } else {
        field += c;
      }
    }
  }
  if (started || field !== '' || fields.length) {
    endField();
    yield fields;
  }
}

export function formatCell(v: Cell): string {
  if (v === null || v === undefined) return '';
  let s: string;
  if (Array.isArray(v)) s = `[${v.join(', ')}]`;
  else s = String(v);
  if (s === '' || /[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

/** Writes rows to a CSV file with a fixed column order, respecting backpressure. */
export class CsvWriter {
  private stream: WriteStream;
  readonly columns: readonly string[];
  rows = 0;

  constructor(path: string, columns: readonly string[]) {
    this.columns = columns;
    this.stream = createWriteStream(path, { encoding: 'utf8' });
    this.stream.write(columns.join(',') + '\n');
  }

  async write(row: object): Promise<void> {
    const r = row as Record<string, Cell>;
    const line = this.columns.map((c) => formatCell(r[c])).join(',') + '\n';
    this.rows++;
    if (!this.stream.write(line)) {
      await new Promise<void>((resolve) => this.stream.once('drain', () => resolve()));
    }
  }

  close(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.stream.end((err?: Error | null) => (err ? reject(err) : resolve()));
    });
  }
}

export async function writeCsv(path: string, columns: readonly string[], rows: Iterable<object>): Promise<number> {
  const w = new CsvWriter(path, columns);
  for (const r of rows) await w.write(r);
  await w.close();
  return w.rows;
}
