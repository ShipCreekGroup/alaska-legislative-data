import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, it } from 'node:test';
import { formatCell, readCsvAll, writeCsv } from '../src/build/csv.ts';

describe('csv', () => {
  it('quotes like DuckDB', () => {
    assert.equal(formatCell(null), '');
    assert.equal(formatCell(''), '""');
    assert.equal(formatCell('a,b'), '"a,b"');
    assert.equal(formatCell('say "hi"'), '"say ""hi"""');
    assert.equal(formatCell('two\nlines'), '"two\nlines"');
    assert.equal(formatCell(true), 'true');
    assert.equal(formatCell(['A', 'B & C']), '"[A, B & C]"');
    assert.equal(formatCell([]), '[]');
  });

  it('round-trips awkward values', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'akleg-'));
    const path = join(dir, 't.csv');
    const rows = [
      { a: 'plain', b: null, c: '' },
      { a: 'quote " and, comma', b: 'multi\r\nline\n', c: '\x16\x10control\x11 \x17' },
      { a: 'ñ unicode', b: '"', c: ',' },
    ];
    await writeCsv(path, ['a', 'b', 'c'], rows);
    assert.deepEqual(await readCsvAll(path), rows);
    writeFileSync(path, 'x,y\r\n1,"a""b"\r\n2,\r\n');
    assert.deepEqual(await readCsvAll(path), [{ x: '1', y: 'a"b' }, { x: '2', y: null }]);
    assert.ok(readFileSync(path, 'utf8').length > 0);
  });
});
