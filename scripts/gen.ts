// Generates everything derived from schema/basis.schema.json:
// - rewrites the schema itself in one standard layout
// - src/types.gen.ts: TypeScript types
// - src/zod.gen.ts: zod/mini validators
// and src/tables/curated.gen.ts, the compact form of data/curated/*.csv.
// Run with `pnpm gen` after editing the schema or the curated CSVs. A test
// fails if any generated file is out of date.
import { readFileSync, writeFileSync } from 'node:fs';
import { compile } from 'json-schema-to-typescript';
import { format as prettier } from 'prettier';
import { readCsvAll } from '../src/build/csv.ts';
import { curatedFromCsv, encodeCurated } from '../src/tables/curated.ts';

const SCHEMA_URL = new URL('../schema/basis.schema.json', import.meta.url);

type Json = null | boolean | number | string | Json[] | { [k: string]: Json };
type Schema = { [k: string]: any };

export function readSchema(): Schema {
  return JSON.parse(readFileSync(SCHEMA_URL, 'utf8'));
}

// ---------------------------------------------------------------------------
// standard layout
// ---------------------------------------------------------------------------

const WIDTH = 120;

/**
 * The schema's standard layout: the root, `$defs`, each def and each
 * `properties` map are always one entry per line; anything else is on one
 * line if it fits in 120 columns. No blank lines.
 */
export function formatSchema(schema: Json): string {
  return format(schema, '', 0, []) + '\n';
}

function inline(v: Json): string {
  if (v === null || typeof v !== 'object') return JSON.stringify(v);
  if (Array.isArray(v)) return v.length ? `[${v.map(inline).join(', ')}]` : '[]';
  const entries = Object.entries(v);
  return entries.length ? `{ ${entries.map(([k, x]) => `${JSON.stringify(k)}: ${inline(x)}`).join(', ')} }` : '{}';
}

function format(v: Json, indent: string, prefix: number, path: string[]): string {
  const one = inline(v);
  if (v === null || typeof v !== 'object') return one;
  const expand = path.length === 0 || (path[0] === '$defs' && path.length <= 2) || path.at(-1) === 'properties';
  if (!expand && indent.length + prefix + one.length <= WIDTH) return one;
  const inner = indent + '  ';
  if (Array.isArray(v)) {
    if (!v.length) return '[]';
    if (v.every((x) => x === null || typeof x !== 'object')) {
      // A long list of strings or numbers: as many per line as fit.
      const rows: string[] = [];
      for (const item of v.map((x) => JSON.stringify(x))) {
        const last = rows.length - 1;
        if (last >= 0 && inner.length + rows[last]!.length + item.length + 2 <= WIDTH) rows[last] += ` ${item},`;
        else rows.push(`${item},`);
      }
      rows[rows.length - 1] = rows.at(-1)!.slice(0, -1);
      return `[\n${rows.map((r) => inner + r).join('\n')}\n${indent}]`;
    }
    return `[\n${v.map((x, i) => inner + format(x, inner, 1, [...path, String(i)])).join(',\n')}\n${indent}]`;
  }
  const entries = Object.entries(v);
  if (!entries.length) return '{}';
  const lines = entries.map(([k, x]) => {
    const key = `${JSON.stringify(k)}: `;
    return inner + key + format(x, inner, key.length + 1, [...path, k]);
  });
  return `{\n${lines.join(',\n')}\n${indent}}`;
}

// ---------------------------------------------------------------------------
// TypeScript types
// ---------------------------------------------------------------------------

export async function generateTypes(schema: Schema = readSchema()): Promise<string> {
  // Wrap so every $def is emitted as its own named, exported type.
  const root = {
    $defs: schema.$defs,
    title: 'BasisTypes',
    type: 'object',
    additionalProperties: false,
    properties: Object.fromEntries(Object.keys(schema.$defs).map((k) => [k, { $ref: `#/$defs/${k}` }])),
  };
  const banner = '/* Generated from schema/basis.schema.json by scripts/gen.ts. Do not edit by hand. */';
  let ts = await compile(root as any, 'BasisTypes', {
    bannerComment: banner,
    additionalProperties: false,
    unreachableDefinitions: true,
    format: true,
    style: { singleQuote: true },
  });
  // Drop the wrapper interface; it only exists to pull in every $def.
  ts = ts.replace(/export interface BasisTypes \{[\s\S]*?\n\}\n/, '');
  return ts;
}

// ---------------------------------------------------------------------------
// zod/mini validators
// ---------------------------------------------------------------------------

/**
 * zod/mini validators, one per $def. Each is annotated with its generated
 * TypeScript type, so `pnpm typecheck` fails if the two disagree. Objects
 * are strict (unknown fields are errors), like the JSON Schema.
 */
export async function generateZod(schema: Schema = readSchema()): Promise<string> {
  const defs: Record<string, Schema> = schema.$defs;
  const KEYWORDS = new Set([
    'description',
    'unevaluatedProperties',
    'additionalProperties',
    'required',
    'properties',
    'type',
    'items',
    'allOf',
    'oneOf',
    'anyOf',
    '$ref',
    'enum',
    'const',
    'pattern',
    'format',
  ]);
  let current = '';

  /** zod/mini source for a schema. Every $def it refers to is added to `refs`. */
  const expr = (s: Schema, refs: Set<string>): string => {
    for (const k of Object.keys(s)) if (!KEYWORDS.has(k)) throw new Error(`Unsupported keyword ${k} in ${current}`);
    if (s.$ref) return ref(s.$ref, refs);
    if (s.oneOf || s.anyOf) return `z.union([${(s.oneOf ?? s.anyOf).map((x: Schema) => expr(x, refs)).join(', ')}])`;
    if ('const' in s) return `z.literal(${JSON.stringify(s.const)})`;
    if (s.enum) return `z.enum(${JSON.stringify(s.enum)})`;
    if (s.allOf) return objectExpr(s, refs);
    if (Array.isArray(s.type))
      return `z.union([${s.type.map((t: string) => expr({ ...s, type: t }, refs)).join(', ')}])`;
    switch (s.type) {
      case undefined:
        return 'z.unknown()';
      case 'null':
        return 'z.null()';
      case 'boolean':
        return 'z.boolean()';
      case 'integer':
        return 'z.int()';
      case 'number':
        return 'z.number()';
      case 'string':
        if (s.format === 'date') return 'z.iso.date()';
        if (s.format) throw new Error(`Unsupported format ${s.format} in ${current}`);
        return s.pattern ? `z.string().check(z.regex(${regex(s.pattern)}))` : 'z.string()';
      case 'array':
        return `z.array(${s.items ? expr(s.items, refs) : 'z.unknown()'})`;
      case 'object':
        return objectExpr(s, refs);
    }
    throw new Error(`Unsupported type ${s.type} in ${current}`);
  };

  const ref = (r: string, refs: Set<string>): string => {
    const m = /^#\/\$defs\/(\w+)$/.exec(r);
    if (!m || !defs[m[1]!]) throw new Error(`Unsupported $ref ${r} in ${current}`);
    refs.add(m[1]!);
    return m[1]!;
  };

  /** Object fields. A field that refers to another def is a getter, so defs can refer to each other in any order. */
  const shape = (s: Schema): string[] => {
    const required = new Set<string>(s.required ?? []);
    return Object.entries((s.properties ?? {}) as Record<string, Schema>).map(([k, p]) => {
      const refs = new Set<string>();
      let e = expr(p, refs);
      if (!required.has(k)) e = `z.optional(${e})`;
      const key = /^[A-Za-z_$][\w$]*$/.test(k) ? k : JSON.stringify(k);
      return refs.size ? `get ${key}() { return ${e}; }` : `${key}: ${e}`;
    });
  };

  const objectExpr = (s: Schema, refs: Set<string>): string => {
    const own = shape(s);
    if (!s.allOf) return `z.strictObject({ ${own.join(', ')} })`;
    // allOf: [{ $ref: a oneOf of objects }] plus own properties becomes one
    // strict object per variant, with the variant's fields and our own.
    if (s.allOf.length !== 1 || !s.allOf[0].$ref) throw new Error(`Unsupported allOf in ${current}`);
    const base = defs[ref(s.allOf[0].$ref, new Set())]!;
    const variants: Schema[] = base.oneOf ? base.oneOf.map((v: Schema) => defs[ref(v.$ref, refs)]!) : [base];
    return `z.union([${variants.map((v) => `z.strictObject({ ${[...shape(v), ...own].join(', ')} })`).join(', ')}])`;
  };

  // Refs used outside getters must be declared first.
  const bodies = new Map<string, string>();
  const eager = new Map<string, Set<string>>();
  for (const [name, s] of Object.entries(defs)) {
    current = name;
    const refs = new Set<string>();
    bodies.set(name, expr(s, refs));
    eager.set(name, refs);
  }
  const order: string[] = [];
  const visiting = new Set<string>();
  const visit = (n: string) => {
    if (order.includes(n)) return;
    if (visiting.has(n)) throw new Error(`Defs ${[...visiting].join(', ')} refer to each other outside objects`);
    visiting.add(n);
    for (const d of eager.get(n)!) visit(d);
    visiting.delete(n);
    order.push(n);
  };
  Object.keys(defs).forEach(visit);

  const lines = [
    '/* Generated from schema/basis.schema.json by scripts/gen.ts. Do not edit by hand. */',
    "import * as z from 'zod/mini';",
    "import type * as T from './types.gen.ts';",
    '',
  ];
  for (const name of order) {
    const desc: string | undefined = defs[name]!.description;
    if (desc) lines.push(`/** ${desc.replace(/\*\//g, '*\\/')} */`);
    lines.push(`export const ${name}: z.ZodMiniType<T.${name}> = ${bodies.get(name)};`, '');
  }
  return prettier(lines.join('\n'), { parser: 'typescript', singleQuote: true, printWidth: 120 });
}

function regex(pattern: string): string {
  return `/${pattern.replace(/\//g, '\\/')}/`;
}

// ---------------------------------------------------------------------------
// bundled curated data
// ---------------------------------------------------------------------------

/** src/tables/curated.gen.ts: the curated CSVs in a compact form, one row per line. */
export async function generateCurated(dir: URL = new URL('../data/curated/', import.meta.url)): Promise<string> {
  const read = (f: string) => readCsvAll(new URL(f, dir).pathname);
  const curated = curatedFromCsv(
    await read('people.csv'),
    await read('members.csv'),
    await read('vote_descriptions.csv'),
  );
  const { people, members, voteDescriptions } = encodeCurated(curated);
  return [
    '/* Generated from data/curated/*.csv by scripts/gen.ts. Do not edit by hand. */',
    "import type { CompactCurated } from './curated.ts';",
    '',
    'export const CURATED: CompactCurated = {',
    '  people: [',
    ...people.map((p) => `${JSON.stringify(p)},`),
    '  ],',
    '  members: [',
    ...members.map((m) => `${JSON.stringify(m)},`),
    '  ],',
    '  voteDescriptions: [',
    ...voteDescriptions.map((d) => `${JSON.stringify(d)},`),
    '  ],',
    '};',
    '',
  ].join('\n');
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const schema = readSchema();
  writeFileSync(SCHEMA_URL, formatSchema(schema));
  writeFileSync(new URL('../src/types.gen.ts', import.meta.url), await generateTypes(schema));
  writeFileSync(new URL('../src/zod.gen.ts', import.meta.url), await generateZod(schema));
  writeFileSync(new URL('../src/tables/curated.gen.ts', import.meta.url), await generateCurated());
  console.log('wrote schema/basis.schema.json, src/types.gen.ts, src/zod.gen.ts, src/tables/curated.gen.ts');
}
