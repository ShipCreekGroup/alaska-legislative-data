// Generates src/types.gen.ts from schema/basis.schema.json.
// Run with `npm run gen` after editing the schema. A test fails if the
// committed file is out of date.
import { readFileSync, writeFileSync } from 'node:fs';
import { compile } from 'json-schema-to-typescript';

export async function generateTypes(): Promise<string> {
  const schema = JSON.parse(readFileSync(new URL('../schema/basis.schema.json', import.meta.url), 'utf8'));
  // Wrap so every $def is emitted as its own named, exported type.
  const root = {
    $defs: schema.$defs,
    title: 'BasisTypes',
    type: 'object',
    additionalProperties: false,
    properties: Object.fromEntries(Object.keys(schema.$defs).map((k) => [k, { $ref: `#/$defs/${k}` }])),
  };
  const banner = '/* Generated from schema/basis.schema.json by scripts/gen-types.ts. Do not edit by hand. */';
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

if (import.meta.url === `file://${process.argv[1]}`) {
  writeFileSync(new URL('../src/types.gen.ts', import.meta.url), await generateTypes());
  console.log('wrote src/types.gen.ts');
}
