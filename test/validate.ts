import { readFileSync } from 'node:fs';
import Ajv2020, { type ValidateFunction } from 'ajv/dist/2020.js';

const schema = JSON.parse(readFileSync(new URL('../schema/basis.schema.json', import.meta.url), 'utf8'));
const ajv = new Ajv2020.default({ allErrors: true, strict: true, allowUnionTypes: true });
ajv.addSchema(schema);

/** A validator for one of the schema's $defs, eg 'BillsResponse'. */
export function validator(def: string): ValidateFunction {
  const v = ajv.getSchema(`${schema.$id}#/$defs/${def}`);
  if (!v) throw new Error(`No $def ${def}`);
  return v;
}

/** Short, readable summary of validation errors. */
export function describeErrors(v: ValidateFunction): string {
  const seen = new Set<string>();
  for (const e of v.errors ?? []) {
    seen.add(`${e.instancePath.replace(/\/\d+/g, '/*')} ${e.message} ${JSON.stringify(e.params)}`);
  }
  return [...seen].slice(0, 10).join('\n');
}
