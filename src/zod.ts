// zod/mini validators for every BASIS API shape, generated from
// schema/basis.schema.json, plus `validate` for createBasis():
//
//   import { createBasis } from 'alaska-legislative-api';
//   import { validate } from 'alaska-legislative-api/zod';
//   const basis = createBasis({ validate });
//
// Needs zod 4 (an optional peer dependency of this package).
import type { Endpoint, Validate } from './client.ts';
import * as s from './zod.gen.ts';

export * from './zod.gen.ts';

/** The validator for each endpoint's whole response body, `{ Basis: ... }`. */
export const responses = {
  bills: s.BillsResponse,
  committees: s.CommitteesResponse,
  meetings: s.MeetingsResponse,
  members: s.MembersResponse,
  sessions: s.SessionsResponse,
} as const satisfies Record<Endpoint, unknown>;

/** At most this many mismatches are reported per response. */
const MAX_PROBLEMS = 10;

/**
 * Check a response body strictly (unknown fields are mismatches). Returns one
 * line per distinct mismatch, with array indexes shown as `*`.
 */
export const validate: Validate = (endpoint, body) => {
  const result = responses[endpoint].safeParse(body);
  if (result.success) return [];
  const seen = new Set<string>();
  for (const line of describeIssues(result.error.issues)) {
    seen.add(line);
    if (seen.size >= MAX_PROBLEMS) break;
  }
  return [...seen];
};

interface Issue {
  code: string;
  path: PropertyKey[];
  expected?: string;
  format?: string;
  pattern?: string;
  values?: unknown[];
  keys?: string[];
  errors?: Issue[][];
}

function describeIssues(issues: readonly unknown[], prefix: PropertyKey[] = []): string[] {
  const out: string[] = [];
  for (const raw of issues) {
    const issue = raw as Issue;
    const path = [...prefix, ...issue.path];
    if (issue.code === 'invalid_union' && issue.errors?.length) {
      // Report the branch that came closest: the one with the fewest issues.
      const best = [...issue.errors].sort((a, b) => count(a) - count(b))[0]!;
      out.push(...describeIssues(best, path));
      continue;
    }
    const where = path.map((p) => (typeof p === 'number' ? '*' : String(p))).join('.') || '(root)';
    out.push(`${where}: ${message(issue)}`);
  }
  return out;
}

// zod/mini ships no error messages unless a locale is loaded, so build our own.
function message(issue: Issue): string {
  switch (issue.code) {
    case 'invalid_type':
      return `expected ${issue.expected}`;
    case 'unrecognized_keys':
      return `unknown fields ${issue.keys?.join(', ')}`;
    case 'invalid_format':
      return issue.format === 'regex' ? `doesn't match ${issue.pattern}` : `not a valid ${issue.format}`;
    case 'invalid_value':
      return `expected one of ${issue.values?.map((v) => JSON.stringify(v)).join(', ')}`;
    default:
      return issue.code.replace(/_/g, ' ');
  }
}

function count(issues: Issue[]): number {
  return issues.reduce((n, i) => n + (i.code === 'invalid_union' && i.errors?.length ? Math.min(...i.errors.map(count)) : 1), 0);
}
