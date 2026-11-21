#!/usr/bin/env node
// Command line interface. `akleg fetch`, `build` and `publish` run the nightly
// job; `akleg get|count|text` are thin wrappers around the client.
import { join } from 'node:path';
import { parseArgs } from 'node:util';
import { build } from './build/build.ts';
import { readCsvAll } from './build/csv.ts';
import { fetchRaw } from './build/fetch.ts';
import { publish } from './build/publish.ts';
import { createBasis, type Endpoint, type Logger } from './client.ts';
import { FileStore, R2Store, type BlobStore } from './store/blob.ts';
import { RawStore } from './store/raw.ts';
import { curatedFromCsv } from './tables/curated.ts';

const HELP = `Usage: akleg <command> [options]

Commands:
  fetch             Download legislatures from the API into the raw store
    --legislatures L  Comma separated, eg 33,34. Default: the current one, the latest with
                      data, and any never fetched (all of them on the first run: hours)
    --all             Every legislature
    --skip-text       Don't download bill text for new versions
    --recheck-missing-versions
                      Also re-check older bills that had no versions yet (slow)

  build             Build the tables and their history from the raw store
    --out DIR         Output directory (default: export)
    --date D          As of this date, yyyy-mm-dd (default: the latest fetch)

  publish           Copy DIR to published/<date>/ and published/latest/ in the store,
                    and delete dated sets older than 30 days
    --dir DIR         Default: export

  Options for fetch, build and publish:
    --store S         r2 (reads R2_ACCOUNT_ID or CLOUDFLARE_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET)
                      or a local directory (default: raw-store)
    --cache DIR       Local copies of raw objects (default: .akleg-cache)
    --curated DIR     Directory with people.csv, members.csv and vote_descriptions.csv
                      (default: the bundled copy of data/curated)

  get ENDPOINT      Print the API response as JSON. ENDPOINT: bills, committees, meetings, members, sessions
  count ENDPOINT    Print the number of results
    --session N       Legislature number, eg 34
    --chamber H|S
    --query Q         Raw query header, eg 'bills;bill=HB 16,sponsors,versions;fulltext=urlonly'
    --range R         eg 10, 10..20, ..30

  text LEG BILL VERSION   Print the plain text of a bill version, eg: akleg text 34 "HB 16" A

Common options:
  --validate        Check every response against the schema and log mismatches (needs zod)
  --verbose         Log every request
  --concurrency N   Maximum requests in flight (default 5)
`;

const ENDPOINTS = ['bills', 'committees', 'meetings', 'members', 'sessions'];

async function main(argv: string[]): Promise<number> {
  const { values, positionals } = parseArgs({
    args: argv,
    allowPositionals: true,
    options: {
      out: { type: 'string', default: 'export' },
      dir: { type: 'string', default: 'export' },
      store: { type: 'string', default: 'raw-store' },
      cache: { type: 'string', default: '.akleg-cache' },
      date: { type: 'string' },
      all: { type: 'boolean', default: false },
      legislatures: { type: 'string' },
      curated: { type: 'string' },
      'skip-text': { type: 'boolean', default: false },
      'recheck-missing-versions': { type: 'boolean', default: false },
      session: { type: 'string' },
      chamber: { type: 'string' },
      query: { type: 'string' },
      range: { type: 'string' },
      validate: { type: 'boolean', default: false },
      verbose: { type: 'boolean', default: false },
      concurrency: { type: 'string' },
      help: { type: 'boolean', short: 'h', default: false },
    },
  });
  const [command, ...rest] = positionals;
  if (values.help || !command) {
    process.stdout.write(HELP);
    return command || values.help ? 0 : 1;
  }
  const logger: Logger = {
    debug: values.verbose ? (...a) => console.error(stamp(), ...a) : () => {},
    info: (...a) => console.error(stamp(), ...a),
    warn: (...a) => console.error(stamp(), 'WARN', ...a),
    error: (...a) => console.error(stamp(), 'ERROR', ...a),
  };
  const basis = createBasis({
    logger,
    validate: values.validate ? (await import('./zod.ts')).validate : undefined,
    maxConcurrency: values.concurrency ? Number(values.concurrency) : undefined,
  });

  if (command === 'fetch' || command === 'build' || command === 'publish') {
    const store: BlobStore = values.store === 'r2' ? R2Store.fromEnv() : new FileStore(values.store!);
    if (command === 'publish') {
      await publish({ store, dir: values.dir!, date: values.date, logger });
      return 0;
    }
    const raw = new RawStore(store, { cacheDir: values.cache });
    const curated = values.curated
      ? curatedFromCsv(
          await readCsvAll(join(values.curated, 'people.csv')),
          await readCsvAll(join(values.curated, 'members.csv')),
          await readCsvAll(join(values.curated, 'vote_descriptions.csv')),
        )
      : undefined;
    if (command === 'fetch') {
      const summary = await fetchRaw({
        raw,
        curated,
        logger,
        date: values.date,
        all: values.all,
        legislatures: values.legislatures ? values.legislatures.split(',').map(Number) : undefined,
        skipText: values['skip-text'],
        recheckMissingVersions: values['recheck-missing-versions'],
        client: {
          maxConcurrency: values.concurrency ? Number(values.concurrency) : undefined,
          validate: values.validate ? (await import('./zod.ts')).validate : undefined,
        },
      });
      logger.info(`Done: ${summary.requests} responses, ${summary.warnings.length} warnings`);
    } else {
      const summary = await build({ raw, curated, logger, outDir: values.out!, date: values.date });
      logger.info(`Done: ${summary.dates.length} dates, ${summary.builds} builds.`, JSON.stringify(summary.rows), `${summary.warnings.length} warnings`);
    }
    return 0;
  }

  if (command === 'get' || command === 'count') {
    const endpoint = rest[0];
    if (!endpoint || !ENDPOINTS.includes(endpoint)) {
      console.error(`Unknown endpoint ${endpoint ?? ''}. Use one of: ${ENDPOINTS.join(', ')}`);
      return 1;
    }
    const params = {
      session: values.session ? Number(values.session) : undefined,
      chamber: values.chamber as 'H' | 'S' | undefined,
      query: values.query,
      range: values.range,
    };
    if (command === 'count') {
      if (endpoint === 'sessions') {
        console.error('The sessions endpoint has no count.');
        return 1;
      }
      console.log(await basis[endpoint as Exclude<Endpoint, 'sessions'>].count(params));
    } else {
      console.log(JSON.stringify((await basis.request(endpoint as Endpoint, params)).data, null, 2));
    }
    return 0;
  }

  if (command === 'text') {
    const [leg, bill, version] = rest;
    if (!leg || !bill || !version) {
      console.error('Usage: akleg text LEG BILL VERSION');
      return 1;
    }
    process.stdout.write((await basis.billText.get({ session: Number(leg), bill, version })) + '\n');
    return 0;
  }

  console.error(`Unknown command ${command}\n\n${HELP}`);
  return 1;
}

function stamp(): string {
  return new Date().toISOString();
}

main(process.argv.slice(2)).then(
  (code) => process.exit(code),
  (e) => {
    console.error(e);
    process.exit(1);
  },
);
