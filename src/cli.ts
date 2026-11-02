#!/usr/bin/env node
// Command line interface: `akleg scrape` builds the published tables;
// `akleg get|count|text` are thin wrappers around the client.
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { BasisClient, type Endpoint, type Logger } from './client.ts';
import { scrape } from './scrape/scrape.ts';

const HELP = `Usage: akleg <command> [options]

Commands:
  scrape            Build the CSV tables (people, members, bills, votes, choices, bill_versions)
    --out DIR         Output directory (default: export)
    --baseline DIR    Previous release's CSVs. Only the latest legislature is re-downloaded.
                      Without this, everything is scraped, which takes hours.
    --legislatures L  Comma separated legislatures to re-download, eg 33,34
    --curated DIR     Directory with people.csv and members.csv (default: bundled data/curated)
    --skip-text       Don't download bill text for new versions
    --recheck-missing-versions
                      Also re-check older bills that have no versions yet (slow)

  get ENDPOINT      Print the API response as JSON. ENDPOINT: bills, committees, meetings, members, sessions
  count ENDPOINT    Print the number of results
    --session N       Legislature number, eg 34
    --chamber H|S
    --query Q         Raw query header, eg 'bills;bill=HB 16,sponsors,versions;fulltext=urlonly'
    --range R         eg 10, 10..20, ..30

  text LEG BILL VERSION   Print the plain text of a bill version, eg: akleg text 34 "HB 16" A

Common options:
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
      baseline: { type: 'string' },
      legislatures: { type: 'string' },
      curated: { type: 'string' },
      'skip-text': { type: 'boolean', default: false },
      'recheck-missing-versions': { type: 'boolean', default: false },
      session: { type: 'string' },
      chamber: { type: 'string' },
      query: { type: 'string' },
      range: { type: 'string' },
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
  const client = new BasisClient({
    logger,
    maxConcurrency: values.concurrency ? Number(values.concurrency) : undefined,
  });

  if (command === 'scrape') {
    const summary = await scrape({
      client,
      logger,
      outDir: values.out!,
      baselineDir: values.baseline,
      curatedDir: values.curated ?? fileURLToPath(new URL('../data/curated', import.meta.url)),
      legislatures: values.legislatures ? values.legislatures.split(',').map(Number) : undefined,
      skipText: values['skip-text'],
      recheckMissingVersions: values['recheck-missing-versions'],
    });
    logger.info('Done.', JSON.stringify(summary.rows), `${summary.warnings.length} warnings`);
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
      console.log(await client.count(endpoint as Endpoint, params));
    } else {
      console.log(JSON.stringify((await client.get(endpoint as Endpoint, params)).data, null, 2));
    }
    return 0;
  }

  if (command === 'text') {
    const [leg, bill, version] = rest;
    if (!leg || !bill || !version) {
      console.error('Usage: akleg text LEG BILL VERSION');
      return 1;
    }
    process.stdout.write((await client.billText(Number(leg), bill, version)) + '\n');
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
