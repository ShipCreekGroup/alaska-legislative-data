# Alaska Legislative Data

Clean data on the Alaska State Legislature: bills, roll-call votes, individual legislator choices, members, people, and full bill text. Covers legislatures **12–34** (roughly 1981–2026);

We fetch and clean the data daily, keep the history of every row, and publish the results to the
[releases page](https://github.com/ShipCreekGroup/alaska-legislative-data/releases).

---

## Usage

The easiest way to analyze this data is tell your favorite ai agent _that has code execution ability_, such as [claude.ai](https://claude.ai) or [chatgpt Codex](https://chatgpt.com/codex/) (as of July 2026, chatgpt.com and gemini can't do this) to read the included [SKILL.md](./SKILL.md), and then ask your question. Example:

> First read https://raw.githubusercontent.com/ShipCreekGroup/alaska-legislative-data/refs/heads/main/SKILL.md.
> Find me recent contentious votes on affordability and cost of living and breakdown on how
> legislators votes.

## License and other Services

While SCG provides this data as a courtesy to the community (MIT license), we also have some paid services
that may be of interest to you.
For example, we use this data to build a proprietary vote tracking service for the Alaska Legislature.
If you are a lobbyist, nonprofit, or someone who wants to be able to keep close tabs on
key votes, the voting history of representatives, and the progress of bills through
the legislature, please get in touch at https://shipcreekgroup.com.

If you have questions about this code or data, please get in touch with
n.crews@shipcreekgroup.com, or file an issue in the tab above.

## Details

The repo has three parts:

1. **[`schema/basis.schema.json`](./schema/basis.schema.json)**: a [JSON Schema](https://json-schema.org)
   describing the responses of the Legislature's BASIS API (`https://www.akleg.gov/publicservice/basis`).
   The official `docs/basis.xsd` is out of date; this schema was written from real responses and is
   checked against the live API every day. Use it to generate types in any language
   (eg with [quicktype](https://quicktype.io)).
2. **A TypeScript client** ([`src/client.ts`](./src/client.ts)), published on npm as `alaska-legislative-api`.
   No dependencies. Runs anywhere `fetch` does (Node, Deno, Bun, browsers, Workers), and accepts your
   own fetch function. Optional [zod](https://zod.dev) validators, generated from the schema, live in
   `alaska-legislative-api/zod`, and the cleaned tables in `alaska-legislative-api/tables`.
3. **The `akleg` command line tool**, which fetches and cleans the data and is what the nightly job runs.

### Client

```bash
npm install alaska-legislative-api
```

```ts
import { createBasis } from 'alaska-legislative-api';

const basis = createBasis();
const bills = await basis.bills.list({
  session: 34,
  query: { bills: { bill: 'HB 16' }, sponsors: true, versions: { fulltext: 'urlonly' } },
});
const members = await basis.members.list({ session: 34, query: { members: { code: 'FRB' }, votes: true } });
const n = await basis.bills.count({ session: 34 });
const legislature = await basis.sessions.get({ session: 34 });
const text = await basis.billText.get({ session: 34, bill: 'HB 16', version: 'A' });
```

Endpoint groups: `bills`, `committees`, `meetings` and `members` each have `list()` and `count()`
(a HEAD request, nothing downloaded); `sessions.get()` returns one legislature; `billText.get()`
returns a bill version's plain text with line numbers removed (`getRaw()` returns it as served);
`request(endpoint, params)` returns the raw parsed response. Every call takes `session` (legislature
number), `chamber` (`'H'` or `'S'`), `query` and `range`. The `query` object is typed per endpoint and
turned into the API's `X-Alaska-Legislature-Basis-Query` header; a raw header string also works.
See [`docs/docs.pdf`](./docs/docs.pdf) for what each filter does.

The client limits itself to 5 requests at once and retries server errors with backoff.
Errors are thrown as `BasisError`; `err.isDataUnavailable` is true for legislatures the API has no data for.

Options: `fetch`, `baseUrl`, `textBaseUrl`, `maxConcurrency`, `maxAttempts`, `sleep`, `logger`,
`headers`, `validate`.

**Validation:** install `zod` (version 4) and pass the generated validator. Each response is checked
strictly, unknown fields included. Mismatches are logged with `logger.warn` and the data is returned
anyway, so a change on the Legislature's side never breaks your code.

```ts
import { createBasis } from 'alaska-legislative-api';
import { validate, Bill } from 'alaska-legislative-api/zod';

const basis = createBasis({ validate });
Bill.parse(someObject); // every schema type is also exported as a zod/mini validator
```

### Tables

`alaska-legislative-api/tables` wraps the client and returns exactly the rows of the published
tables, with the same IDs (see [SKILL.md](./SKILL.md) for the columns):

```ts
import { createBasis } from 'alaska-legislative-api';
import { createTables } from 'alaska-legislative-api/tables';

const tables = createTables(createBasis());
const people = await tables.people();
const members = await tables.members({ legislature: 34 });
const bills = await tables.bills({ legislature: 34 });
const { votes, choices } = await tables.votes({ legislature: 34 }); // one request per member
const versions = await tables.billVersions({ legislature: 34, bills: ['HB 16'] });
```

The hand-kept people and members lists are bundled (about 88 KB, 22 KB gzipped). Pass
`curated` to use your own, and `logger` to collect data warnings.

IDs are built with the helpers in `src/tables/ids.ts`, eg `billId(34, 'HB 16')` is
`'bil_34th_HB0016'` and `memberId(34, 'H', 'per_robyn-burke-34th')` is `'mem_34th_H_robyn-burke-34th'`.

**Browsers:** works, except `range`, which the server's CORS policy blocks.

**Other environments:** pass any function shaped like `fetch` as the `fetch` option, and a `sleep`
if `setTimeout` doesn't exist there.

### Command line

Needs Node 22.18 or later. From a clone of this repo, run `node src/cli.ts` instead of `npx akleg`. Add `--validate` to any command to log responses that don't match the schema.

```bash
npx akleg count bills --session 34
npx akleg get bills --session 34 --query 'bills;bill=HB 16,sponsors'
npx akleg text 34 "HB 16" A
npx akleg fetch --store raw-store --legislatures 34     # download into a local raw store
npx akleg build --store raw-store --out export          # build the tables and their history
```

### How the data is built

Only two things are stored as sources of truth; everything else is rebuilt from them by code:

1. **The raw store**: every API response we fetch, on [Cloudflare R2](https://developers.cloudflare.com/r2/)
   (or a local directory with `--store DIR`). `raw/objects/<sha256>.gz` holds each response body,
   named by the hash of its content and written only when the content is new;
   `raw/log/<YYYY-MM-DD>.ndjson.gz` has one line per request (time, URL, query, range, status, body
   hash). Nothing in it is ever edited or deleted, so an unchanged response only adds a log line.
2. **The curated CSVs** in [`data/curated`](./data/curated): `people.csv`, `members.csv` and
   `vote_descriptions.csv` (the one hand-typed column of `votes`).

`akleg fetch` downloads the current legislature (all of them on the first run, which takes hours,
mostly bill text) through the same `tables` code that builds the tables, recording every response.
Bill text is downloaded once per version.

`akleg build` replays the raw store to rebuild the tables as of each fetch date and compares
consecutive dates row by row. It writes each table twice: `X.csv` with the current rows and
`X_history.csv` with every version of every row plus `ValidFrom` and `ValidTo` (the first date the
version was no longer current; empty for current rows). A row the API stops returning gets a
`ValidTo`. Only legislatures fetched on a date are compared, so old legislatures aren't closed just
because they weren't downloaded again. History starts on the first fetch.

Because everything is rebuilt from the roots, fixing a cleaning bug fixes every past version too,
and a new column is filled into existing versions instead of creating new ones. Edits to the curated
CSVs also apply to every date.

[`scripts/build-duckdb.sql`](./scripts/build-duckdb.sql) packs the CSVs into `ak_leg.duckdb`, where
each `X_history` is a table and `X` is a view of its current rows, and writes Parquet copies.
`akleg publish` copies the results to `published/<YYYY-MM-DD>/` and `published/latest/` on R2,
keeping the last 30 days.

People and legislators can't be tracked across legislatures from the API alone (member codes change,
names repeat), so `people.csv` and `members.csv` are hand-curated. Edit them to add new legislators,
then run `pnpm gen` to update the bundled copy. A new person's `PersonId` is `per_` + their name
as a slug + their first legislature, eg `per_robyn-burke-34th`; it never changes afterwards, even if
their name does. `members.csv` has no `MemberId` column: it's built from `LegislatureNumber`,
`MemberChamber` and `PersonId`.

### Nightly job

[`.github/workflows/scrape.yml`](./.github/workflows/scrape.yml) runs `akleg fetch`, `akleg build`,
the DuckDB script and `akleg publish`, then publishes the export as the only GitHub release (older
releases are deleted). A separate job runs the contract tests against the live API. Run the workflow
by hand with "full" checked to fetch every legislature again.

It reads the R2 bucket's location from two GitHub Actions variables, `CLOUDFLARE_ACCOUNT_ID` and
`R2_BUCKET`, and its credentials from two secrets, `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY` (an
R2 API token with read and write access to the bucket).
The first run fills the raw store from scratch.

### Development

```bash
pnpm install
pnpm test         # offline tests, using recorded responses in test/fixtures
pnpm test:live    # check the live API against the schema
pnpm gen          # after editing the schema: rewrites it in the standard layout, regenerates the types and zod validators
pnpm typecheck    # also checks the zod validators agree with the TypeScript types
pnpm lint         # oxlint
pnpm format       # oxfmt (format:check to only check)
pnpm build        # dist/ (npm package)
```

To change the schema: edit `schema/basis.schema.json`, run `pnpm gen`, and commit the schema,
`src/types.gen.ts` and `src/zod.gen.ts`. Tests fail if any of them is out of date.
