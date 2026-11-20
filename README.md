# Alaska Legislative Data

Clean data on the Alaska State Legislature: bills, roll-call votes, individual legislator choices, members, people, and full bill text. Covers legislatures **12–34** (roughly 1981–2026);

We scrape and clean the data daily, and publish the results to the
[releases page](https://github.com/ShipCreekGroup/alaska-legislative-data/releases).

---

## Usage

The easiest way to analyze this data is tell your favorite ai agent *that has code execution ability*, such as [claude.ai](https://claude.ai) or [chatgpt Codex](https://chatgpt.com/codex/) (as of July 2026, chatgpt.com and gemini can't do this) to read the included [SKILL.md](./SKILL.md), and then ask your question. Example:

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
   `alaska-legislative-api/zod`.
3. **The `akleg` command line tool**, which scrapes and cleans the data and is what the daily job runs.

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

**Browsers:** works, except `range`, which the server's CORS policy blocks.

**Other environments:** pass any function shaped like `fetch` as the `fetch` option, and a `sleep`
if `setTimeout` doesn't exist there.

### Command line

Needs Node 22.18 or later. From a clone of this repo, run `node src/cli.ts` instead of `npx akleg`. Add `--validate` to any command to log responses that don't match the schema.

```bash
npx akleg count bills --session 34
npx akleg get bills --session 34 --query 'bills;bill=HB 16,sponsors'
npx akleg text 34 "HB 16" A
npx akleg scrape --baseline path/to/previous/export --out export
```

`akleg scrape` writes `people.csv`, `members.csv`, `bills.csv`, `votes.csv`, `choices.csv` and
`bill_versions.csv`. With `--baseline` (the previous release's CSVs) it re-downloads only the latest
legislature and any missing ones, which takes a few minutes. Without it, everything is scraped, which
takes hours (mostly bill text). [`scripts/build-duckdb.sql`](./scripts/build-duckdb.sql) packs the
CSVs into `ak_leg.duckdb`.

People and legislators can't be tracked across legislatures from the API alone (member codes change,
names repeat), so `people.csv` and `members.csv` are hand-curated in [`data/curated`](./data/curated).
Edit those files to add new legislators.

### Daily job

[`.github/workflows/scrape.yml`](./.github/workflows/scrape.yml) runs every night: it downloads the
latest release, runs `akleg scrape` on top of it, builds `ak_leg.duckdb`, and publishes a new release.
A separate job runs the contract tests against the live API. Run the workflow by hand with
"full" checked to rebuild everything from scratch.

### Development

```bash
npm install
npm test            # offline tests, using recorded responses in test/fixtures
npm run test:live   # check the live API against the schema
npm run gen         # after editing the schema: rewrites it in the standard layout, regenerates the types and zod validators
npm run typecheck   # also checks the zod validators agree with the TypeScript types
npm run build       # dist/ (npm package)
```

To change the schema: edit `schema/basis.schema.json`, run `npm run gen`, and commit the schema,
`src/types.gen.ts` and `src/zod.gen.ts`. Tests fail if any of them is out of date.
