import { ClientOnly, createFileRoute } from '@tanstack/solid-router';
import { For, createSignal } from 'solid-js';
import { Breadcrumbs, Container, PageTitle, Section } from '~/components/site';
import { buttonVariants } from '~/components/ui/button';
import { ApiBuilder } from '~/components/api-builder';
import { fetchMeta } from '~/server/fns';
import { date } from '~/lib/format';
import { seo } from '~/lib/seo';
import { cn } from '~/lib/utils';

const ZIP = 'https://github.com/ShipCreekGroup/alaska-legislative-data/releases/latest/download/export.zip';
const SKILL = 'https://raw.githubusercontent.com/ShipCreekGroup/alaska-legislative-data/refs/heads/main/SKILL.md';
const crumbs = [
  { name: 'Home', path: '/' },
  { name: 'Data & API', path: '/developers' },
];

const SNIPPETS: Record<string, string> = {
  DuckDB: `curl -sL -o export.zip ${ZIP}\nunzip export.zip\nduckdb export/ak_leg.duckdb -readonly -c "SELECT count(*) FROM votes WHERE LegislatureNumber = 34"`,
  Python: `import duckdb  # pip install duckdb\n\ncon = duckdb.connect("export/ak_leg.duckdb", read_only=True)\ndf = con.sql("""\n    SELECT m.MemberParty, c.Choice, count(*) AS n\n    FROM choices c JOIN members m USING (MemberId)\n    WHERE m.LegislatureNumber = 34\n    GROUP BY ALL ORDER BY ALL\n""").df()`,
  R: `library(duckdb)  # install.packages("duckdb")\n\ncon <- dbConnect(duckdb(), "export/ak_leg.duckdb", read_only = TRUE)\ndbGetQuery(con, "\n  SELECT BillNumber, BillShortTitle FROM bills\n  WHERE LegislatureNumber = 34 AND list_contains(BillSubjects, 'EDUCATION')\n")`,
  TypeScript: `// npm install alaska-legislative-api\n// Builds the same tables straight from the Legislature's API.\nimport { createBasis } from "alaska-legislative-api";\nimport { createTables } from "alaska-legislative-api/tables";\n\nconst tables = createTables(createBasis());\nconst bills = await tables.bills({ legislature: 34 });`,
  'Any page': `# Every record page has a JSON copy at /data/<type>/<id>.json\ncurl https://akleg.shipcreekgroup.com/data/bills/<BillId>.json`,
};

export const Route = createFileRoute('/developers')({
  loader: () => fetchMeta(),
  head: () =>
    seo({
      title: 'Download Alaska Legislature data and use the API',
      description:
        'Download every Alaska Legislature bill, roll-call vote and legislator as CSV, Parquet or DuckDB, updated nightly. Or query the Legislature’s live BASIS API.',
      path: '/developers',
      crumbs,
    }),
  component: Developers,
});

function Developers() {
  const meta = Route.useLoaderData();
  const [lang, setLang] = createSignal('DuckDB');
  return (
    <Container>
      <Breadcrumbs crumbs={crumbs} />
      <PageTitle title="Data and API">
        Everything on this site is free to download and reuse under the MIT license. Please credit Ship Creek Group.
        Last updated {date(meta().updated)}.
      </PageTitle>

      <Section title="Download">
        <div class="flex flex-wrap items-center justify-between gap-4 rounded-xl border bg-card p-5">
          <div class="max-w-xl">
            <div class="font-display font-bold">export.zip</div>
            <p class="text-sm text-muted-foreground">
              All six tables (bills, bill versions, votes, individual choices, members, people) and their change
              history, as CSV, Parquet and one DuckDB database file. Rebuilt every night.
            </p>
          </div>
          <a class={buttonVariants()} href={ZIP}>
            Download zip
          </a>
        </div>
        <div>
          <div class="inline-flex rounded-t-md border border-b-0 bg-card p-0.5">
            <For each={Object.keys(SNIPPETS)}>
              {(l) => (
                <button
                  type="button"
                  class={cn(
                    'rounded px-2.5 py-0.5 text-xs font-semibold',
                    lang() === l ? 'bg-accent' : 'text-muted-foreground',
                  )}
                  onClick={() => setLang(l)}
                >
                  {l}
                </button>
              )}
            </For>
          </div>
          <pre class="overflow-x-auto rounded-b-lg rounded-tr-lg bg-[oklch(0.2_0.03_255)] p-4 font-mono text-xs leading-relaxed text-[oklch(0.9_0.01_250)]">
            {SNIPPETS[lang()]}
          </pre>
        </div>
      </Section>

      <Section title="For AI agents">
        <p class="max-w-3xl text-sm">
          Point an agent at <a href={SKILL}>SKILL.md</a>: it covers setup, every table and column, how to join them, and
          common mistakes. Agents can also read any page on this site directly, use the JSON copy linked at the bottom
          of every record, or start from <a href="/llms.txt">/llms.txt</a>.
        </p>
      </Section>

      <Section title="Ask the Legislature's server directly">
        <p class="max-w-3xl text-sm text-muted-foreground">
          For today's data, including committee meetings and fiscal notes that aren't in the download. Click any
          underlined word to change it. Please send one request at a time: this server also runs akleg.gov.
        </p>
        <ClientOnly fallback={<div class="h-64 rounded-xl border bg-card" />}>
          <ApiBuilder
            legislatures={meta()
              .legislatures.filter((l) => l.number >= 18)
              .map((l) => l.number)}
          />
        </ClientOnly>
      </Section>
    </Container>
  );
}
