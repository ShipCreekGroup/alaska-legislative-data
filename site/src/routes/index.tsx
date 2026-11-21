import { Link, createFileRoute } from '@tanstack/solid-router';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '~/components/ui/card';
import { buttonVariants } from '~/components/ui/button';
import { Section } from '~/components/site';
import { VoteCards } from '~/components/lists';
import { fetchLegislatureSummary, fetchMeta } from '~/server/fns';
import { date, fmt, legName, legYears } from '~/lib/format';
import { seo } from '~/lib/seo';

export const Route = createFileRoute('/')({
  loader: async () => {
    const meta = await fetchMeta();
    const current = meta.legislatures[0]!.number;
    const summary = await fetchLegislatureSummary({ data: { legislature: current } });
    return { meta, summary };
  },
  head: ({ loaderData }) =>
    seo({
      title: 'Alaska Legislative Data: bills, roll-call votes and legislators since 1981',
      description: `Search every Alaska Legislature bill, roll-call vote and legislator since 1981: ${fmt(loaderData?.meta.bills ?? 0)} bills and ${fmt(loaderData?.meta.choices ?? 0)} individual votes. Free, updated nightly.`,
      path: '/',
      jsonLd: {
        '@type': 'Dataset',
        name: 'Alaska Legislative Data',
        description:
          'Bills, roll-call votes, how each legislator voted, legislators and full bill text for the Alaska State Legislature, 1981 to present. Refreshed nightly from the Legislature’s BASIS system.',
        license: 'https://opensource.org/licenses/MIT',
        creator: { '@type': 'Organization', name: 'Ship Creek Group', url: 'https://shipcreekgroup.com' },
        spatialCoverage: 'Alaska, United States',
        temporalCoverage: '1981/..',
        isAccessibleForFree: true,
        dateModified: loaderData?.meta.updated,
        distribution: [
          {
            '@type': 'DataDownload',
            encodingFormat: 'application/zip',
            contentUrl: 'https://github.com/ShipCreekGroup/alaska-legislative-data/releases/latest/download/export.zip',
          },
        ],
      },
    }),
  component: Home,
});

function Home() {
  const data = Route.useLoaderData();
  const s = () => data().summary;
  return (
    <>
      <div class="bg-[oklch(0.25_0.07_255)] text-white">
        <div class="mx-auto grid max-w-6xl gap-4 px-4 pt-6 pb-10">
          <h1 class="max-w-3xl text-4xl font-extrabold leading-tight">
            How the Alaska Legislature voted, on every bill since 1981
          </h1>
          <p class="max-w-2xl text-lg text-white/80">
            Look up any bill, roll-call vote or legislator. Free, and updated every night from the Legislature's own
            records.
          </p>
          <p class="text-sm text-white/70 tabular-nums">
            <b class="text-white">{fmt(data().meta.bills)}</b> bills ·{' '}
            <b class="text-white">{fmt(data().meta.votes)}</b> roll calls ·{' '}
            <b class="text-white">{fmt(data().meta.choices)}</b> individual votes ·{' '}
            <b class="text-white">{fmt(data().meta.people)}</b> legislators · updated {date(data().meta.updated)}
          </p>
          <div class="flex flex-wrap gap-2 pt-2">
            <Link
              to="/legislatures/$legislature"
              params={{ legislature: String(s().number) }}
              class={buttonVariants({ class: 'bg-gold text-[oklch(0.2_0.05_255)] hover:bg-gold/90' })}
            >
              Browse the {legName(s().number)} ({legYears(s().number)})
            </Link>
            <Link
              to="/ask"
              class={buttonVariants({
                variant: 'outline',
                class: 'border-white/30 bg-transparent text-white hover:bg-white/10',
              })}
            >
              Ask a question in plain English
            </Link>
          </div>
        </div>
      </div>
      <main class="mx-auto max-w-6xl px-4 py-8">
        <div class="grid gap-4 md:grid-cols-3">
          <Card>
            <CardHeader>
              <CardTitle>Look something up</CardTitle>
              <CardDescription>Every bill, vote and legislator has its own page you can link to.</CardDescription>
            </CardHeader>
            <CardContent class="flex flex-wrap gap-2 text-sm">
              <Link to="/bills">Bills</Link>·<Link to="/votes">Roll-call votes</Link>·
              <Link to="/legislators">Legislators</Link>·<Link to="/legislatures">All legislatures</Link>
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>Ask an AI assistant</CardTitle>
              <CardDescription>
                Copy a ready-made prompt into Claude to get answers with the work shown.
              </CardDescription>
            </CardHeader>
            <CardContent class="text-sm">
              <Link to="/ask">Get the prompt →</Link>
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>Download or use the API</CardTitle>
              <CardDescription>CSV, Parquet and DuckDB files, plus the Legislature's live API.</CardDescription>
            </CardHeader>
            <CardContent class="text-sm">
              <Link to="/developers">Data and API →</Link>
            </CardContent>
          </Card>
        </div>

        <Section title={`Closest votes in the ${legName(s().number)}`}>
          <VoteCards votes={s().closeVotes} />
        </Section>
        <Section title="Most recent votes">
          <VoteCards votes={s().recentVotes} />
        </Section>
      </main>
    </>
  );
}
