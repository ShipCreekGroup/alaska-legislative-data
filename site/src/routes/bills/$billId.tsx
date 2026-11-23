import { Link, createFileRoute } from '@tanstack/solid-router';
import { For, Show } from 'solid-js';
import { Badge } from '~/components/ui/badge';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '~/components/ui/table';
import { Breadcrumbs, Container, DataLinks, PageTitle, Section } from '~/components/site';
import { VotesTable } from '~/components/lists';
import { fetchBill } from '~/server/fns';
import { SITE_URL, akleg, clip, date, legName, legYears } from '~/lib/format';
import { billSummary as summary } from '~/lib/summaries';
import { seo } from '~/lib/seo';
import type { Bill } from '~/lib/types';
import { href } from '~/lib/search';

const crumbsFor = (b: Bill) => [
  { name: 'Home', path: '/' },
  { name: legName(b.legislature), path: `/legislatures/${b.legislature}` },
  { name: 'Bills', path: href('/bills', { legislature: b.legislature }) },
  { name: b.number, path: `/bills/${encodeURIComponent(b.id)}` },
];

export const Route = createFileRoute('/bills/$billId')({
  loader: ({ params }) => fetchBill({ data: { id: params.billId } }),
  head: ({ loaderData: b }) => {
    if (!b) return {};
    const path = `/bills/${encodeURIComponent(b.id)}`;
    return seo({
      title: `${b.number} (${legName(b.legislature)}): ${b.title ?? 'Alaska bill'}`,
      description: clip(summary(b)),
      path,
      crumbs: crumbsFor(b),
      markdown: true,
      jsonLd: {
        '@type': 'Legislation',
        name: `${b.number}: ${b.title ?? ''}`.trim(),
        legislationIdentifier: b.number,
        legislationType: 'Bill',
        legislationJurisdiction: { '@type': 'AdministrativeArea', name: 'Alaska' },
        legislationPassedBy: {
          '@type': 'GovernmentOrganization',
          name: `${legName(b.legislature)}, Alaska State Legislature`,
        },
        dateModified: b.statusDate ?? undefined,
        keywords: b.subjects?.join(', '),
        workExample: b.versions.map((v) => ({
          '@type': 'Legislation',
          name: v.name,
          url: `${SITE_URL}/bill-versions/${encodeURIComponent(v.id)}`,
          legislationDate: v.introDate,
        })),
        sameAs: akleg.bill(b.legislature, b.number),
      },
    });
  },
  component: BillPage,
});

function BillPage() {
  const b = Route.useLoaderData();
  return (
    <Container>
      <Breadcrumbs crumbs={crumbsFor(b())} />
      <PageTitle
        eyebrow={`${legName(b().legislature)} · ${legYears(b().legislature)}`}
        title={
          <>
            {b().number} <span class="font-semibold text-muted-foreground">{b().title}</span>
          </>
        }
      >
        {summary(b())}
      </PageTitle>
      <div class="flex flex-wrap items-center gap-2">
        <Show when={b().vetoed}>
          <Badge variant="nay">Vetoed</Badge>
        </Show>
        <For each={b().subjects ?? []}>
          {(s) => (
            <a href={href('/bills', { legislature: b().legislature, subject: s })} class="no-underline">
              <Badge variant="outline" class="hover:bg-accent">
                {s}
              </Badge>
            </a>
          )}
        </For>
      </div>
      <p class="mt-4 text-sm">
        <a href={akleg.bill(b().legislature, b().number)} rel="external">
          Official bill page on akleg.gov ↗
        </a>
      </p>

      <Section title={`Versions (${b().versions.length})`}>
        <Show
          when={b().versions.length}
          fallback={<p class="text-muted-foreground">No versions of this bill are recorded.</p>}
        >
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Version</TableHead>
                <TableHead>Introduced</TableHead>
                <TableHead>Passed House</TableHead>
                <TableHead>Passed Senate</TableHead>
                <TableHead>PDF</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              <For each={b().versions}>
                {(v) => (
                  <TableRow>
                    <TableCell class="font-medium">
                      <Link to="/bill-versions/$versionId" params={{ versionId: v.id }}>
                        {v.name}
                      </Link>
                    </TableCell>
                    <TableCell>{date(v.introDate, 'short')}</TableCell>
                    <TableCell>{v.passedHouse ? date(v.passedHouse, 'short') : '—'}</TableCell>
                    <TableCell>{v.passedSenate ? date(v.passedSenate, 'short') : '—'}</TableCell>
                    <TableCell>
                      <a href={v.pdfUrl} rel="external">
                        PDF
                      </a>
                    </TableCell>
                  </TableRow>
                )}
              </For>
            </TableBody>
          </Table>
        </Show>
      </Section>

      <Section title={`Floor votes (${b().votes.length})`}>
        <Show
          when={b().votes.length}
          fallback={<p class="text-muted-foreground">No House or Senate floor votes on this bill are recorded.</p>}
        >
          <VotesTable votes={b().votes} showBill={false} />
        </Show>
      </Section>
      <DataLinks kind="bills" id={b().id} />
    </Container>
  );
}
