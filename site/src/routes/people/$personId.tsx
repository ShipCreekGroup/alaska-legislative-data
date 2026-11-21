import { Link, createFileRoute } from '@tanstack/solid-router';
import { For } from 'solid-js';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '~/components/ui/table';
import { Breadcrumbs, Container, DataLinks, PageTitle, PartyBadge, Section } from '~/components/site';
import { fetchPerson } from '~/server/fns';
import { chamberName, clip, fmt, legName, legYears, memberTitle, partyName } from '~/lib/format';
import { personSummary as summary } from '~/lib/summaries';
import { seo } from '~/lib/seo';
import type { Person } from '~/lib/types';

const crumbsFor = (p: Person) => [
  { name: 'Home', path: '/' },
  { name: 'Legislators', path: '/people' },
  { name: p.name, path: `/people/${encodeURIComponent(p.id)}` },
];

export const Route = createFileRoute('/people/$personId')({
  loader: ({ params }) => fetchPerson({ data: { id: params.personId } }),
  head: ({ loaderData: p }) => {
    if (!p) return {};
    const last = p.terms[p.terms.length - 1];
    return seo({
      title: `${p.name}, Alaska ${last ? memberTitle(last.chamber) : 'legislator'}: terms and voting record`,
      description: clip(summary(p)),
      path: `/people/${encodeURIComponent(p.id)}`,
      crumbs: crumbsFor(p),
      markdown: true,
      jsonLd: {
        '@type': 'Person',
        name: p.name,
        givenName: p.firstName,
        familyName: p.lastName,
        additionalName: p.middleName ?? undefined,
        jobTitle: last ? `Alaska State ${memberTitle(last.chamber)}` : 'Alaska State Legislator',
        affiliation: last?.party ? { '@type': 'PoliticalParty', name: partyName(last.party) } : undefined,
      },
    });
  },
  component: () => {
    const p = Route.useLoaderData();
    return (
      <Container>
        <Breadcrumbs crumbs={crumbsFor(p())} />
        <PageTitle eyebrow="Alaska legislator" title={p().name}>
          {summary(p())}
        </PageTitle>
        <Section title="Terms">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Legislature</TableHead>
                <TableHead>Chamber</TableHead>
                <TableHead>District</TableHead>
                <TableHead>Party</TableHead>
                <TableHead>Caucus</TableHead>
                <TableHead class="text-right">Yea</TableHead>
                <TableHead class="text-right">Nay</TableHead>
                <TableHead class="text-right">Missed</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              <For each={[...p().terms].reverse()}>
                {(t) => (
                  <TableRow>
                    <TableCell class="font-medium">
                      <Link to="/members/$memberId" params={{ memberId: t.id }}>
                        {legName(t.legislature)} ({legYears(t.legislature)})
                      </Link>
                    </TableCell>
                    <TableCell>{chamberName(t.chamber)}</TableCell>
                    <TableCell>{t.district ?? '—'}</TableCell>
                    <TableCell>
                      <PartyBadge party={t.party} />
                    </TableCell>
                    <TableCell>{t.majority == null ? '—' : t.majority ? 'Majority' : 'Minority'}</TableCell>
                    <TableCell class="text-right">{fmt(t.y ?? 0)}</TableCell>
                    <TableCell class="text-right">{fmt(t.n ?? 0)}</TableCell>
                    <TableCell class="text-right">{fmt(t.missed ?? 0)}</TableCell>
                  </TableRow>
                )}
              </For>
            </TableBody>
          </Table>
        </Section>
        <DataLinks kind="people" id={p().id} />
      </Container>
    );
  },
});
