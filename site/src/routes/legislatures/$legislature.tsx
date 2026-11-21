import { Link, createFileRoute, notFound } from '@tanstack/solid-router';
import { Breadcrumbs, Container, PageTitle, Section, Stat } from '~/components/site';
import { MembersTable, VoteCards } from '~/components/lists';
import { fetchLegislatureSummary } from '~/server/fns';
import { fmt, legName, legYears, legFull } from '~/lib/format';
import { seo } from '~/lib/seo';
import { num } from '~/lib/search';

export const Route = createFileRoute('/legislatures/$legislature')({
  loader: ({ params }) => {
    const n = num(params.legislature);
    if (!n) throw notFound();
    return fetchLegislatureSummary({ data: { legislature: n } });
  },
  head: ({ loaderData: d }) => {
    if (!d) return {};
    const name = `${legName(d.number)} (${legYears(d.number)})`;
    return seo({
      title: `${name}, Alaska State Legislature`,
      description: `The ${name} Alaska State Legislature: ${fmt(d.counts.bills)} bills, ${fmt(d.counts.votes)} roll-call votes and ${d.counts.members} members, with how each legislator voted.`,
      path: `/legislatures/${d.number}`,
      crumbs: crumbsFor(d.number),
      markdown: true,
      jsonLd: { '@type': 'GovernmentOrganization', name: `${name}, Alaska State Legislature`, areaServed: 'Alaska' },
    });
  },
  component: LegislaturePage,
});

const crumbsFor = (n: number) => [
  { name: 'Home', path: '/' },
  { name: 'Legislatures', path: '/legislatures' },
  { name: legName(n), path: `/legislatures/${n}` },
];

function LegislaturePage() {
  const d = Route.useLoaderData();
  const house = () => d().members.filter((m) => m.chamber === 'H');
  const senate = () => d().members.filter((m) => m.chamber === 'S');
  const leg = () => d().number;
  return (
    <Container>
      <Breadcrumbs crumbs={crumbsFor(leg())} />
      <PageTitle eyebrow={legYears(leg())} title={`${legFull(leg())}`}>
        The {legName(leg())} met in {legYears(leg())}. It considered {fmt(d().counts.bills)} bills and recorded{' '}
        {fmt(d().counts.votes)} roll-call votes on the House and Senate floors.
      </PageTitle>
      <div class="flex flex-wrap gap-8 rounded-xl border bg-card p-5">
        <Stat
          label="bills"
          value={
            <Link to="/bills" search={{ legislature: leg() }}>
              {fmt(d().counts.bills)}
            </Link>
          }
        />
        <Stat
          label="roll-call votes"
          value={
            <Link to="/votes" search={{ legislature: leg() }}>
              {fmt(d().counts.votes)}
            </Link>
          }
        />
        <Stat
          label="members"
          value={
            <Link to="/legislators" search={{ legislature: leg() }}>
              {d().counts.members}
            </Link>
          }
        />
      </div>
      <Section
        title="Closest votes"
        aside={
          <Link to="/votes" search={{ legislature: leg() }}>
            All votes →
          </Link>
        }
      >
        <VoteCards votes={d().closeVotes} />
      </Section>
      <Section title={`House (${house().length})`}>
        <MembersTable members={house()} />
      </Section>
      <Section title={`Senate (${senate().length})`}>
        <MembersTable members={senate()} />
      </Section>
    </Container>
  );
}
