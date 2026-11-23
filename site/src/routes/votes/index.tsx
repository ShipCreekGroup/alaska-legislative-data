import { createFileRoute } from '@tanstack/solid-router';
import { Breadcrumbs, Container, PageTitle, Pager, SearchBox } from '~/components/site';
import { VotesTable } from '~/components/lists';
import { ChamberSelect, LegislatureSelect } from '~/components/legselect';
import { fetchMeta, fetchVotes } from '~/server/fns';
import { chamberName, fmt, legName, legYears, legFull } from '~/lib/format';
import { isVariant, seo } from '~/lib/seo';
import { href, num, str } from '~/lib/search';

export const Route = createFileRoute('/votes/')({
  validateSearch: (
    s: Record<string, unknown>,
  ): { legislature?: number; chamber?: string; q?: string; page?: number } => ({
    legislature: num(s.legislature),
    chamber: str(s.chamber),
    q: str(s.q),
    page: num(s.page),
  }),
  loaderDeps: ({ search }) => search,
  loader: async ({ deps }) => {
    const meta = await fetchMeta();
    const legislature = deps.legislature ?? meta.legislatures[0]!.number;
    const list = await fetchVotes({ data: { ...deps, legislature } });
    return { list, legislature, options: meta.legislatures.filter((l) => l.votes > 0).map((l) => l.number) };
  },
  head: ({ loaderData: d, match }) => {
    if (!d) return {};
    return seo({
      title: `Roll-call votes in the ${legName(d.legislature)} (${legYears(d.legislature)})`,
      description: `${fmt(d.list.total)} House and Senate floor votes in the ${legFull(d.legislature)}, with how each legislator voted.`,
      path: href('/votes', { legislature: match.search.legislature }),
      crumbs: [
        { name: 'Home', path: '/' },
        { name: 'Votes', path: '/votes' },
      ],
      noindex: isVariant({ q: match.search.q, chamber: match.search.chamber, page: match.search.page }),
    });
  },
  component: VotesPage,
});

function VotesPage() {
  const d = Route.useLoaderData();
  const s = Route.useSearch();
  return (
    <Container>
      <Breadcrumbs
        crumbs={[
          { name: 'Home', path: '/' },
          { name: 'Votes', path: '/votes' },
        ]}
      />
      <PageTitle title={`Roll-call votes in the ${legName(d().legislature)}`}>
        {s().chamber ? `${chamberName(s().chamber!)} floor` : 'House and Senate floor'} votes, newest first. Click a
        vote to see how each legislator voted.
      </PageTitle>
      <div class="mb-4">
        <SearchBox q={s().q} placeholder="Bill number or motion">
          <LegislatureSelect value={d().legislature} options={d().options} />
          <ChamberSelect value={s().chamber} />
        </SearchBox>
      </div>
      <VotesTable votes={d().list.items} />
      <Pager
        {...d().list}
        noun="votes"
        href={(page) => href('/votes', { legislature: s().legislature, chamber: s().chamber, q: s().q, page })}
      />
    </Container>
  );
}
