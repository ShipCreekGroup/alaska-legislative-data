import { Link, createFileRoute } from '@tanstack/solid-router';
import { For } from 'solid-js';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '~/components/ui/table';
import { Breadcrumbs, Container, PageTitle, Pager, SearchBox } from '~/components/site';
import { fetchPeople } from '~/server/fns';
import { legYears, ordinal } from '~/lib/format';
import { isVariant, seo } from '~/lib/seo';
import { href, num, str } from '~/lib/search';

const crumbs = [
  { name: 'Home', path: '/' },
  { name: 'Legislators', path: '/legislators' },
  { name: 'All since 1959', path: '/people' },
];

export const Route = createFileRoute('/people/')({
  validateSearch: (s: Record<string, unknown>): { q?: string; page?: number } => ({ q: str(s.q), page: num(s.page) }),
  loaderDeps: ({ search }) => search,
  loader: ({ deps }) => fetchPeople({ data: deps }),
  head: ({ match }) =>
    seo({
      title: 'Every Alaska legislator since statehood',
      description:
        'An alphabetical list of every person who has served in the Alaska State Legislature since 1959, with each term and voting record.',
      path: '/people',
      crumbs,
      noindex: isVariant(match.search),
    }),
  component: () => {
    const d = Route.useLoaderData();
    const s = Route.useSearch();
    return (
      <Container>
        <Breadcrumbs crumbs={crumbs} />
        <PageTitle title="Every Alaska legislator since 1959">
          One page per person, covering every term they served in either chamber.
        </PageTitle>
        <div class="mb-4">
          <SearchBox q={s().q} placeholder="Search by name" />
        </div>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Served</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            <For each={d().items}>
              {(p) => (
                <TableRow>
                  <TableCell class="font-medium">
                    <Link to="/people/$personId" params={{ personId: p.id }}>
                      {p.name}
                    </Link>
                  </TableCell>
                  <TableCell class="text-muted-foreground">
                    {p.first
                      ? p.first === p.last
                        ? `${ordinal(p.first)} Legislature (${legYears(p.first)})`
                        : `${ordinal(p.first)}–${ordinal(p.last!)} Legislatures (${legYears(p.first).slice(0, 4)}–${legYears(p.last!).slice(5)})`
                      : ''}
                  </TableCell>
                </TableRow>
              )}
            </For>
          </TableBody>
        </Table>
        <Pager {...d()} noun="legislators" href={(page) => href('/people', { q: s().q, page })} />
      </Container>
    );
  },
});
