import { Link, createFileRoute } from '@tanstack/solid-router';
import { For } from 'solid-js';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '~/components/ui/table';
import { Breadcrumbs, Container, PageTitle } from '~/components/site';
import { fetchMeta } from '~/server/fns';
import { fmt, legName, legYears } from '~/lib/format';
import { seo } from '~/lib/seo';

const crumbs = [
  { name: 'Home', path: '/' },
  { name: 'Legislatures', path: '/legislatures' },
];

export const Route = createFileRoute('/legislatures/')({
  loader: () => fetchMeta(),
  head: () =>
    seo({
      title: 'All Alaska Legislatures since statehood',
      description:
        'Every Alaska State Legislature with its bills, roll-call votes and members, from the 1st Legislature (1959) to today.',
      path: '/legislatures',
      crumbs,
    }),
  component: () => {
    const meta = Route.useLoaderData();
    return (
      <Container>
        <Breadcrumbs crumbs={crumbs} />
        <PageTitle title="Alaska Legislatures">
          Each legislature serves two years. Bills are recorded from the 12th Legislature (1981) on, and roll-call votes
          are most complete from the 19th (1995) on.
        </PageTitle>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Legislature</TableHead>
              <TableHead>Years</TableHead>
              <TableHead class="text-right">Bills</TableHead>
              <TableHead class="text-right">Roll-call votes</TableHead>
              <TableHead class="text-right">Members</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            <For each={meta().legislatures}>
              {(l) => (
                <TableRow>
                  <TableCell class="font-medium">
                    <Link to="/legislatures/$legislature" params={{ legislature: String(l.number) }}>
                      {legName(l.number)}
                    </Link>
                  </TableCell>
                  <TableCell>{legYears(l.number)}</TableCell>
                  <TableCell class="text-right">{fmt(l.bills)}</TableCell>
                  <TableCell class="text-right">{fmt(l.votes)}</TableCell>
                  <TableCell class="text-right">{fmt(l.members)}</TableCell>
                </TableRow>
              )}
            </For>
          </TableBody>
        </Table>
      </Container>
    );
  },
});
