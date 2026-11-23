import { Link, createFileRoute } from '@tanstack/solid-router';
import { Show } from 'solid-js';
import { Breadcrumbs, Container, PageTitle, SearchBox, Section } from '~/components/site';
import { MembersTable } from '~/components/lists';
import { LegislatureSelect } from '~/components/legselect';
import { fetchMembers, fetchMeta } from '~/server/fns';
import { legName, legYears } from '~/lib/format';
import { isVariant, seo } from '~/lib/seo';
import { href, num, str } from '~/lib/search';

export const Route = createFileRoute('/legislators/')({
  validateSearch: (s: Record<string, unknown>): { legislature?: number; q?: string } => ({
    legislature: num(s.legislature),
    q: str(s.q),
  }),
  loaderDeps: ({ search }) => search,
  loader: async ({ deps }) => {
    const meta = await fetchMeta();
    const legislature = deps.legislature ?? meta.legislatures[0]!.number;
    const members = await fetchMembers({ data: { ...deps, legislature } });
    return { members, legislature, options: meta.legislatures.filter((l) => l.members > 0).map((l) => l.number) };
  },
  head: ({ loaderData: d, match }) => {
    if (!d) return {};
    return seo({
      title: `Alaska legislators in the ${legName(d.legislature)} (${legYears(d.legislature)})`,
      description: `Every Alaska state representative and senator in the ${legName(d.legislature)}, with party, district, caucus and full voting record.`,
      path: href('/legislators', { legislature: match.search.legislature }),
      crumbs: [
        { name: 'Home', path: '/' },
        { name: 'Legislators', path: '/legislators' },
      ],
      noindex: isVariant({ q: match.search.q }),
    });
  },
  component: () => {
    const d = Route.useLoaderData();
    const s = Route.useSearch();
    const by = (c: string) => d().members.filter((m) => m.chamber === c);
    return (
      <Container>
        <Breadcrumbs
          crumbs={[
            { name: 'Home', path: '/' },
            { name: 'Legislators', path: '/legislators' },
          ]}
        />
        <PageTitle title={`Legislators in the ${legName(d().legislature)}`}>
          Representatives and senators who served in {legYears(d().legislature)}. For a legislator's whole career, see{' '}
          <Link to="/people">all legislators since 1959</Link>.
        </PageTitle>
        <div class="mb-2">
          <SearchBox q={s().q} placeholder="Name, district or code">
            <LegislatureSelect value={d().legislature} options={d().options} />
          </SearchBox>
        </div>
        <Show when={by('H').length}>
          <Section title={`House (${by('H').length})`}>
            <MembersTable members={by('H')} />
          </Section>
        </Show>
        <Show when={by('S').length}>
          <Section title={`Senate (${by('S').length})`}>
            <MembersTable members={by('S')} />
          </Section>
        </Show>
        <Show when={!d().members.length}>
          <p class="text-muted-foreground">
            No legislators match. <a href={href('/legislators', { legislature: s().legislature })}>Clear the search</a>
          </p>
        </Show>
      </Container>
    );
  },
});
