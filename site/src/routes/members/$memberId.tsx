import { Link, createFileRoute } from '@tanstack/solid-router';
import { For, Show } from 'solid-js';
import { Breadcrumbs, Container, DataLinks, PageTitle, Pager, SearchBox, Section, Stat } from '~/components/site';
import { MemberVotesTable } from '~/components/lists';
import { fetchMember } from '~/server/fns';
import {
  SITE_URL,
  akleg,
  chamberName,
  clip,
  fmt,
  legName,
  legYears,
  memberTitle,
  partyName,
  legFull,
} from '~/lib/format';
import { memberSummary as summary } from '~/lib/summaries';
import { isVariant, seo } from '~/lib/seo';
import { href, num, str } from '~/lib/search';

type M = Awaited<ReturnType<typeof fetchMember>>;

const path = (m: { id: string }) => `/members/${encodeURIComponent(m.id)}`;
const crumbsFor = (m: M) => [
  { name: 'Home', path: '/' },
  { name: legName(m.legislature), path: `/legislatures/${m.legislature}` },
  { name: 'Legislators', path: href('/legislators', { legislature: m.legislature }) },
  { name: m.name, path: path(m) },
];

export const Route = createFileRoute('/members/$memberId')({
  validateSearch: (s: Record<string, unknown>): { q?: string; choice?: string; page?: number } => ({
    q: str(s.q),
    choice: str(s.choice),
    page: num(s.page),
  }),
  loaderDeps: ({ search }) => search,
  loader: ({ params, deps }) => fetchMember({ data: { id: params.memberId, ...deps } }),
  head: ({ loaderData: m, match }) => {
    if (!m) return {};
    return seo({
      title: `${memberTitle(m.chamber)} ${m.name} (${m.party ?? '?'}-${m.district ?? '?'}) voting record, ${legName(m.legislature)}`,
      description: clip(summary(m)),
      path: path(m),
      crumbs: crumbsFor(m),
      markdown: true,
      noindex: isVariant(match.search),
      jsonLd: {
        '@type': 'Person',
        name: m.name,
        givenName: m.firstName,
        familyName: m.lastName,
        jobTitle: `Alaska State ${memberTitle(m.chamber)}, District ${m.district ?? '?'} (${legName(m.legislature)})`,
        affiliation: m.party ? { '@type': 'PoliticalParty', name: partyName(m.party) } : undefined,
        memberOf: {
          '@type': 'GovernmentOrganization',
          name: m.chamber === 'H' ? 'Alaska House of Representatives' : 'Alaska Senate',
        },
        mainEntityOfPage: `${SITE_URL}/people/${encodeURIComponent(m.personId)}`,
      },
    });
  },
  component: MemberPage,
});

function MemberPage() {
  const m = Route.useLoaderData();
  const s = Route.useSearch();
  const total = () => m().y + m().n + m().e + m().a;
  const filters: [string | undefined, string][] = [
    [undefined, 'All'],
    ['Y', 'Yea'],
    ['N', 'Nay'],
    ['A', 'Absent'],
    ['E', 'Excused'],
  ];
  return (
    <Container>
      <Breadcrumbs crumbs={crumbsFor(m())} />
      <PageTitle
        eyebrow={`${legName(m().legislature)} · ${legYears(m().legislature)}`}
        title={`${memberTitle(m().chamber)} ${m().name}`}
      >
        {summary(m())}
      </PageTitle>
      <div class="flex flex-wrap gap-8 rounded-xl border bg-card p-5">
        <Stat label="party" value={partyName(m().party)} />
        <Stat label={`${chamberName(m().chamber)} district`} value={m().district ?? '—'} />
        <Stat label="yea votes" value={fmt(m().y)} />
        <Stat label="nay votes" value={fmt(m().n)} />
        <Stat
          label="missed"
          value={total() ? `${fmt(m().e + m().a)} (${((100 * (m().e + m().a)) / total()).toFixed(1)}%)` : '—'}
        />
      </div>
      <p class="mt-4 flex flex-wrap gap-4 text-sm">
        <Link to="/people/$personId" params={{ personId: m().personId }}>
          {m().name}'s full career →
        </Link>
        <Show when={m().code}>
          <a href={akleg.legislator(m().legislature, m().code!)} rel="external">
            Official page on akleg.gov ↗
          </a>
        </Show>
      </p>
      <Show when={(m().otherTerms ?? []).length > 1}>
        <p class="mt-2 text-sm text-muted-foreground">
          Other terms:{' '}
          <For each={(m().otherTerms ?? []).filter((t) => t.id !== m().id)}>
            {(t, i) => (
              <>
                {i() > 0 ? ' · ' : ''}
                <Link to="/members/$memberId" params={{ memberId: t.id }}>
                  {legName(t.legislature)} ({chamberName(t.chamber)})
                </Link>
              </>
            )}
          </For>
        </p>
      </Show>

      <Section title="Voting record">
        <div class="flex flex-wrap items-center justify-between gap-3">
          <div class="flex flex-wrap gap-1 text-sm">
            <For each={filters}>
              {([c, label]) => (
                <a
                  href={href(path(m()), { choice: c, q: s().q })}
                  class={`rounded-md px-2.5 py-1 no-underline ${s().choice === c ? 'bg-primary text-primary-foreground' : 'text-foreground hover:bg-accent'}`}
                >
                  {label}
                </a>
              )}
            </For>
          </div>
          <SearchBox q={s().q} placeholder="Bill number or title" hidden={{ choice: s().choice }} />
        </div>
        <MemberVotesTable votes={m().votesPage.items} />
        <Pager
          {...m().votesPage}
          noun="votes"
          href={(page) => href(path(m()), { choice: s().choice, q: s().q, page })}
        />
      </Section>
      <DataLinks kind="members" id={m().id} />
    </Container>
  );
}
