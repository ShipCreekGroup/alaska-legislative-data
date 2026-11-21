import { createFileRoute } from '@tanstack/solid-router';
import { For, Show } from 'solid-js';
import { Breadcrumbs, Container, PageTitle, Pager, SearchBox } from '~/components/site';
import { BillsTable } from '~/components/lists';
import { LegislatureSelect } from '~/components/legselect';
import { fetchBills, fetchMeta } from '~/server/fns';
import { fmt, legName, legYears, legFull } from '~/lib/format';
import { isVariant, seo } from '~/lib/seo';
import { href, num, str } from '~/lib/search';

export const Route = createFileRoute('/bills/')({
  validateSearch: (
    s: Record<string, unknown>,
  ): { legislature?: number; q?: string; subject?: string; page?: number } => ({
    legislature: num(s.legislature),
    q: str(s.q),
    subject: str(s.subject),
    page: num(s.page),
  }),
  loaderDeps: ({ search }) => search,
  loader: async ({ deps }) => {
    const meta = await fetchMeta();
    const legislature = deps.legislature ?? meta.legislatures[0]!.number;
    const list = await fetchBills({ data: { ...deps, legislature } });
    return { list, legislature, options: meta.legislatures.filter((l) => l.bills > 0).map((l) => l.number) };
  },
  head: ({ loaderData: d, match }) => {
    if (!d) return {};
    return seo({
      title: `Bills in the ${legName(d.legislature)} (${legYears(d.legislature)})`,
      description: `All ${fmt(d.list.total)} bills and resolutions introduced in the ${legFull(d.legislature)}, with titles, subjects, versions and floor votes.`,
      path: href('/bills', { legislature: match.search.legislature }),
      crumbs: [
        { name: 'Home', path: '/' },
        { name: 'Bills', path: '/bills' },
      ],
      noindex: isVariant({ q: match.search.q, subject: match.search.subject, page: match.search.page }),
    });
  },
  component: BillsPage,
});

function BillsPage() {
  const d = Route.useLoaderData();
  const s = Route.useSearch();
  return (
    <Container>
      <Breadcrumbs
        crumbs={[
          { name: 'Home', path: '/' },
          { name: 'Bills', path: '/bills' },
        ]}
      />
      <PageTitle title={`Bills in the ${legName(d().legislature)}`}>
        Bills and resolutions introduced in {legYears(d().legislature)}. Click a bill for its versions, full text and
        every floor vote on it.
      </PageTitle>
      <div class="mb-4">
        <SearchBox q={s().q} placeholder="Bill number, title or subject" hidden={{ subject: s().subject }}>
          <LegislatureSelect value={d().legislature} options={d().options} />
        </SearchBox>
      </div>
      <Show when={!s().subject && !s().q}>
        <details class="mb-4 text-sm">
          <summary class="cursor-pointer font-medium text-link">Browse by subject</summary>
          <div class="mt-2 flex flex-wrap gap-x-3 gap-y-1">
            <For each={d().list.subjects.slice(0, 80)}>
              {(sub) => <a href={href('/bills', { legislature: s().legislature, subject: sub })}>{sub}</a>}
            </For>
          </div>
        </details>
      </Show>
      <Show when={s().subject}>
        <p class="mb-3 text-sm">
          Subject: <b>{s().subject}</b> · <a href={href('/bills', { legislature: s().legislature, q: s().q })}>clear</a>
        </p>
      </Show>
      <BillsTable bills={d().list.items} />
      <Pager
        {...d().list}
        noun="bills"
        href={(page) => href('/bills', { legislature: s().legislature, q: s().q, subject: s().subject, page })}
      />
    </Container>
  );
}
