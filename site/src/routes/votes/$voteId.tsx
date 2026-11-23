import { Link, createFileRoute } from '@tanstack/solid-router';
import { For, Show } from 'solid-js';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '~/components/ui/table';
import { Breadcrumbs, Container, DataLinks, PageTitle, PartyBadge, Section, TallyBar } from '~/components/site';
import { fetchVote } from '~/server/fns';
import {
  CHOICE,
  SITE_URL,
  chamberName,
  clip,
  date,
  legName,
  memberTitleShort,
  partyPlural,
  voteMotion,
  voteVersion,
} from '~/lib/format';
import { byParty, voteSummary as summary } from '~/lib/summaries';
import { seo } from '~/lib/seo';
import type { Choice, Vote } from '~/lib/types';
import { href } from '~/lib/search';

const crumbsFor = (v: Vote) => [
  { name: 'Home', path: '/' },
  { name: legName(v.legislature), path: `/legislatures/${v.legislature}` },
  { name: 'Votes', path: href('/votes', { legislature: v.legislature }) },
  { name: `${chamberName(v.chamber)} vote ${v.number}`, path: `/votes/${encodeURIComponent(v.id)}` },
];

export const Route = createFileRoute('/votes/$voteId')({
  loader: ({ params }) => fetchVote({ data: { id: params.voteId } }),
  head: ({ loaderData: v }) => {
    if (!v) return {};
    return seo({
      title: `${chamberName(v.chamber)} vote on ${v.billNumber ?? voteMotion(v.title)}, ${date(v.date, 'short')}: ${v.y}–${v.n}`,
      description: clip(summary(v)),
      path: `/votes/${encodeURIComponent(v.id)}`,
      crumbs: crumbsFor(v),
      markdown: true,
      jsonLd: {
        '@type': 'WebPage',
        name: `Alaska ${chamberName(v.chamber)} roll-call vote ${v.number}, ${legName(v.legislature)}`,
        datePublished: v.date ?? undefined,
        about: v.billId
          ? {
              '@type': 'Legislation',
              name: `${v.billNumber}: ${v.billTitle ?? ''}`,
              url: `${SITE_URL}/bills/${encodeURIComponent(v.billId)}`,
            }
          : undefined,
      },
    });
  },
  component: VotePage,
});

const ORDER: Choice[] = ['Y', 'N', 'E', 'A'];

function VotePage() {
  const v = Route.useLoaderData();
  const group = (c: Choice) => v().choices.filter((x) => x.choice === c);
  return (
    <Container>
      <Breadcrumbs crumbs={crumbsFor(v())} />
      <PageTitle
        eyebrow={`${chamberName(v().chamber)} roll call ${v().number} · ${date(v().date)}`}
        title={
          <>
            <Show when={v().billId} fallback={voteMotion(v().title)}>
              {v().billNumber}: {voteMotion(v().title)}
            </Show>
          </>
        }
      >
        {summary(v())}
      </PageTitle>
      <div class="grid max-w-3xl gap-3 rounded-xl border bg-card p-5">
        <div class="flex flex-wrap gap-6 font-display text-lg font-bold tabular-nums">
          <span class="text-yea">{v().y} Yea</span>
          <span class="text-nay">{v().n} Nay</span>
          <span class="text-muted-foreground">{v().e} Excused</span>
          <span class="text-muted-foreground">{v().a} Absent</span>
        </div>
        <TallyBar {...v()} class="h-3" />
        <dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-sm">
          <Show when={v().billId}>
            <dt class="text-muted-foreground">Bill</dt>
            <dd>
              <Link to="/bills/$billId" params={{ billId: v().billId! }}>
                {v().billNumber}
              </Link>{' '}
              {v().billTitle}
            </dd>
          </Show>
          <Show when={voteVersion(v().title)}>
            <dt class="text-muted-foreground">Version voted on</dt>
            <dd>{voteVersion(v().title)}</dd>
          </Show>
          <Show when={v().amendment}>
            <dt class="text-muted-foreground">Amendment</dt>
            <dd>
              No. {v().amendment}
              <Show when={v().subAmendment}> (sub-amendment {v().subAmendment})</Show>
            </dd>
          </Show>
          <Show when={v().description}>
            <dt class="text-muted-foreground">Description</dt>
            <dd>{v().description}</dd>
          </Show>
        </dl>
      </div>

      <Section title="By party">
        <div class="max-w-xl">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Party</TableHead>
                <For each={ORDER}>{(c) => <TableHead class="text-right">{CHOICE[c]}</TableHead>}</For>
              </TableRow>
            </TableHeader>
            <TableBody>
              <For each={byParty(v())}>
                {([p, r]) => (
                  <TableRow>
                    <TableCell>{partyPlural(p === '?' ? null : p)}</TableCell>
                    <For each={ORDER}>{(c) => <TableCell class="text-right">{r[c]}</TableCell>}</For>
                  </TableRow>
                )}
              </For>
            </TableBody>
          </Table>
        </div>
      </Section>

      <Section title="How each legislator voted">
        <div class="grid gap-4 md:grid-cols-2">
          <For each={ORDER.filter((c) => group(c).length)}>
            {(c) => (
              <div class="rounded-xl border bg-card p-4">
                <h3
                  class={`mb-2 font-bold ${c === 'Y' ? 'text-yea' : c === 'N' ? 'text-nay' : 'text-muted-foreground'}`}
                >
                  {CHOICE[c]} ({group(c).length})
                </h3>
                <ul class="grid gap-1 text-sm sm:grid-cols-2">
                  <For each={group(c)}>
                    {(m) => (
                      <li class="flex items-center gap-2">
                        <PartyBadge party={m.party} />
                        <Link to="/members/$memberId" params={{ memberId: m.memberId }}>
                          {memberTitleShort(v().chamber)} {m.name}
                        </Link>
                        <span class="text-xs text-muted-foreground">{m.district}</span>
                      </li>
                    )}
                  </For>
                </ul>
              </div>
            )}
          </For>
        </div>
      </Section>
      <DataLinks kind="votes" id={v().id} />
    </Container>
  );
}
