import { Link } from '@tanstack/solid-router';
import { For, Show } from 'solid-js';
import { Badge } from '~/components/ui/badge';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '~/components/ui/table';
import { ChoiceBadge, PartyBadge, TallyBar, TallyText } from '~/components/site';
import { chamberName, date, memberTitleShort, voteMotion } from '~/lib/format';
import type { BillRow, MemberRow, MemberVote, VoteRow } from '~/lib/types';

export function VoteCards(props: { votes: VoteRow[] }) {
  return (
    <div class="grid gap-2">
      <For each={props.votes}>
        {(v) => (
          <Link
            to="/votes/$voteId"
            params={{ voteId: v.id }}
            class="grid gap-1.5 rounded-lg border bg-card p-3 text-foreground no-underline hover:border-ring sm:grid-cols-[1fr_auto] sm:items-center sm:gap-4"
          >
            <div class="min-w-0">
              <div class="font-medium">
                <span class="text-link">{v.billNumber ?? 'Non-bill vote'}</span> {v.billTitle}
              </div>
              <div class="text-sm text-muted-foreground">
                {chamberName(v.chamber)} · {date(v.date)} · {voteMotion(v.title)}
              </div>
            </div>
            <div class="grid w-40 gap-1">
              <TallyText {...v} />
              <TallyBar {...v} />
            </div>
          </Link>
        )}
      </For>
    </div>
  );
}

export function VotesTable(props: { votes: VoteRow[]; showBill?: boolean }) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Date</TableHead>
          <TableHead>Chamber</TableHead>
          <Show when={props.showBill !== false}>
            <TableHead>Bill</TableHead>
          </Show>
          <TableHead>Motion</TableHead>
          <TableHead>Result</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        <For each={props.votes}>
          {(v) => (
            <TableRow>
              <TableCell class="whitespace-nowrap">{date(v.date, 'short')}</TableCell>
              <TableCell>{chamberName(v.chamber)}</TableCell>
              <Show when={props.showBill !== false}>
                <TableCell class="max-w-64">
                  <Show when={v.billId} fallback={<span class="text-muted-foreground">None</span>}>
                    <Link to="/bills/$billId" params={{ billId: v.billId! }} class="font-medium">
                      {v.billNumber}
                    </Link>{' '}
                    <span class="text-muted-foreground">{v.billTitle}</span>
                  </Show>
                </TableCell>
              </Show>
              <TableCell class="max-w-80">
                <Link to="/votes/$voteId" params={{ voteId: v.id }}>
                  {voteMotion(v.title)}
                </Link>
              </TableCell>
              <TableCell class="w-36">
                <div class="grid gap-1">
                  <TallyText {...v} />
                  <TallyBar {...v} />
                </div>
              </TableCell>
            </TableRow>
          )}
        </For>
      </TableBody>
    </Table>
  );
}

export function BillsTable(props: { bills: BillRow[] }) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Bill</TableHead>
          <TableHead>Title</TableHead>
          <TableHead>Subjects</TableHead>
          <TableHead>Last action</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        <For each={props.bills}>
          {(b) => (
            <TableRow>
              <TableCell class="whitespace-nowrap font-medium">
                <Link to="/bills/$billId" params={{ billId: b.id }}>
                  {b.number}
                </Link>
              </TableCell>
              <TableCell>
                {b.title}
                <Show when={b.vetoed}>
                  {' '}
                  <Badge variant="nay">Vetoed</Badge>
                </Show>
              </TableCell>
              <TableCell class="max-w-72 text-xs text-muted-foreground">
                {(b.subjects ?? []).slice(0, 4).join(' · ')}
              </TableCell>
              <TableCell class="whitespace-nowrap">{b.statusDate ? date(b.statusDate, 'short') : ''}</TableCell>
            </TableRow>
          )}
        </For>
      </TableBody>
    </Table>
  );
}

export function MembersTable(props: { members: MemberRow[] }) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Name</TableHead>
          <TableHead>Party</TableHead>
          <TableHead>District</TableHead>
          <TableHead>Caucus</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        <For each={props.members}>
          {(m) => (
            <TableRow>
              <TableCell class="font-medium">
                <Link to="/members/$memberId" params={{ memberId: m.id }}>
                  {memberTitleShort(m.chamber)} {m.name}
                </Link>
              </TableCell>
              <TableCell>
                <PartyBadge party={m.party} />
              </TableCell>
              <TableCell>{m.district ?? '—'}</TableCell>
              <TableCell>{m.majority === null ? '—' : m.majority ? 'Majority' : 'Minority'}</TableCell>
            </TableRow>
          )}
        </For>
      </TableBody>
    </Table>
  );
}

export function MemberVotesTable(props: { votes: MemberVote[] }) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Date</TableHead>
          <TableHead>Bill</TableHead>
          <TableHead>Motion</TableHead>
          <TableHead>Their vote</TableHead>
          <TableHead>Result</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        <For each={props.votes}>
          {(v) => (
            <TableRow>
              <TableCell class="whitespace-nowrap">{date(v.date, 'short')}</TableCell>
              <TableCell class="max-w-64">
                <Show when={v.billId} fallback={<span class="text-muted-foreground">None</span>}>
                  <Link to="/bills/$billId" params={{ billId: v.billId! }} class="font-medium">
                    {v.billNumber}
                  </Link>{' '}
                  <span class="text-muted-foreground">{v.billTitle}</span>
                </Show>
              </TableCell>
              <TableCell class="max-w-72">
                <Link to="/votes/$voteId" params={{ voteId: v.id }}>
                  {voteMotion(v.title)}
                </Link>
              </TableCell>
              <TableCell>
                <ChoiceBadge choice={v.choice} />
              </TableCell>
              <TableCell>
                <TallyText y={v.y} n={v.n} e={0} a={0} />
              </TableCell>
            </TableRow>
          )}
        </For>
      </TableBody>
    </Table>
  );
}
