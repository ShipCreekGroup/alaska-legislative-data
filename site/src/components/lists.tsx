// Lists and tables. The tables can be 100+ rows, so rows are plain elements with short class
// names (see .dt in styles.css) and plain links; the root route turns internal link clicks into
// client-side navigation.
import { For, Show } from 'solid-js';
import { TallyBar, TallyText } from '~/components/site';
import { CHOICE, chamberName, date, memberTitleShort, partyName, voteMotion } from '~/lib/format';
import { paths } from '~/lib/paths';
import type { BillRow, MemberRow, MemberVote, VoteRow } from '~/lib/types';

const tally = (y: number, n: number) =>
  `<span class="ty">${y}</span><span class="mu">–</span><span class="tn">${n}</span>`;
const esc = (s: string | null | undefined) =>
  (s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;');

export function VoteCards(props: { votes: VoteRow[] }) {
  return (
    <div class="grid gap-2">
      <For each={props.votes}>
        {(v) => (
          <a
            href={paths.vote(v.id)}
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
          </a>
        )}
      </For>
    </div>
  );
}

/*
 * The row bodies below are built as HTML strings and inserted with innerHTML. Solid then treats
 * each table body as one static block, so a 100-row table costs no hydration markers.
 * Every value goes through esc().
 */

function billCell(id: string | null | undefined, number: string | null | undefined, title: string | null | undefined) {
  return id
    ? `<a class="b" href="${paths.bill(id)}">${esc(number)}</a> <span class="mu">${esc(title)}</span>`
    : '<span class="mu">None</span>';
}

export function VotesTable(props: { votes: VoteRow[]; showBill?: boolean }) {
  const rows = () =>
    props.votes
      .map(
        (v) =>
          `<tr><td class="nw">${date(v.date, 'short')}</td><td>${chamberName(v.chamber)}</td>${
            props.showBill === false ? '' : `<td>${billCell(v.billId, v.billNumber, v.billTitle)}</td>`
          }<td><a href="${paths.vote(v.id)}">${esc(voteMotion(v.title))}</a></td><td class="nw">${tally(v.y, v.n)}</td></tr>`,
      )
      .join('');
  return (
    <div class="dt-wrap">
      <table class="dt">
        <thead>
          <tr>
            <th>Date</th>
            <th>Chamber</th>
            <Show when={props.showBill !== false}>
              <th>Bill</th>
            </Show>
            <th>Motion</th>
            <th>Yea–Nay</th>
          </tr>
        </thead>
        <tbody innerHTML={rows()} />
      </table>
    </div>
  );
}

export function BillsTable(props: { bills: BillRow[] }) {
  const rows = () =>
    props.bills
      .map(
        (b) =>
          `<tr><td class="nw b"><a href="${paths.bill(b.id)}">${esc(b.number)}</a></td><td>${esc(b.title)}${
            b.vetoed ? ' <span class="ch ch-N">Vetoed</span>' : ''
          }</td><td class="mu text-xs">${esc((b.subjects ?? []).slice(0, 4).join(' · '))}</td><td class="nw">${
            b.statusDate ? date(b.statusDate, 'short') : ''
          }</td></tr>`,
      )
      .join('');
  return (
    <div class="dt-wrap">
      <table class="dt">
        <thead>
          <tr>
            <th>Bill</th>
            <th>Title</th>
            <th>Subjects</th>
            <th>Last action</th>
          </tr>
        </thead>
        <tbody innerHTML={rows()} />
      </table>
    </div>
  );
}

export function MembersTable(props: { members: MemberRow[] }) {
  const rows = () =>
    props.members
      .map(
        (m) =>
          `<tr><td class="b"><a href="${paths.member(m.id)}">${memberTitleShort(m.chamber)} ${esc(m.name)}</a></td><td><span class="pb" title="${esc(
            partyName(m.party),
          )}">${esc(m.party ?? '?')}</span></td><td>${esc(m.district ?? '—')}</td><td>${
            m.majority === null ? '—' : m.majority ? 'Majority' : 'Minority'
          }</td></tr>`,
      )
      .join('');
  return (
    <div class="dt-wrap">
      <table class="dt">
        <thead>
          <tr>
            <th>Name</th>
            <th>Party</th>
            <th>District</th>
            <th>Caucus</th>
          </tr>
        </thead>
        <tbody innerHTML={rows()} />
      </table>
    </div>
  );
}

export function MemberVotesTable(props: { votes: MemberVote[] }) {
  const rows = () =>
    props.votes
      .map(
        (v) =>
          `<tr><td class="nw">${date(v.date, 'short')}</td><td>${billCell(v.billId, v.billNumber, v.billTitle)}</td><td><a href="${paths.vote(
            v.id,
          )}">${esc(voteMotion(v.title))}</a></td><td><span class="ch ch-${v.choice}">${CHOICE[v.choice]}</span></td><td class="nw">${tally(
            v.y,
            v.n,
          )}</td></tr>`,
      )
      .join('');
  return (
    <div class="dt-wrap">
      <table class="dt">
        <thead>
          <tr>
            <th>Date</th>
            <th>Bill</th>
            <th>Motion</th>
            <th>Their vote</th>
            <th>Yea–Nay</th>
          </tr>
        </thead>
        <tbody innerHTML={rows()} />
      </table>
    </div>
  );
}
