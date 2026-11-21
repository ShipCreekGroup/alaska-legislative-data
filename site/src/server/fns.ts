// Server functions: the only way pages get data. They run on the server during SSR and are called
// over HTTP during client navigation.
import { createServerFn } from '@tanstack/solid-start';
import { notFound } from '@tanstack/solid-router';
import { getDoc, getMeta, getPeopleIndex } from './store';
import type { Bill, BillVersion, Legislature, Member, Meta, Page, Person, PersonIndexRow, Vote } from '~/lib/types';

export const PAGE_SIZE = 100;

function paginate<T>(items: T[], page: number, size = PAGE_SIZE): Page<T> {
  const pages = Math.max(1, Math.ceil(items.length / size));
  const p = Math.min(Math.max(1, page), pages);
  return { items: items.slice((p - 1) * size, p * size), total: items.length, page: p, pages };
}

const matches = (q: string | undefined, ...fields: (string | null | undefined)[]) => {
  if (!q) return true;
  const needle = q.toLowerCase();
  return fields.some((f) => f && f.toLowerCase().includes(needle));
};

async function must<T>(p: Promise<T | null>): Promise<T> {
  const v = await p;
  if (v === null) throw notFound();
  return v;
}

export const fetchMeta = createServerFn({ method: 'GET' }).handler(() => must(getMeta<Meta>()));

type ListInput = { legislature: number; page?: number; q?: string; chamber?: string };

export const fetchLegislatureSummary = createServerFn({ method: 'GET' })
  .validator((d: { legislature: number }) => d)
  .handler(async ({ data }) => {
    const leg = await must(getDoc<Legislature>('legislatures', data.legislature));
    const meta = await must(getMeta<Meta>());
    return {
      number: leg.number,
      counts: { bills: leg.bills.length, votes: leg.votes.length, members: leg.members.length },
      members: leg.members,
      recentVotes: leg.votes.slice(0, 10),
      closeVotes: [...leg.votes]
        .filter((v) => v.y + v.n >= 15 && v.n > 0)
        .sort((a, b) => Math.abs(a.y - a.n) - Math.abs(b.y - b.n))
        .slice(0, 10),
      legislatures: meta.legislatures.map((l) => l.number),
    };
  });

export const fetchBills = createServerFn({ method: 'GET' })
  .validator((d: ListInput & { subject?: string }) => d)
  .handler(async ({ data }) => {
    const leg = await must(getDoc<Legislature>('legislatures', data.legislature));
    const subjects = new Map<string, number>();
    for (const b of leg.bills) for (const s of b.subjects ?? []) subjects.set(s, (subjects.get(s) ?? 0) + 1);
    const rows = leg.bills.filter(
      (b) =>
        matches(data.q, b.number, b.title, ...(b.subjects ?? [])) &&
        (!data.subject || b.subjects?.includes(data.subject)),
    );
    return {
      ...paginate(rows, data.page ?? 1),
      subjects: [...subjects.entries()].sort((a, b) => b[1] - a[1]).map(([s]) => s),
    };
  });

export const fetchVotes = createServerFn({ method: 'GET' })
  .validator((d: ListInput) => d)
  .handler(async ({ data }) => {
    const leg = await must(getDoc<Legislature>('legislatures', data.legislature));
    const rows = leg.votes.filter(
      (v) =>
        (!data.chamber || v.chamber === data.chamber) &&
        matches(data.q, v.billNumber, v.billTitle, v.title, v.description),
    );
    return paginate(rows, data.page ?? 1);
  });

export const fetchMembers = createServerFn({ method: 'GET' })
  .validator((d: ListInput) => d)
  .handler(async ({ data }) => {
    const leg = await must(getDoc<Legislature>('legislatures', data.legislature));
    return leg.members.filter(
      (m) => (!data.chamber || m.chamber === data.chamber) && matches(data.q, m.name, m.district, m.code),
    );
  });

export const fetchBill = createServerFn({ method: 'GET' })
  .validator((d: { id: string }) => d)
  .handler(({ data }) => must(getDoc<Bill>('bills', data.id)));

export const fetchBillVersion = createServerFn({ method: 'GET' })
  .validator((d: { id: string }) => d)
  .handler(({ data }) => must(getDoc<BillVersion>('bill-versions', data.id)));

export const fetchVote = createServerFn({ method: 'GET' })
  .validator((d: { id: string }) => d)
  .handler(({ data }) => must(getDoc<Vote>('votes', data.id)));

export const fetchMember = createServerFn({ method: 'GET' })
  .validator((d: { id: string; page?: number; q?: string; choice?: string }) => d)
  .handler(async ({ data }) => {
    const m = await must(getDoc<Member>('members', data.id));
    const rows = m.votes.filter(
      (v) => (!data.choice || v.choice === data.choice) && matches(data.q, v.billNumber, v.billTitle, v.title),
    );
    const { votes: _all, ...rest } = m;
    return { ...rest, votesPage: paginate(rows, data.page ?? 1) };
  });

export const fetchPerson = createServerFn({ method: 'GET' })
  .validator((d: { id: string }) => d)
  .handler(({ data }) => must(getDoc<Person>('people', data.id)));

export const fetchPeople = createServerFn({ method: 'GET' })
  .validator((d: { q?: string; page?: number }) => d)
  .handler(async ({ data }) => {
    const all = await must(getPeopleIndex<PersonIndexRow[]>());
    return paginate(
      all.filter((p) => matches(data.q, p.name)),
      data.page ?? 1,
      200,
    );
  });
