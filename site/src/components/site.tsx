import { Link, useNavigate } from '@tanstack/solid-router';
import { For, Show, type JSX } from 'solid-js';
import { Badge } from '~/components/ui/badge';
import { buttonVariants } from '~/components/ui/button';
import { Input } from '~/components/ui/input';
import { CHOICE, fmt, partyName } from '~/lib/format';
import type { Crumb } from '~/lib/seo';
import type { Choice, Tally } from '~/lib/types';
import { cn } from '~/lib/utils';

export function SiteHeader() {
  const nav = [
    ['/legislatures', 'Legislatures'],
    ['/bills', 'Bills'],
    ['/votes', 'Votes'],
    ['/legislators', 'Legislators'],
    ['/ask', 'Ask AI'],
    ['/developers', 'Data & API'],
  ] as const;
  return (
    <header class="bg-[oklch(0.25_0.07_255)] text-white">
      <div class="mx-auto flex max-w-6xl flex-wrap items-center gap-x-6 gap-y-2 px-4 py-3">
        <Link to="/" class="font-display text-lg font-extrabold text-white no-underline">
          Alaska Legislative <span class="text-gold">Data</span>
        </Link>
        <nav class="flex flex-wrap gap-1 text-sm" aria-label="Main">
          <For each={nav}>
            {([to, label]) => (
              <Link
                to={to}
                class="rounded-md px-2.5 py-1.5 font-medium text-white/80 no-underline hover:bg-white/10 hover:text-white"
                activeProps={{ class: 'bg-white/10 text-white' }}
              >
                {label}
              </Link>
            )}
          </For>
        </nav>
      </div>
    </header>
  );
}

export function SiteFooter(props: { updated?: string }) {
  return (
    <footer class="mt-16 border-t">
      <div class="mx-auto flex max-w-6xl flex-wrap justify-between gap-3 px-4 py-6 text-sm text-muted-foreground">
        <span>
          Data from the Alaska State Legislature, cleaned and published nightly by Ship Creek Group, Anchorage. MIT
          license.
          <Show when={props.updated}> Last updated {props.updated}.</Show>
        </span>
        <span class="flex gap-4">
          <a href="https://github.com/ShipCreekGroup/alaska-legislative-data">GitHub</a>
          <a href="https://github.com/ShipCreekGroup/alaska-legislative-data/issues">Report a problem</a>
          <a href="/llms.txt">llms.txt</a>
        </span>
      </div>
    </footer>
  );
}

export function Container(props: { children: JSX.Element; class?: string }) {
  return <main class={cn('mx-auto max-w-6xl px-4 py-8', props.class)}>{props.children}</main>;
}

export function Breadcrumbs(props: { crumbs: Crumb[] }) {
  return (
    <nav aria-label="Breadcrumb" class="mb-3 text-sm text-muted-foreground">
      <ol class="flex flex-wrap items-center gap-1.5">
        <For each={props.crumbs}>
          {(c, i) => (
            <li class="flex items-center gap-1.5">
              <Show when={i() > 0}>
                <span aria-hidden="true">/</span>
              </Show>
              <Show
                when={i() < props.crumbs.length - 1}
                fallback={
                  <span aria-current="page" class="text-foreground">
                    {c.name}
                  </span>
                }
              >
                <a href={c.path} class="text-muted-foreground hover:text-foreground">
                  {c.name}
                </a>
              </Show>
            </li>
          )}
        </For>
      </ol>
    </nav>
  );
}

export function PageTitle(props: { eyebrow?: JSX.Element; title: JSX.Element; children?: JSX.Element }) {
  return (
    <div class="mb-6 grid gap-2">
      <Show when={props.eyebrow}>
        <div class="text-xs font-semibold uppercase tracking-wider text-muted-foreground">{props.eyebrow}</div>
      </Show>
      <h1 class="text-3xl font-extrabold leading-tight">{props.title}</h1>
      <Show when={props.children}>
        <div class="max-w-3xl text-[15px] leading-relaxed text-muted-foreground">{props.children}</div>
      </Show>
    </div>
  );
}

export function Section(props: { title: string; id?: string; aside?: JSX.Element; children: JSX.Element }) {
  return (
    <section class="mt-8 grid gap-3" id={props.id}>
      <div class="flex flex-wrap items-end justify-between gap-2">
        <h2 class="text-xl font-bold">{props.title}</h2>
        {props.aside}
      </div>
      {props.children}
    </section>
  );
}

export function ChoiceBadge(props: { choice: Choice }) {
  return (
    <Badge
      variant={props.choice === 'Y' ? 'yea' : props.choice === 'N' ? 'nay' : 'secondary'}
      title={CHOICE[props.choice]}
    >
      {CHOICE[props.choice]}
    </Badge>
  );
}

export function PartyBadge(props: { party: string | null | undefined }) {
  return (
    <span
      class="inline-flex size-6 items-center justify-center rounded-md bg-muted text-xs font-bold"
      title={partyName(props.party)}
    >
      {props.party ?? '?'}
    </span>
  );
}

export function TallyBar(props: Tally & { class?: string }) {
  const total = () => Math.max(1, props.y + props.n + props.e + props.a);
  return (
    <div
      class={cn('flex h-2 w-full overflow-hidden rounded-full bg-muted', props.class)}
      role="img"
      aria-label={`${props.y} yea, ${props.n} nay, ${props.e + props.a} absent or excused`}
    >
      <div class="bg-yea" style={{ width: `${(props.y / total()) * 100}%` }} />
      <div class="bg-nay" style={{ width: `${(props.n / total()) * 100}%` }} />
    </div>
  );
}

export function TallyText(props: Tally) {
  return (
    <span class="whitespace-nowrap tabular-nums">
      <span class="font-semibold text-yea">{props.y}</span>
      <span class="text-muted-foreground">–</span>
      <span class="font-semibold text-nay">{props.n}</span>
    </span>
  );
}

export function Stat(props: { label: string; value: JSX.Element }) {
  return (
    <div class="grid gap-0.5">
      <div class="font-display text-2xl font-extrabold tabular-nums">{props.value}</div>
      <div class="text-xs text-muted-foreground">{props.label}</div>
    </div>
  );
}

/** Plain links (not client-only buttons) so crawlers can follow every page of a list. */
export function Pager(props: {
  page: number;
  pages: number;
  total: number;
  href: (page: number) => string;
  noun: string;
}) {
  return (
    <Show when={props.total > 0}>
      <div class="mt-3 flex flex-wrap items-center justify-between gap-3 text-sm text-muted-foreground">
        <span>
          {fmt(props.total)} {props.noun}
          <Show when={props.pages > 1}>
            {' '}
            · page {props.page} of {props.pages}
          </Show>
        </span>
        <Show when={props.pages > 1}>
          <span class="flex gap-2">
            <Show when={props.page > 1}>
              <a
                class={buttonVariants({ variant: 'outline', size: 'sm' })}
                href={props.href(props.page - 1)}
                rel="prev"
              >
                ← Previous
              </a>
            </Show>
            <Show when={props.page < props.pages}>
              <a
                class={buttonVariants({ variant: 'outline', size: 'sm' })}
                href={props.href(props.page + 1)}
                rel="next"
              >
                Next →
              </a>
            </Show>
          </span>
        </Show>
      </div>
    </Show>
  );
}

/** A GET form, so search works without JavaScript and results have their own URL. */
export function SearchBox(props: {
  q?: string;
  placeholder: string;
  hidden?: Record<string, string | number | undefined>;
  children?: JSX.Element;
}) {
  const navigate = useNavigate();
  return (
    <form
      method="get"
      class="flex flex-wrap items-center gap-2"
      onSubmit={(e) => {
        e.preventDefault();
        const fd = new FormData(e.currentTarget);
        const search: Record<string, string> = {};
        fd.forEach((v, k) => {
          if (String(v)) search[k] = String(v);
        });
        navigate({ to: '.', search: search as never });
      }}
    >
      <For each={Object.entries(props.hidden ?? {}).filter(([, v]) => v !== undefined)}>
        {([k, v]) => <input type="hidden" name={k} value={String(v)} />}
      </For>
      <Input
        name="q"
        type="search"
        value={props.q ?? ''}
        placeholder={props.placeholder}
        class="w-64"
        aria-label={props.placeholder}
      />
      {props.children}
      <button type="submit" class={buttonVariants({ variant: 'secondary', size: 'default' })}>
        Search
      </button>
    </form>
  );
}

export function NotFoundPage() {
  return (
    <Container>
      <PageTitle title="Page not found">
        This page doesn't exist. The link may be mistyped, or the record may have been removed by the Legislature.
      </PageTitle>
      <a href="/">Go to the home page</a>
    </Container>
  );
}

/** Footer of every record page: the same data as JSON, for scripts and AI agents. */
export function DataLinks(props: { kind: string; id: string }) {
  const url = () => `/data/${props.kind}/${encodeURIComponent(props.id)}.json`;
  return (
    <aside class="mt-10 rounded-lg border border-dashed p-4 text-sm text-muted-foreground">
      This record as <a href={`/${props.kind}/${encodeURIComponent(props.id)}.md`}>Markdown</a> or{' '}
      <a href={url()}>JSON</a>. ID <code class="font-mono text-foreground">{props.id}</code> is stable and matches the
      downloadable data. <a href="/developers">About the data</a>
    </aside>
  );
}
