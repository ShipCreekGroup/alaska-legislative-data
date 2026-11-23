// A sentence-style request builder for the Legislature's BASIS API.
// "Get [bills] from the [34th Legislature] [in both chambers] where [bill number is HB 1] with [sponsors]"
// Each underlined word maps to one part of the request shown below it. The match count comes from a
// HEAD request, so nothing large is downloaded until the user asks for it.
import { For, Show, createEffect, createMemo, createSignal, on } from 'solid-js';
import { createStore, produce } from 'solid-js/store';
import { Button } from '~/components/ui/button';
import { fmt, legYears, ordinal } from '~/lib/format';
import { cn } from '~/lib/utils';

const BASE = 'https://www.akleg.gov/publicservice/basis/';
type Ep = 'bills' | 'members' | 'committees' | 'meetings' | 'sessions';
const EP: Record<Ep, { noun: string; filters: Record<string, [string, string]>; incl: Record<string, string> }> = {
  bills: {
    noun: 'bills',
    filters: { bill: ['bill number is', 'HB 1'], subject: ['subject is', 'EDUCATION'] },
    incl: {
      sponsors: 'sponsors',
      actions: 'actions',
      subjects: 'subjects',
      versions: 'versions',
      votes: 'floor votes',
      fiscalnotes: 'fiscal notes',
    },
  },
  members: {
    noun: 'legislators',
    filters: { district: ['district is', '18'], code: ['member code is', 'FRB'], party: ['party is', 'D'] },
    incl: { committees: 'committees', bills: 'bills sponsored', votes: 'every vote cast' },
  },
  committees: {
    noun: 'committees',
    filters: { code: ['committee code is', 'FIN'] },
    incl: { members: 'members', meetings: 'meetings', bills: 'bills referred' },
  },
  meetings: {
    noun: 'committee meetings',
    filters: { date: ['date is', '4/15/2026'], startdate: ['starting', '4/1/2026'], enddate: ['ending', '4/30/2026'] },
    incl: { media: 'video links', minutes: 'minutes' },
  },
  sessions: {
    noun: 'session details',
    filters: {},
    incl: { journals: 'journals', subjects: 'all subjects', stats: 'stats' },
  },
};
type Q = { ep: Ep; leg: number; chamber: '' | 'H' | 'S'; f: Record<string, string>; inc: string[] };
const EXAMPLES: [string, Q][] = [
  ['Who sponsored HB 1?', { ep: 'bills', leg: 34, chamber: '', f: { bill: 'HB 1' }, inc: ['sponsors'] }],
  ['Education bills', { ep: 'bills', leg: 34, chamber: '', f: { subject: 'EDUCATION' }, inc: [] }],
  ['House members', { ep: 'members', leg: 34, chamber: 'H', f: {}, inc: [] }],
  ['Every vote by one legislator', { ep: 'members', leg: 34, chamber: '', f: { code: 'FRB' }, inc: ['votes'] }],
  ['Meetings on April 15', { ep: 'meetings', leg: 34, chamber: '', f: { date: '4/15/2026' }, inc: [] }],
  ['Session dates', { ep: 'sessions', leg: 34, chamber: '', f: {}, inc: [] }],
];

function header(q: Q): string {
  const fs = Object.entries(q.f).filter(([, v]) => v);
  const parts: string[] = [];
  if (fs.length || q.inc.length) parts.push(q.ep + fs.map(([k, v]) => `;${k}=${v}`).join(''));
  for (const i of q.inc) parts.push(i === 'versions' ? 'versions;fulltext=urlonly' : i);
  return parts.join(',');
}
const url = (q: Q) =>
  `${BASE}${q.ep}?minifyresult=false&json=true&session=${q.leg}${q.chamber ? `&chamber=${q.chamber}` : ''}`;
function headers(q: Q): Record<string, string> {
  const h: Record<string, string> = { 'X-Alaska-Legislature-Basis-Version': '1.4' };
  const hq = header(q);
  if (hq) h['X-Alaska-Legislature-Basis-Query'] = hq;
  return h;
}

function code(lang: string, q: Q): string {
  const u = url(q),
    hq = header(q);
  switch (lang) {
    case 'curl':
      return `curl "${u}" \\\n  -H "X-Alaska-Legislature-Basis-Version: 1.4"${hq ? ` \\\n  -H "X-Alaska-Legislature-Basis-Query: ${hq}"` : ''}`;
    case 'JavaScript':
      return `const res = await fetch("${u}", {\n  headers: {\n    "X-Alaska-Legislature-Basis-Version": "1.4",${hq ? `\n    "X-Alaska-Legislature-Basis-Query": "${hq}",` : ''}\n  },\n});\nconst data = (await res.json()).Basis;`;
    case 'TypeScript':
      return `// npm install alaska-legislative-api\nimport { createBasis } from "alaska-legislative-api";\n\nconst basis = createBasis();\nconst result = await basis.${q.ep === 'sessions' ? 'sessions.get' : q.ep + '.list'}({\n  session: ${q.leg},${q.chamber ? `\n  chamber: "${q.chamber}",` : ''}${hq ? `\n  query: "${hq}",` : ''}\n});`;
    default:
      return `import requests\n\nres = requests.get(\n    "${u}",\n    headers={\n        "X-Alaska-Legislature-Basis-Version": "1.4",${hq ? `\n        "X-Alaska-Legislature-Basis-Query": "${hq}",` : ''}\n    },\n)\ndata = res.json()["Basis"]`;
  }
}

const tok =
  'cursor-pointer appearance-none rounded-t border-0 border-b-2 border-dashed px-1.5 font-bold leading-normal';
const COLORS = {
  ep: 'text-link bg-link/10 border-link',
  u: 'text-[oklch(0.5_0.15_300)] bg-[oklch(0.5_0.15_300)]/10 border-[oklch(0.5_0.15_300)]',
  f: 'text-[oklch(0.55_0.12_75)] bg-gold/15 border-[oklch(0.55_0.12_75)]',
  i: 'text-yea bg-yea/10 border-yea',
};

export function ApiBuilder(props: { legislatures: number[] }) {
  const [q, setQ] = createStore<Q>(structuredClone(EXAMPLES[0]![1]));
  const [count, setCount] = createSignal<number | null | 'loading'>('loading');
  const [result, setResult] = createSignal<{
    status: 'idle' | 'loading' | 'ok' | 'error';
    body?: unknown;
    count?: number;
    error?: string;
  }>({ status: 'idle' });
  const [view, setView] = createSignal<'Table' | 'JSON'>('Table');
  const [lang, setLang] = createSignal('curl');
  const [hot, setHot] = createSignal<string | null>(null);
  const [example, setExample] = createSignal(0);
  const ep = () => EP[q.ep];

  let timer: ReturnType<typeof setTimeout> | undefined;
  let seq = 0;
  createEffect(
    on(
      () => JSON.stringify(q),
      () => {
        clearTimeout(timer);
        setCount('loading');
        const id = ++seq;
        timer = setTimeout(async () => {
          try {
            const res = await fetch(url(q), { method: 'HEAD', headers: headers(q) });
            if (id === seq) setCount(Number(res.headers.get('X-Alaska-Query-Count') ?? 0));
          } catch {
            if (id === seq) setCount(null);
          }
        }, 500);
      },
    ),
  );

  async function send() {
    setResult({ status: 'loading' });
    try {
      const res = await fetch(url(q), { headers: headers(q) });
      const body = await res.json();
      setResult({ status: 'ok', body, count: Number(res.headers.get('X-Alaska-Query-Count') ?? 0) });
    } catch (e) {
      setResult({ status: 'error', error: String(e) });
    }
  }
  const pick = (i: number) => {
    setExample(i);
    setQ(structuredClone(EXAMPLES[i]![1]));
    void send();
  };
  const edit = (fn: (s: Q) => void) => {
    setExample(-1);
    setQ(produce(fn));
  };

  const seg = (key: string, cls: string, text: string) => (
    <span
      class={cn('rounded px-px font-semibold', cls.split(' ')[0], hot() === key && 'outline-2 outline-current')}
      onMouseEnter={() => setHot(key)}
      onMouseLeave={() => setHot(null)}
    >
      {text}
    </span>
  );
  const hov = (key: string) => ({
    onMouseEnter: () => setHot(key),
    onMouseLeave: () => setHot(null),
    onFocus: () => setHot(key),
    onBlur: () => setHot(null),
  });

  return (
    <div class="grid gap-4">
      <div class="grid gap-4 rounded-xl border bg-card p-5">
        <div class="flex flex-wrap items-center gap-x-2 gap-y-1 font-display text-xl leading-loose">
          <span>Get</span>
          <select
            aria-label="What to get"
            class={cn(tok, COLORS.ep, hot() === 'ep' && 'outline-2')}
            {...hov('ep')}
            onChange={(e) =>
              edit((s) => {
                s.ep = e.currentTarget.value as Ep;
                s.f = {};
                s.inc = [];
              })
            }
          >
            <For each={Object.entries(EP)}>
              {([k, v]) => (
                <option value={k} selected={k === q.ep}>
                  {v.noun}
                </option>
              )}
            </For>
          </select>
          <span>from the</span>
          <select
            aria-label="Legislature"
            class={cn(tok, COLORS.u, hot() === 'leg' && 'outline-2')}
            {...hov('leg')}
            onChange={(e) =>
              edit((s) => {
                s.leg = Number(e.currentTarget.value);
              })
            }
          >
            <For each={props.legislatures}>
              {(n) => (
                <option value={n} selected={n === q.leg}>
                  {ordinal(n)} Legislature ({legYears(n)})
                </option>
              )}
            </For>
          </select>
          <Show when={q.ep !== 'sessions'}>
            <select
              aria-label="Chamber"
              class={cn(tok, COLORS.u, hot() === 'cham' && 'outline-2')}
              {...hov('cham')}
              onChange={(e) =>
                edit((s) => {
                  s.chamber = e.currentTarget.value as Q['chamber'];
                })
              }
            >
              <option value="" selected={!q.chamber}>
                in both chambers
              </option>
              <option value="H" selected={q.chamber === 'H'}>
                in the House
              </option>
              <option value="S" selected={q.chamber === 'S'}>
                in the Senate
              </option>
            </select>
          </Show>
          <For each={Object.keys(q.f)}>
            {(k, i) => (
              <span class="inline-flex items-center gap-1.5 whitespace-nowrap">
                <span>
                  {i() ? 'and' : 'where'} {ep().filters[k]?.[0]}
                </span>
                <input
                  aria-label={ep().filters[k]?.[0]}
                  value={q.f[k]}
                  placeholder={ep().filters[k]?.[1]}
                  size={Math.max(6, (q.f[k] || '').length + 1)}
                  class={cn(tok, COLORS.f, 'cursor-text', hot() === 'f-' + k && 'outline-2')}
                  {...hov('f-' + k)}
                  onInput={(e) =>
                    edit((s) => {
                      s.f[k] = e.currentTarget.value.trim();
                    })
                  }
                />
                <button
                  type="button"
                  aria-label="Remove filter"
                  class="px-1 text-base text-muted-foreground hover:text-nay"
                  onClick={() =>
                    edit((s) => {
                      delete s.f[k];
                    })
                  }
                >
                  ×
                </button>
              </span>
            )}
          </For>
          <Show when={Object.keys(ep().filters).some((k) => !(k in q.f))}>
            <select
              aria-label="Narrow down"
              class={cn(tok, 'border-dotted text-base text-muted-foreground')}
              onChange={(e) => {
                const k = e.currentTarget.value;
                e.currentTarget.value = '';
                if (k)
                  edit((s) => {
                    s.f[k] = '';
                  });
              }}
            >
              <option value="">+ narrow down</option>
              <For each={Object.keys(ep().filters).filter((k) => !(k in q.f))}>
                {(k) => <option value={k}>{ep().filters[k]![0].replace(/ is$/, '')}</option>}
              </For>
            </select>
          </Show>
          <Show when={q.inc.length}>
            <span>with</span>
            <For each={q.inc}>
              {(k) => (
                <span class="inline-flex items-center gap-1 whitespace-nowrap">
                  <span
                    tabindex="0"
                    class={cn(tok, COLORS.i, 'cursor-default', hot() === 'i-' + k && 'outline-2')}
                    {...hov('i-' + k)}
                  >
                    {ep().incl[k]}
                  </span>
                  <button
                    type="button"
                    aria-label={`Remove ${ep().incl[k]}`}
                    class="px-1 text-base text-muted-foreground hover:text-nay"
                    onClick={() =>
                      edit((s) => {
                        s.inc = s.inc.filter((x) => x !== k);
                      })
                    }
                  >
                    ×
                  </button>
                </span>
              )}
            </For>
          </Show>
          <Show when={Object.keys(ep().incl).some((k) => !q.inc.includes(k))}>
            <select
              aria-label="Add details"
              class={cn(tok, 'border-dotted text-base text-muted-foreground')}
              onChange={(e) => {
                const k = e.currentTarget.value;
                e.currentTarget.value = '';
                const order = Object.keys(ep().incl);
                if (k)
                  edit((s) => {
                    s.inc = [...s.inc, k].sort((a, b) => order.indexOf(a) - order.indexOf(b));
                  });
              }}
            >
              <option value="">+ add details</option>
              <For each={Object.keys(ep().incl).filter((k) => !q.inc.includes(k))}>
                {(k) => <option value={k}>{ep().incl[k]}</option>}
              </For>
            </select>
          </Show>
        </div>

        <div class="overflow-x-auto rounded-lg bg-muted px-4 py-2.5 font-mono text-xs leading-relaxed whitespace-nowrap">
          <div>
            <span class="text-muted-foreground">GET</span> {BASE}
            {seg('ep', COLORS.ep, q.ep)}?json=true&amp;{seg('leg', COLORS.u, `session=${q.leg}`)}
            <Show when={q.chamber}>&amp;{seg('cham', COLORS.u, `chamber=${q.chamber}`)}</Show>
          </div>
          <Show when={header(q)}>
            <div>
              <span class="text-muted-foreground">X-Alaska-Legislature-Basis-Query:</span>{' '}
              <Show when={Object.values(q.f).some(Boolean) || q.inc.length}>{seg('ep', COLORS.ep, q.ep)}</Show>
              <For each={Object.entries(q.f).filter(([, v]) => v)}>
                {([k, v]) => seg('f-' + k, COLORS.f, `;${k}=${v}`)}
              </For>
              <For each={q.inc}>
                {(k) => seg('i-' + k, COLORS.i, `,${k === 'versions' ? 'versions;fulltext=urlonly' : k}`)}
              </For>
            </div>
          </Show>
        </div>

        <div class="flex flex-wrap items-center justify-between gap-3 border-t pt-3">
          <span>
            <Show when={count() === 'loading'}>
              <span class="text-muted-foreground">Counting…</span>
            </Show>
            <Show when={count() === null}>
              <span class="text-sm text-muted-foreground">Couldn't reach akleg.gov to count results.</span>
            </Show>
            <Show when={typeof count() === 'number'}>
              <b class="font-display text-lg tabular-nums">{fmt(count() as number)}</b>{' '}
              {count() === 1 ? ep().noun.replace(/s$/, '') : ep().noun} match
              <Show when={(count() as number) > 150}>
                <span class="text-sm text-muted-foreground"> · a big list, so it may take 20–30 seconds</span>
              </Show>
            </Show>
          </span>
          <Button onClick={send} disabled={count() === 0 || result().status === 'loading'}>
            {result().status === 'loading'
              ? 'Fetching…'
              : typeof count() === 'number' && count()
                ? `Get ${count() === 1 ? 'it' : fmt(count() as number)}`
                : 'Get results'}
          </Button>
        </div>
      </div>

      <div class="flex flex-wrap items-center gap-2 text-sm">
        <span class="text-muted-foreground">Examples:</span>
        <For each={EXAMPLES}>
          {([label], i) => (
            <button
              type="button"
              onClick={() => pick(i())}
              class={cn('rounded-full border px-3 py-1 hover:bg-accent', example() === i() && 'border-ring bg-accent')}
            >
              {label}
            </button>
          )}
        </For>
      </div>

      <div class="rounded-xl border bg-card">
        <div class="flex flex-wrap items-center justify-between gap-2 border-b px-4 py-2.5 text-sm">
          <span class="text-muted-foreground">
            {result().status === 'idle'
              ? 'Results appear here.'
              : result().status === 'loading'
                ? 'Fetching…'
                : result().status === 'error'
                  ? `Request failed: ${result().error}`
                  : `${fmt(result().count ?? 0)} results from akleg.gov`}
          </span>
          <div class="inline-flex rounded-md border p-0.5">
            <For each={['Table', 'JSON'] as const}>
              {(v) => (
                <button
                  type="button"
                  class={cn(
                    'rounded px-2.5 py-0.5 text-xs font-semibold',
                    view() === v ? 'bg-accent text-foreground' : 'text-muted-foreground',
                  )}
                  onClick={() => setView(v)}
                >
                  {v}
                </button>
              )}
            </For>
          </div>
        </div>
        <Show when={result().status === 'ok'}>
          <Show
            when={view() === 'Table'}
            fallback={
              <pre class="max-h-[32rem] overflow-auto p-4 font-mono text-xs">
                {JSON.stringify(result().body, null, 2)}
              </pre>
            }
          >
            <ResultTable body={result().body} />
          </Show>
        </Show>
      </div>

      <details class="text-sm">
        <summary class="cursor-pointer font-semibold text-link">Show code for this request</summary>
        <div class="mt-3">
          <div class="inline-flex rounded-t-md border border-b-0 p-0.5">
            <For each={['curl', 'JavaScript', 'TypeScript', 'Python']}>
              {(l) => (
                <button
                  type="button"
                  class={cn(
                    'rounded px-2.5 py-0.5 text-xs font-semibold',
                    lang() === l ? 'bg-accent' : 'text-muted-foreground',
                  )}
                  onClick={() => setLang(l)}
                >
                  {l}
                </button>
              )}
            </For>
          </div>
          <pre class="overflow-x-auto rounded-b-lg rounded-tr-lg bg-[oklch(0.2_0.03_255)] p-4 font-mono text-xs leading-relaxed text-[oklch(0.9_0.01_250)]">
            {code(lang(), q)}
          </pre>
        </div>
      </details>
    </div>
  );
}

const PREF = [
  'BillNumber',
  'ShortTitle',
  'StatusText',
  'StatusDate',
  'Code',
  'FirstName',
  'LastName',
  'Party',
  'District',
  'Chamber',
  'Name',
  'Category',
  'Date',
  'Title',
  'Location',
];

function ResultTable(props: { body: unknown }) {
  const shape = createMemo(() => {
    const b = (props.body as { Basis?: Record<string, unknown> })?.Basis;
    if (!b) return null;
    const key = Object.keys(b).find((k) => Array.isArray(b[k]));
    if (!key) return null;
    const items = (b[key] as Record<string, unknown>[]).filter((x) => x && typeof x === 'object');
    if (!items.length) return { cols: [], nests: [], items };
    const first = items[0]!;
    let cols = PREF.filter((k) => k in first && typeof first[k] !== 'object');
    if (cols.length < 3)
      cols = Object.keys(first)
        .filter((k) => first[k] !== null && typeof first[k] !== 'object' && String(first[k]).trim())
        .slice(0, 6);
    const nests = Object.keys(first).filter(
      (k) => Array.isArray(first[k]) && items.some((x) => (x[k] as unknown[])?.length),
    );
    return { cols, nests, items };
  });
  return (
    <Show
      when={shape()}
      fallback={
        <pre class="max-h-[32rem] overflow-auto p-4 font-mono text-xs">{JSON.stringify(props.body, null, 2)}</pre>
      }
    >
      {(s) => (
        <div class="max-h-[32rem] overflow-auto">
          <table class="w-full text-sm">
            <thead class="sticky top-0 bg-muted">
              <tr>
                <For each={[...s().cols, ...s().nests]}>
                  {(c) => <th class="px-3 py-2 text-left font-mono text-xs font-semibold">{c}</th>}
                </For>
              </tr>
            </thead>
            <tbody>
              <For each={s().items}>
                {(x) => (
                  <tr class="border-t">
                    <For each={s().cols}>{(c) => <td class="px-3 py-1.5">{String(x[c] ?? '').trim()}</td>}</For>
                    <For each={s().nests}>
                      {(c) => (
                        <td class="px-3 py-1.5">
                          <Show when={(x[c] as unknown[])?.length} fallback="—">
                            <span class="rounded-full bg-yea/10 px-2 text-xs font-semibold text-yea">
                              {(x[c] as unknown[]).length} {c.toLowerCase()}
                            </span>
                          </Show>
                        </td>
                      )}
                    </For>
                  </tr>
                )}
              </For>
            </tbody>
          </table>
        </div>
      )}
    </Show>
  );
}
