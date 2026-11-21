import { Link, createFileRoute } from '@tanstack/solid-router';
import { Show } from 'solid-js';
import { Breadcrumbs, Container, DataLinks, PageTitle, Section } from '~/components/site';
import { fetchBillVersion } from '~/server/fns';
import { SITE_URL, clip, date, legName, legYears, legFull } from '~/lib/format';
import { versionSummary as summary } from '~/lib/summaries';
import { seo } from '~/lib/seo';
import type { BillVersion } from '~/lib/types';
import { href } from '~/lib/search';

const crumbsFor = (v: BillVersion) => [
  { name: 'Home', path: '/' },
  { name: legName(v.legislature), path: `/legislatures/${v.legislature}` },
  { name: 'Bills', path: href('/bills', { legislature: v.legislature }) },
  { name: v.billNumber, path: `/bills/${encodeURIComponent(v.billId)}` },
  { name: `Version ${v.letter}`, path: `/bill-versions/${encodeURIComponent(v.id)}` },
];

export const Route = createFileRoute('/bill-versions/$versionId')({
  loader: ({ params }) => fetchBillVersion({ data: { id: params.versionId } }),
  head: ({ loaderData: v }) => {
    if (!v) return {};
    return seo({
      title: `${v.billNumber} version ${v.letter} full text (${legName(v.legislature)}): ${v.billTitle ?? v.title}`,
      description: clip(summary(v) + (v.fullText ? ' ' + v.fullText : '')),
      path: `/bill-versions/${encodeURIComponent(v.id)}`,
      crumbs: crumbsFor(v),
      markdown: true,
      jsonLd: {
        '@type': 'Legislation',
        name: v.name,
        alternateName: v.title,
        legislationIdentifier: `${v.billNumber} ${v.letter}`,
        legislationDate: v.introDate,
        legislationJurisdiction: { '@type': 'AdministrativeArea', name: 'Alaska' },
        isPartOf: {
          '@type': 'Legislation',
          name: v.billNumber,
          url: `${SITE_URL}/bills/${encodeURIComponent(v.billId)}`,
        },
        encoding: { '@type': 'MediaObject', encodingFormat: 'application/pdf', contentUrl: v.pdfUrl },
      },
    });
  },
  component: () => {
    const v = Route.useLoaderData();
    return (
      <Container>
        <Breadcrumbs crumbs={crumbsFor(v())} />
        <PageTitle
          eyebrow={`${legName(v().legislature)} · ${legYears(v().legislature)}`}
          title={`${v().billNumber} version ${v().letter}`}
        >
          {summary(v())}
        </PageTitle>
        <dl class="grid max-w-3xl grid-cols-[max-content_1fr] gap-x-6 gap-y-1.5 rounded-xl border bg-card p-5 text-sm">
          <dt class="text-muted-foreground">Bill</dt>
          <dd>
            <Link to="/bills/$billId" params={{ billId: v().billId }}>
              {v().billNumber}
            </Link>{' '}
            {v().billTitle}
          </dd>
          <dt class="text-muted-foreground">Title</dt>
          <dd>{v().title}</dd>
          <dt class="text-muted-foreground">Introduced</dt>
          <dd>{date(v().introDate)}</dd>
          <dt class="text-muted-foreground">Work order</dt>
          <dd class="font-mono text-xs">{v().workOrder}</dd>
          <dt class="text-muted-foreground">Official PDF</dt>
          <dd>
            <a href={v().pdfUrl} rel="external">
              {v().pdfUrl.replace(/^https?:\/\//, '')} ↗
            </a>
          </dd>
        </dl>
        <Section title="Full text">
          <Show
            when={v().fullText}
            fallback={
              <p class="text-muted-foreground">
                The text of this version isn't in our data. Use the official PDF above.
              </p>
            }
          >
            <pre class="max-w-4xl overflow-x-auto rounded-xl border bg-card p-5 font-mono text-[13px] leading-relaxed whitespace-pre-wrap">
              {v().fullText}
            </pre>
          </Show>
        </Section>
        <DataLinks kind="bill-versions" id={v().id} />
      </Container>
    );
  },
});
