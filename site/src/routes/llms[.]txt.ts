import { createFileRoute } from '@tanstack/solid-router';
import { SITE_URL } from '~/lib/format';
import { getMeta } from '~/server/store';
import type { Meta } from '~/lib/types';

export const Route = createFileRoute('/llms.txt')({
  server: {
    handlers: {
      GET: async () => {
        const meta = await getMeta<Meta>();
        const current = meta?.legislatures[0]?.number ?? 34;
        const body = `# Alaska Legislative Data

> Every Alaska State Legislature bill, roll-call vote, individual legislator vote, legislator and bill text since 1981.
> Updated nightly (last: ${meta?.updated}). MIT license. Maintained by Ship Creek Group, Anchorage.

Every record has a permanent page, a Markdown copy and a JSON copy. IDs are stable and match the downloadable tables.

## Pages
- ${SITE_URL}/bills/<BillId>: a bill, its versions and every floor vote on it
- ${SITE_URL}/bill-versions/<BillVersionId>: full text of one version
- ${SITE_URL}/votes/<VoteId>: one roll call, with how each legislator voted
- ${SITE_URL}/members/<MemberId>: one legislator's term and full voting record
- ${SITE_URL}/people/<PersonId>: one legislator's whole career
- ${SITE_URL}/legislatures/<number>: one two-year legislature (current: ${current})
- Lists: ${SITE_URL}/bills?legislature=${current}, ${SITE_URL}/votes?legislature=${current}, ${SITE_URL}/legislators?legislature=${current}

## Markdown and JSON
- Add .md to any record page for a Markdown copy with no layout, e.g. ${SITE_URL}/legislatures/${current}.md
  (or send Accept: text/markdown). Markdown lists are not paginated.
- ${SITE_URL}/data/<bills|bill-versions|votes|members|people|legislatures>/<id>.json

## Bulk data and analysis
- [SKILL.md](https://raw.githubusercontent.com/ShipCreekGroup/alaska-legislative-data/refs/heads/main/SKILL.md): schema, join rules, pitfalls, example SQL
- [export.zip](https://github.com/ShipCreekGroup/alaska-legislative-data/releases/latest/download/export.zip): DuckDB, CSV and Parquet
- [Sitemap](${SITE_URL}/sitemap.xml)
`;
        return new Response(body, {
          headers: { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'public, max-age=3600' },
        });
      },
    },
  },
});
