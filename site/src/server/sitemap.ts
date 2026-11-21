// Sitemaps: one index, then one file per record type and legislature, each well under the
// 50,000-URL limit.
import { getDoc, getMeta, getPeopleIndex } from './store';
import { SITE_URL } from '~/lib/format';
import type { Bill, Legislature, Meta, PersonIndexRow } from '~/lib/types';

const xml = (body: string) =>
  new Response(`<?xml version="1.0" encoding="UTF-8"?>\n${body}`, {
    headers: { 'Content-Type': 'application/xml; charset=utf-8', 'Cache-Control': 'public, max-age=3600' },
  });
const esc = (s: string) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;');
const loc = (path: string, lastmod?: string | null) =>
  `<url><loc>${esc(SITE_URL + path)}</loc>${lastmod ? `<lastmod>${lastmod}</lastmod>` : ''}</url>`;
const urlset = (urls: string[]) =>
  xml(`<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n${urls.join('\n')}\n</urlset>`);
const enc = encodeURIComponent;

export async function sitemapIndex(): Promise<Response> {
  const meta = (await getMeta<Meta>())!;
  const names = ['pages', 'people'];
  for (const l of meta.legislatures) {
    if (l.bills) names.push(`bills-${l.number}`, `versions-${l.number}`);
    if (l.votes) names.push(`votes-${l.number}`);
    if (l.members) names.push(`members-${l.number}`);
  }
  return xml(
    `<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n${names
      .map((n) => `<sitemap><loc>${SITE_URL}/sitemaps/${n}.xml</loc><lastmod>${meta.updated}</lastmod></sitemap>`)
      .join('\n')}\n</sitemapindex>`,
  );
}

export async function sitemap(name: string): Promise<Response | null> {
  const meta = (await getMeta<Meta>())!;
  if (name === 'pages') {
    const legs = meta.legislatures.map((l) => l.number);
    return urlset([
      loc('/', meta.updated),
      ...['/legislatures', '/bills', '/votes', '/legislators', '/people', '/ask', '/developers'].map((p) => loc(p)),
      ...legs.map((n) => loc(`/legislatures/${n}`)),
      ...meta.legislatures.filter((l) => l.bills).map((l) => loc(`/bills?legislature=${l.number}`)),
      ...meta.legislatures.filter((l) => l.votes).map((l) => loc(`/votes?legislature=${l.number}`)),
      ...meta.legislatures.filter((l) => l.members).map((l) => loc(`/legislators?legislature=${l.number}`)),
    ]);
  }
  if (name === 'people') {
    const people = (await getPeopleIndex<PersonIndexRow[]>()) ?? [];
    return urlset(people.map((p) => loc(`/people/${enc(p.id)}`)));
  }
  const m = /^(bills|versions|votes|members)-(\d+)$/.exec(name);
  if (!m) return null;
  const leg = await getDoc<Legislature>('legislatures', Number(m[2]));
  if (!leg) return null;
  switch (m[1]) {
    case 'bills':
      return urlset(leg.bills.map((b) => loc(`/bills/${enc(b.id)}`, b.statusDate)));
    case 'votes':
      return urlset(leg.votes.map((v) => loc(`/votes/${enc(v.id)}`, v.date)));
    case 'members':
      return urlset(leg.members.map((x) => loc(`/members/${enc(x.id)}`)));
    case 'versions': {
      // Version ids live in each bill's document.
      const urls: string[] = [];
      for (const b of leg.bills) {
        const bill = await getDoc<Bill>('bills', b.id);
        for (const v of bill?.versions ?? []) urls.push(loc(`/bill-versions/${enc(v.id)}`, v.introDate));
      }
      return urlset(urls);
    }
  }
  return null;
}
