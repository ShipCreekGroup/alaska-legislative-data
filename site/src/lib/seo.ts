import { SITE_NAME, SITE_URL } from './format';

export interface Crumb {
  name: string;
  path: string;
}

/**
 * Head tags for one page: title, description, canonical link, Open Graph, and JSON-LD
 * (the page's own schema.org object plus a BreadcrumbList).
 */
export function seo(opts: {
  title: string;
  description: string;
  path: string;
  crumbs?: Crumb[];
  jsonLd?: object;
  noindex?: boolean;
}) {
  const url = SITE_URL + opts.path;
  const title = opts.path === '/' ? opts.title : `${opts.title} | ${SITE_NAME}`;
  const graph: object[] = [];
  if (opts.jsonLd) graph.push({ ...opts.jsonLd, url });
  if (opts.crumbs?.length)
    graph.push({
      '@type': 'BreadcrumbList',
      itemListElement: opts.crumbs.map((c, i) => ({
        '@type': 'ListItem',
        position: i + 1,
        name: c.name,
        item: SITE_URL + c.path,
      })),
    });
  return {
    meta: [
      { title },
      { name: 'description', content: opts.description },
      { property: 'og:title', content: title },
      { property: 'og:description', content: opts.description },
      { property: 'og:url', content: url },
      { property: 'og:type', content: 'website' },
      { property: 'og:site_name', content: SITE_NAME },
      ...(opts.noindex ? [{ name: 'robots', content: 'noindex, follow' }] : []),
    ],
    links: [{ rel: 'canonical', href: url }],
    scripts: graph.length
      ? [
          {
            type: 'application/ld+json',
            children: JSON.stringify({ '@context': 'https://schema.org', '@graph': graph }),
          },
        ]
      : [],
  };
}

/** Pages 2+ of a list and filtered views stay crawlable but aren't indexed separately. */
export const isVariant = (search: Record<string, unknown>) =>
  Object.entries(search).some(([k, v]) => v !== undefined && v !== '' && !(k === 'page' && v === 1));
