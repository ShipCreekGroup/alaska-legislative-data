// Runs on every request to the server.
// 1. Markdown: /bills/<id>.md (or Accept: text/markdown) returns the record as Markdown.
// 2. Caching: GET responses get Cache-Control. On Cloudflare, rendered responses are also kept in
//    the edge cache. The cache key includes the data's last-updated date, so the nightly update
//    replaces everything without an explicit purge.
import { createMiddleware, createStart } from '@tanstack/solid-start';
import { SITE_URL } from '~/lib/format';
import type { Meta } from '~/lib/types';
import { markdownTarget, renderMarkdown } from '~/server/markdown';
import { getMeta } from '~/server/store';

const BROWSER = 'public, max-age=300';
const EDGE = 'public, max-age=86400';

type EdgeCache = { match(r: Request): Promise<Response | undefined>; put(r: Request, res: Response): Promise<void> };
const edgeCache = (): EdgeCache | undefined => (globalThis as { caches?: { default?: EdgeCache } }).caches?.default;

const edge = createMiddleware().server(async ({ request, next }) => {
  if (request.method !== 'GET') return next();
  const url = new URL(request.url);
  const md = markdownTarget(url, request.headers.get('accept'));

  const cache = edgeCache();
  let key: Request | undefined;
  if (cache) {
    const version = (await getMeta<Meta>())?.updated ?? '';
    const k = new URL(url);
    k.searchParams.set('__v', version);
    if (md) k.searchParams.set('__md', '1');
    key = new Request(k.toString());
    const hit = await cache.match(key);
    if (hit) {
      const res = new Response(hit.body, hit);
      res.headers.set('Cache-Control', BROWSER);
      return res;
    }
  }

  let res: Response;
  if (md) {
    const text = await renderMarkdown(md.kind, md.id);
    res =
      text === null
        ? new Response('Not found\n', { status: 404, headers: { 'Content-Type': 'text/plain; charset=utf-8' } })
        : new Response(text, {
            headers: {
              'Content-Type': 'text/markdown; charset=utf-8',
              // The HTML page is the one search engines should index.
              'X-Robots-Tag': 'noindex',
              Link: `<${SITE_URL}${md.htmlPath}>; rel="canonical"`,
              'Access-Control-Allow-Origin': '*',
            },
          });
  } else {
    res = (await next()).response;
  }

  if (res.status !== 200 || res.headers.has('Set-Cookie')) return res;
  res = new Response(res.body, res);
  if (!res.headers.has('Cache-Control')) res.headers.set('Cache-Control', BROWSER);
  if (markdownTarget(url, 'text/markdown')) res.headers.append('Vary', 'Accept');
  if (cache && key) {
    const copy = res.clone();
    const stored = new Response(copy.body, copy);
    stored.headers.set('Cache-Control', EDGE);
    await cache.put(key, stored);
  }
  return res;
});

export const startInstance = createStart(() => ({ requestMiddleware: [edge] }));
