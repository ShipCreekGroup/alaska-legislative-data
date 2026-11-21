import { createFileRoute } from '@tanstack/solid-router';
import { SITE_URL } from '~/lib/format';

export const Route = createFileRoute('/robots.txt')({
  server: {
    handlers: {
      GET: () =>
        new Response(`User-agent: *\nAllow: /\n\nSitemap: ${SITE_URL}/sitemap.xml\n`, {
          headers: { 'Content-Type': 'text/plain; charset=utf-8' },
        }),
    },
  },
});
