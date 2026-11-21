import { createFileRoute } from '@tanstack/solid-router';
import { sitemap } from '~/server/sitemap';

export const Route = createFileRoute('/sitemaps/$name')({
  server: {
    handlers: {
      GET: async ({ params }) =>
        (await sitemap(params.name.replace(/\.xml$/, ''))) ?? new Response('Not found', { status: 404 }),
    },
  },
});
