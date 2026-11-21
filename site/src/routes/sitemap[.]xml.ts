import { createFileRoute } from '@tanstack/solid-router';
import { sitemapIndex } from '~/server/sitemap';

export const Route = createFileRoute('/sitemap.xml')({
  server: { handlers: { GET: () => sitemapIndex() } },
});
