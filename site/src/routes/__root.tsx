import { HeadContent, Outlet, Scripts, createRootRoute } from '@tanstack/solid-router';
import { HydrationScript } from 'solid-js/web';
import { Suspense, type JSX } from 'solid-js';
import appCss from '~/styles.css?url';
import { NotFoundPage, SiteFooter, SiteHeader } from '~/components/site';
import { SITE_NAME } from '~/lib/format';

export const Route = createRootRoute({
  head: () => ({
    meta: [
      { charSet: 'utf-8' },
      { name: 'viewport', content: 'width=device-width, initial-scale=1' },
      { name: 'theme-color', content: '#0f2a4a' },
    ],
    links: [
      { rel: 'stylesheet', href: appCss },
      { rel: 'preconnect', href: 'https://fonts.googleapis.com' },
      {
        rel: 'stylesheet',
        href: 'https://fonts.googleapis.com/css2?family=Archivo:wght@600;700;800&family=Public+Sans:ital,wght@0,400;0,500;0,600;1,400&family=JetBrains+Mono:wght@400;500&display=swap',
      },
      { rel: 'alternate', type: 'text/plain', href: '/llms.txt', title: `${SITE_NAME} for language models` },
      { rel: 'sitemap', type: 'application/xml', href: '/sitemap.xml' },
    ],
  }),
  shellComponent: RootDocument,
  component: () => <Outlet />,
  notFoundComponent: NotFoundPage,
});

function RootDocument(props: { children: JSX.Element }) {
  return (
    <html lang="en">
      <head>
        <HydrationScript />
      </head>
      <body>
        <HeadContent />
        <a href="#content" class="sr-only focus:not-sr-only">
          Skip to content
        </a>
        <SiteHeader />
        <div id="content">
          <Suspense>{props.children}</Suspense>
        </div>
        <SiteFooter />
        <Scripts />
      </body>
    </html>
  );
}
