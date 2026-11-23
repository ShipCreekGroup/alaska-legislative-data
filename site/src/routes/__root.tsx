import { HeadContent, Outlet, Scripts, createRootRoute, useRouter } from '@tanstack/solid-router';
import { HydrationScript } from 'solid-js/web';
import { Suspense, onCleanup, onMount, type JSX } from 'solid-js';
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
  component: RootComponent,
  notFoundComponent: NotFoundPage,
});

/**
 * Tables are rendered as plain HTML links (see components/lists.tsx). This turns a click on any
 * same-site link into client-side navigation, as <Link> would.
 */
function RootComponent() {
  const router = useRouter();
  onMount(() => {
    const onClick = (e: MouseEvent) => {
      if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
      const a = (e.target as Element | null)?.closest?.('a');
      if (!a || a.target || a.hasAttribute('download') || a.origin !== location.origin) return;
      const href = a.getAttribute('href') ?? '';
      if (!href.startsWith('/') || /\.(json|xml|txt|md)(\?|$)/.test(a.pathname)) return;
      e.preventDefault();
      void router.navigate({ href });
    };
    document.addEventListener('click', onClick);
    onCleanup(() => document.removeEventListener('click', onClick));
  });
  return <Outlet />;
}

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
