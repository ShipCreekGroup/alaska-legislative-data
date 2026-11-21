import { createFileRoute } from '@tanstack/solid-router';
import { For, createSignal } from 'solid-js';
import { Breadcrumbs, Container, PageTitle } from '~/components/site';
import { Button, buttonVariants } from '~/components/ui/button';
import { seo } from '~/lib/seo';
import { cn } from '~/lib/utils';

const SKILL = 'https://raw.githubusercontent.com/ShipCreekGroup/alaska-legislative-data/refs/heads/main/SKILL.md';
const QUESTIONS = [
  'How did my representative vote on the PFD this year?',
  'Find the closest votes on cost of living in 2025–26, and how each legislator voted.',
  'Which legislators most often voted against their own caucus?',
  'Which education funding bills passed both chambers since 2015?',
  'Summarize what a bill does, using its text.',
];
const crumbs = [
  { name: 'Home', path: '/' },
  { name: 'Ask AI', path: '/ask' },
];
const promptFor = (q: string) =>
  `First read ${SKILL} and follow its setup steps.\n\n${q}\n\nShow me the SQL you ran and link each bill to akleg.gov.`;

export const Route = createFileRoute('/ask')({
  head: () =>
    seo({
      title: 'Ask about Alaska Legislature votes in plain English',
      description:
        'Copy a ready-made prompt into Claude to answer questions about Alaska Legislature bills and votes, with the work shown.',
      path: '/ask',
      crumbs,
    }),
  component: () => {
    const [prompt, setPrompt] = createSignal(promptFor(QUESTIONS[1]!));
    const [copied, setCopied] = createSignal(false);
    return (
      <Container class="max-w-3xl">
        <Breadcrumbs crumbs={crumbs} />
        <PageTitle title="Ask in plain English">
          Pick a question or write your own, then paste the prompt into Claude. It downloads the data, works out the
          answer, and shows its work. No coding needed.
        </PageTitle>
        <div class="grid gap-4 rounded-xl border bg-card p-5">
          <div class="flex flex-wrap gap-2">
            <For each={QUESTIONS}>
              {(q) => (
                <button
                  type="button"
                  class={cn(
                    'rounded-full border px-3 py-1 text-left text-sm hover:bg-accent',
                    prompt() === promptFor(q) && 'border-ring bg-accent',
                  )}
                  onClick={() => setPrompt(promptFor(q))}
                >
                  {q}
                </button>
              )}
            </For>
          </div>
          <textarea
            aria-label="Prompt"
            class="min-h-36 w-full rounded-md border bg-muted p-3 font-mono text-sm"
            value={prompt()}
            onInput={(e) => setPrompt(e.currentTarget.value)}
          />
          <div class="flex flex-wrap gap-2">
            <Button
              onClick={async () => {
                await navigator.clipboard.writeText(prompt()).catch(() => {});
                setCopied(true);
                setTimeout(() => setCopied(false), 1500);
              }}
            >
              {copied() ? 'Copied' : 'Copy prompt'}
            </Button>
            <a
              class={buttonVariants({ variant: 'outline' })}
              href={`https://claude.ai/new?q=${encodeURIComponent(prompt())}`}
              target="_blank"
              rel="noopener"
            >
              Open in Claude ↗
            </a>
          </div>
          <p class="text-sm text-muted-foreground">
            Works in Claude or ChatGPT Codex. Regular ChatGPT and Gemini can't run the code it needs yet.
          </p>
        </div>
        <details class="mt-6 border-t py-4">
          <summary class="cursor-pointer font-display font-bold">Limits of the data</summary>
          <ul class="mt-3 list-disc space-y-1.5 pl-5 text-sm">
            <li>Vote records are most complete from about 1995 (the 19th Legislature) on.</li>
            <li>Only floor votes. Committee votes aren't included.</li>
            <li>Refreshed nightly, so a vote taken today shows up tomorrow.</li>
            <li>
              If the Legislature corrects a record, the earlier version is kept. This history starts in November 2026.
            </li>
            <li>AI assistants can make mistakes. Ask for the work, and check important numbers on akleg.gov.</li>
          </ul>
        </details>
        <p class="mt-4 text-sm text-muted-foreground">
          Need to track key votes over time? Ship Creek Group runs a vote-tracking service for lobbyists, nonprofits and
          advocates. <a href="https://shipcreekgroup.com">Get in touch</a>
        </p>
      </Container>
    );
  },
});
