// JSON copy of any record page: /data/bills/<BillId>.json, /data/votes/<VoteId>.json, ...
import { createFileRoute } from '@tanstack/solid-router';
import { getDoc, type Kind } from '~/server/store';

const KINDS: Kind[] = ['legislatures', 'bills', 'bill-versions', 'votes', 'members', 'people'];

export const Route = createFileRoute('/data/$kind/$file')({
  server: {
    handlers: {
      GET: async ({ params }) => {
        const kind = params.kind as Kind;
        if (!KINDS.includes(kind) || !params.file.endsWith('.json')) return new Response('Not found', { status: 404 });
        const doc = await getDoc(kind, decodeURIComponent(params.file.slice(0, -5)));
        if (!doc) return new Response('Not found', { status: 404 });
        return Response.json(doc, {
          headers: { 'Cache-Control': 'public, max-age=3600', 'Access-Control-Allow-Origin': '*' },
        });
      },
    },
  },
});
