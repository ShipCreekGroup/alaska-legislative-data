# Alaska Legislative Data website

A page for every bill, bill version, roll-call vote, legislator term, person and legislature, all
rendered on the server so search engines and AI agents can read them without JavaScript.

Built with TanStack Start (SolidJS), TanStack Router, Tailwind and shadcn-style components on Kobalte.

## Pages

| Path                                                           | What                                                                |
| -------------------------------------------------------------- | ------------------------------------------------------------------- |
| `/bills/<BillId>`                                              | A bill, its versions and every floor vote on it                     |
| `/bill-versions/<BillVersionId>`                               | Full text of one version                                            |
| `/votes/<VoteId>`                                              | One roll call, with how each legislator voted and a party breakdown |
| `/members/<MemberId>`                                          | One legislator's term and voting record                             |
| `/people/<PersonId>`                                           | One legislator's whole career                                       |
| `/legislatures/<n>`                                            | One legislature                                                     |
| `/bills`, `/votes`, `/legislators`, `/people`, `/legislatures` | Lists, filterable by legislature and searchable                     |
| `/data/<type>/<id>.json`                                       | The JSON behind any record page                                     |
| `/sitemap.xml`, `/robots.txt`, `/llms.txt`                     | For search engines and language models                              |
| `/ask`, `/developers`                                          | Prompt for AI assistants; downloads and the live API builder        |

Every page has a title, description, canonical URL, breadcrumbs and schema.org JSON-LD.

## Run locally

```bash
pnpm install
node scripts/extract.ts path/to/ak_leg.duckdb .data   # one JSON file per page, about 60,000 (~750 MB)
pnpm dev                                              # http://localhost:3000
```

## Deploy to Cloudflare

Pages are rendered on request by a Cloudflare Worker that reads the JSON files from R2.

1. Each night, after `ak_leg.duckdb` is built, run `scripts/extract.ts` and upload the output folder
   to the R2 bucket under `site/latest/`.
2. Put the bucket name in `wrangler.jsonc`.
3. Build and deploy:
   ```bash
   NITRO_PRESET=cloudflare-module VITE_SITE_URL=https://<your domain> pnpm build
   npx wrangler deploy
   ```

`VITE_SITE_URL` is used for canonical links, sitemaps and structured data.
