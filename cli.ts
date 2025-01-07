import {Client} from './api.ts';

const client = new Client();

async function main() {
  const results = await client.getBills({
    session: 33,
    queries: {
      Sponsors: {
        building: '100',
        chamber: 'H',
      },
    },
    range: '10',
  });

  console.log(results);
}

await main();
