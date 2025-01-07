import {Client} from './api.js';

const client = new Client();

async function getAllMembers(session?: number) {
  if (!session) {
    session = 33;
  }
  const results = await client.getMembers({
    session: session,
    // range: '10',
    queries: {
      // Members: {
      //   chamber: 'S',
      //   firstname: 'J*',
      // },
      Votes: {},
      // Committees: {},
    },
  });

  return results;
}

async function getBills(session?: number) {
  if (!session) {
    session = 33;
  }
  const results = await client.getBills({
    session: session,
    range: '10',
    queries: {
      Votes: {},
    },
  });
  return results;
}

async function main() {
  let results = await getAllMembers();
  // results = results.slice(0, 5);
  const jsonResults = JSON.stringify(results, null, 2);
  console.log(jsonResults);
}

await main();
