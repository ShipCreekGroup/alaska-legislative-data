import {Client} from './api.js';

const client = new Client();

async function getAllMembers(session?: number) {
  if (!session) {
    session = 33;
  }
  const results = await client.getMembers({
    session: session,
    range: '1',
    queries:{
      Members: {
        chamber: 'S',
        firstname: 'J*',
      },
      Committees: {},
    },
  });

  return results;
}

async function getAllBills(session?: number) {
  if (!session) {
    session = 33;
  }
  const results = await client.getBills({
    session: session,
    range: '1',
  });
  return results;
}

async function test() {
  return await client.getBills({
    session: 33,
    queries: {
      Sponsors: {
        building: '100',
        chamber: 'S',
      },
      Bills: {
        bill: '*2',
      },
      Votes:{}
    },
    range: '1',
  });
}

async function main() {
  const results = await getAllMembers();
  console.log(results.length);
  console.log(results.slice(0, 5));
  // console.log(results[0].Votes)
}

await main();
