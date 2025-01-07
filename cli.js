// @ts-check
import { Bills } from './api.js';

function bills(session) {
  if (!session) {
    session = 33;
  }
  const results = new Bills({
    session: session,
    range: '10',
    queries: {
      Actions: {},
      Bills : {
        // committeecode: 'dasd',
        // bill: 'HB   *1*'
      },
      Fiscalnotes: {},
      Sponsors: {},
      Subjects: {},
      Versions: {},
      Votes: {},
    },
  });
  return results;
}

async function main() {
  let b = bills();
  // results = results.slice(0, 5);
  const jsonResults = JSON.stringify(await b.fetch(), null, 2);
  console.log(jsonResults);
//   console.log(await b.count());
}

await main(); 