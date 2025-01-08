// @ts-check
import { Bills, Members, Sessions, buildArgs} from './api.js';

async function bills() {
  const results = new Bills({
    range: '10',
    queries: {
      // actions: {},
      // bills : {},
      // fiscalnotes: {},
      sponsors: {
        district: '13',
      },
      // subjects: {},
      // versions: {},
      // votes: {},
    },
  });
  return await results.fetch();
}

async function members() {
  const results = new Members({
    range: '1',
    queries: {
      // members: {IsActive: false},
      // committees: {},
      bills: {
        bill: '*115',
      },
      // votes: {},
    },
  });
  return await results.fetch();
}
async function sessions() {
  const results = new Sessions({
    range: '1',
    queries: {
      // members: {isactive: false},
      // committees: {},
      // bills: {},
      // votes: {},
    },
  });
  return await results.fetch();
}

async function main() {
  const results = await bills();
  // const results = await members();
  // const results = await sessions();
  const jsonResults = JSON.stringify(results, null, 2);
  console.log(jsonResults);
}

await main(); 