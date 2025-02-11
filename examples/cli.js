// @ts-check
import { Bills, Members, Sessions} from '../index.js';

/** @type {import('../index.js').IntoConfig} */
const config = {
  logger: 'DEBUG',
};

async function bills() {
  const results = new Bills({
    range: '20',
    queries: {
      // Documents: {},
      // actions: {},
      // bills : {onfloor: true},
      // fiscalnotes: {},
      // sponsors: {
      //   district: '13',
      // },
      // subjects: {},
      // versions: {},
      // votes: {},
    },
  });
  return await results.fetch();
}

async function members() {
  const results = new Members({
    session: 33,
    // range: '10',
    queries: {
      // members: {firstname: 'je*'},
      // committees: {},
      bills: {
        bill: 'hb 228',
      },
      // votes: {},
    },
  }, config);
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
  console.error(results.length);
  const jsonResults = JSON.stringify(results, null, 2);
  console.log(jsonResults);
}

await main(); 