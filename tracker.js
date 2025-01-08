// @ts-check
import { Bills, Members} from './api.js';

function sortByDistrict(a, b) {
  return a.District.localeCompare(b.District);
}

function simplifyMember(member) {
  return member;
  return {
    // "Code": "ORT",
    Code: member.Code,
    // "UID": 0,
    // "LastName": "Ortiz",
    LastName: member.LastName,
    // "MiddleName": "",
    // "FirstName": "Dan",
    FirstName: member.FirstName,
    // "FormalName": "Representative Dan Ortiz",
    // "ShortName": "Ortiz               ",
    // "SessionContact": {
    //     "tollFree": null,
    //     "Street": null,
    //     "Room": null,
    //     "City": null,
    //     "State": null,
    //     "Zip": null,
    //     "Phone": null,
    //     "Fax": null,
    //     "POBox": null
    // },
    // "InterimContact": {
    //     "tollFree": null,
    //     "Street": null,
    //     "Room": null,
    //     "City": null,
    //     "State": null,
    //     "Zip": null,
    //     "Phone": null,
    //     "Fax": null,
    //     "POBox": null
    // },
    // "Chamber": "H",
    Chamber: member.Chamber,
    // "District": "1",
    District: member.District,
    // "Seat": " ",
    // "Party": "N",
    Party: member.Party,
    // "Phone": "4653824",
    // "EMail": "Representative.Dan.Ortiz@akleg.gov",
    // "Building": "CAPITOL",
    // "Room": "500",
    // "Comment": "",
    // "IsActive": true,
    // "IsMajority": false,
    IsMajority: member.IsMajority,
  };
}

async function fetchVotesForMember(memberCode) {
  const queries = {
    members: { code: memberCode },
    votes: {}
  };
  const members = await (new Members({session: 33, queries})).fetch();
  
  /** @type {import('./api.js').Vote[]} */
  // @ts-ignore  this thinks the type is BasicMember, but it's FullMember
  let votes = members[0].Votes;
  votes = votes.filter((vote) => vote.Bill !== null);
  return votes;
}

async function main() {
  const allMembers = await (new Members({session: 33})).fetch();
  console.error(allMembers.length);
  const prunedMembers = allMembers.map(simplifyMember);
  prunedMembers.sort(sortByDistrict);
  for (const member of prunedMembers) {
    console.error(`${member.FormalName} (${member.District})`);
  }
  const voteFetches = prunedMembers.slice(0, 3).map((member) => fetchVotesForMember(member.Code));

  let allVotes = [];
  for (const result of await Promise.all(voteFetches)) {
    allVotes.push(...result);
  }
  console.error(allVotes.length);

  console.log(JSON.stringify(allVotes, null, 2));
}

main();