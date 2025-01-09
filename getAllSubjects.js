// @ts-check
// 
// Looks through all bills for the last 13 years and prints all unique subjects.
import { Bills} from './api.js';

async function getSubjectsForSession(session) {
  const allBills = await new Bills({session, queries:{subjects:{}}}).fetch();
  const subjects = allBills.flatMap((bill) => bill.Subjects || []);
  return subjects;
}

async function getAllBillSubjects() {
  const requests = [];
  for (let session=20; session<=33; session++) {
      requests.push(getSubjectsForSession(session));
  }
  const results = await Promise.all(requests);
  const allSubjects = results.flat();
  const uniqueSubjects = [...new Set(allSubjects)];
  const sortedSubjects = uniqueSubjects.sort();
  return sortedSubjects;
  }

  async function printAllBillSubjects() {
  const allSubjects = await getAllBillSubjects();
  for (const subject of allSubjects) {
      console.log(subject);
  }
}

printAllBillSubjects();