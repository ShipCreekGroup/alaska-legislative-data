export type Choice = 'Y' | 'N' | 'E' | 'A';
export type Chamber = 'H' | 'S';

export interface Tally {
  y: number;
  n: number;
  e: number;
  a: number;
}

export interface Meta {
  updated: string;
  bills: number;
  votes: number;
  choices: number;
  people: number;
  versions: number;
  legislatures: { number: number; bills: number; votes: number; members: number }[];
}

export interface VoteRow extends Tally {
  id: string;
  date: string | null;
  chamber: Chamber;
  number: number;
  title: string | null;
  description: string;
  billId?: string | null;
  billNumber?: string | null;
  billTitle?: string | null;
  amendment?: number | null;
  subAmendment?: number | null;
}

export interface BillRow {
  id: string;
  number: string;
  title: string | null;
  statusDate: string | null;
  vetoed: boolean | null;
  subjects: string[] | null;
}

export interface MemberRow {
  id: string;
  personId: string;
  name: string;
  lastName: string;
  chamber: Chamber;
  district: string | null;
  party: string | null;
  majority: boolean | null;
  code: string | null;
}

export interface Legislature {
  number: number;
  bills: BillRow[];
  votes: VoteRow[];
  members: MemberRow[];
}

export interface VersionRow {
  id: string;
  letter: string;
  title: string;
  name: string;
  introDate: string;
  passedHouse: string | null;
  passedSenate: string | null;
  pdfUrl: string;
  hasText: boolean;
}

export interface Bill {
  id: string;
  legislature: number;
  number: string;
  name: string | null;
  title: string | null;
  statusDate: string | null;
  subjects: string[] | null;
  vetoed: boolean | null;
  versions: VersionRow[];
  votes: VoteRow[];
}

export interface BillVersion {
  id: string;
  billId: string;
  legislature: number;
  billNumber: string;
  billTitle: string | null;
  letter: string;
  title: string;
  name: string;
  introDate: string;
  passedHouse: string | null;
  passedSenate: string | null;
  workOrder: string;
  pdfUrl: string;
  fullText: string | null;
}

export interface ChoiceRow {
  memberId: string;
  personId: string;
  name: string;
  lastName: string;
  party: string | null;
  district: string | null;
  majority: boolean | null;
  choice: Choice;
}

export interface Vote extends VoteRow {
  legislature: number;
  amendmentId: string | null;
  choices: ChoiceRow[];
}

export interface MemberVote {
  id: string;
  date: string | null;
  number: number;
  title: string | null;
  billId: string | null;
  billNumber: string | null;
  billTitle: string | null;
  choice: Choice;
  y: number;
  n: number;
}

export interface Term {
  id: string;
  legislature: number;
  chamber: Chamber;
  district: string | null;
  party: string | null;
  majority?: boolean | null;
  code?: string | null;
  y?: number;
  n?: number;
  missed?: number;
}

export interface Member extends Tally {
  id: string;
  legislature: number;
  personId: string;
  name: string;
  firstName: string;
  lastName: string;
  chamber: Chamber;
  district: string | null;
  party: string | null;
  majority: boolean | null;
  code: string | null;
  otherTerms: Term[];
  votes: MemberVote[];
}

export interface Person {
  id: string;
  name: string;
  firstName: string;
  lastName: string;
  middleName: string | null;
  nickName: string | null;
  suffix: string | null;
  terms: Term[];
}

export interface PersonIndexRow {
  id: string;
  name: string;
  lastName: string;
  first: number | null;
  last: number | null;
}

export interface Page<T> {
  items: T[];
  total: number;
  page: number;
  pages: number;
}
