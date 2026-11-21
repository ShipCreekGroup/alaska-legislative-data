// Rows of the published tables, and their column order in the CSV files.

export const COLUMNS = {
  people: ['PersonId', 'PersonFullName', 'PersonFirstName', 'PersonLastName', 'PersonMiddleName', 'PersonNickName', 'PersonSuffix'],
  members: [
    'MemberId', 'LegislatureNumber', 'PersonId', 'MemberChamber', 'MemberDistrict', 'MemberIsMajority', 'MemberParty',
    'MemberCode', 'MemberFormerCodes',
  ],
  bills: ['BillId', 'LegislatureNumber', 'BillNumber', 'BillName', 'BillShortTitle', 'BillStatusDate', 'BillSubjects', 'BillVetoed'],
  votes: [
    'VoteId', 'BillId', 'VoteNumber', 'VoteDate', 'VoteTitle', 'LegislatureNumber', 'VoteChamber', 'VoteDescription',
    'VoteAmendmentNumber', 'VoteSubAmendmentNumber', 'VoteAmendmentId',
  ],
  choices: ['VoteId', 'MemberId', 'Choice'],
  bill_versions: [
    'BillVersionId', 'BillId', 'BillVersionLetter', 'BillVersionTitle', 'BillVersionName', 'BillVersionIntroDate',
    'BillVersionPassedHouse', 'BillVersionPassedSenate', 'BillVersionWorkOrder', 'BillVersionPdfUrl', 'BillVersionFullText',
  ],
} as const;

export type TableName = keyof typeof COLUMNS;

export interface PersonRow {
  /** eg 'per_robyn-burke-34th': the name first served under and the first legislature. Never changes. */
  PersonId: string;
  PersonFullName: string;
  PersonFirstName: string;
  PersonLastName: string;
  PersonMiddleName: string | null;
  PersonNickName: string | null;
  PersonSuffix: string | null;
}

export interface MemberRow {
  /** eg 'mem_34th_H_robyn-burke-34th' */
  MemberId: string;
  LegislatureNumber: number;
  PersonId: string;
  MemberChamber: 'H' | 'S';
  MemberDistrict: string | null;
  MemberIsMajority: boolean | null;
  MemberParty: string | null;
  /** The member's three-letter code in this legislature, eg 'FRB'. */
  MemberCode: string | null;
  /** Earlier codes in the same legislature, separated by ';' (eg after a name change). */
  MemberFormerCodes: string | null;
}

export interface BillRow {
  /** eg 'bil_34th_HB0016' */
  BillId: string;
  LegislatureNumber: number;
  /** eg 'HB 16' */
  BillNumber: string;
  BillName: string | null;
  BillShortTitle: string | null;
  BillStatusDate: string | null;
  BillSubjects: string[];
  BillVetoed: boolean | null;
}

export interface VoteRow {
  /** eg 'vot_34th_H0396' */
  VoteId: string;
  BillId: string | null;
  VoteNumber: number;
  VoteDate: string | null;
  VoteTitle: string | null;
  LegislatureNumber: number;
  VoteChamber: 'H' | 'S';
  VoteDescription: string;
  /** 2 for 'Amendment No. 2' and for 'Amendment No. 10 to Amendment No. 2'. */
  VoteAmendmentNumber: number | null;
  /** 10 for 'Amendment No. 10 to Amendment No. 2'; null for an amendment that isn't to another one. */
  VoteSubAmendmentNumber: number | null;
  /** eg 'amd_34th_HB0016_H002_10'. null for votes that aren't on an amendment to a bill. */
  VoteAmendmentId: string | null;
}

export interface ChoiceRow {
  VoteId: string;
  MemberId: string;
  /** 'Y' yea, 'N' nay, 'E' excused, 'A' absent. */
  Choice: string | null;
}

export interface BillVersionRow {
  /** eg 'ver_34th_HB0016_A' */
  BillVersionId: string;
  BillId: string;
  BillVersionLetter: string;
  BillVersionTitle: string;
  BillVersionName: string;
  BillVersionIntroDate: string | null;
  BillVersionPassedHouse: string | null;
  BillVersionPassedSenate: string | null;
  BillVersionWorkOrder: string;
  BillVersionPdfUrl: string | null;
  BillVersionFullText: string | null;
}
