/* Generated from schema/basis.schema.json by scripts/gen-types.ts. Do not edit by hand. */

/**
 * 'H' for House, 'S' for Senate.
 *
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "Chamber".
 */
export type Chamber = 'H' | 'S';
/**
 * A date as yyyy-mm-dd, eg '2025-01-22'. Some very old records use other formats, eg 'Jul -10- 1'.
 *
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "IsoDate".
 */
export type IsoDate = string;
/**
 * A Microsoft JSON date, eg '/Date(1737450000000)/' (milliseconds since 1970).
 *
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "MsDate".
 */
export type MsDate = string;

/**
 * A link to (or the inline content of) a document. With fulltext=urlonly only Url is set.
 *
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "Document".
 */
export interface Document {
  Url: string | null;
  Mime: string | null;
  Encoding: string | null;
  Data: string | null;
}
/**
 * A Warning or Error element. The API returns these as XML even when JSON was requested.
 *
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "Fault".
 */
export interface Fault {
  /**
   * eg 'FaultException' or 'BaoSystemFault'
   */
  Code: string;
  /**
   * eg 'Invalid Session Number.'
   */
  Description: string;
}
/**
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "Committee".
 */
export interface Committee {
  Chamber: Chamber;
  /**
   * Three letter code, eg 'HSS' for Health & Social Services.
   */
  Code: string;
  /**
   * Note the API's spelling. eg 'Standing Committee'.
   */
  Catagory: string;
  /**
   * eg 'HEALTH & SOCIAL SERVICES'
   */
  Name: string;
  /**
   * eg 'T TH SA'
   */
  MeetingDays: string;
  /**
   * eg 'JNUCAP106'
   */
  Location: string;
  /**
   * eg '3:15 PM', or ''
   */
  StartTime: string;
  /**
   * eg '000000' (appears to mean unset)
   */
  EndTime: string;
  Email: string;
  /**
   * Present on the committees endpoint; filled with the 'meetings' include.
   */
  Meetings?: Meeting[];
  /**
   * With the 'members' include.
   */
  Membership?: CommitteeMembership[];
  /**
   * With the 'members' include.
   */
  PastMembership?: CommitteeMembership[];
  /**
   * With the 'bills' include.
   */
  BillSponsorships?: Bill[];
  /**
   * With the 'bills' include.
   */
  BillsInCommittee?: Bill[];
  /**
   * With the 'bills' include.
   */
  BillsNextReferral?: Bill[];
  /**
   * With the 'bills' include.
   */
  BillsReferredOutOfCommittee?: Bill[];
  /**
   * With the 'bills' include.
   */
  BillsReferredToCommittee?: Bill[];
}
/**
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "Meeting".
 */
export interface Meeting {
  /**
   * Missing on some meetings nested in committees.
   */
  Id?: number;
  MediaFiles: MediaFile[];
  AudioFiles: MediaFile[];
  VideoFiles: MediaFile[];
  Media?: Document;
  /**
   * With the 'minutes' include. Shape not yet observed.
   */
  Minutes?: {
    [k: string]: unknown;
  };
  MeetingDate: IsoDate;
  /**
   * eg '15:15:00'
   */
  MeetingTime: string;
  MeetingTitle: string;
  MeetingChangeText: string;
  MeetingCanceled: boolean;
  MeetingPostponed: boolean;
  BillChangeText: string;
  BillinCommittee: boolean;
  Location: string;
  /**
   * Committee code, eg 'HSS'
   */
  MeetingSponsor: string;
  /**
   * eg 'Standing Committee'
   */
  SponsorType: string;
  Chamber: Chamber;
  HasDocuments: boolean;
  HasVideo: boolean;
  HasAudio: boolean;
  HasMedia: boolean;
  HasMinutes: boolean;
  HasSchedule: boolean;
  Url: string;
}
/**
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "MediaFile".
 */
export interface MediaFile {
  /**
   * eg '/Date(1736451596000)/'
   */
  StartTime: string | null;
  Length: number;
  /**
   * eg 'V' for video, 'A' for audio
   */
  MediaType: string;
  Duration: number;
  Url: string | null;
  Mime: string | null;
  Encoding: string | null;
  Data: string | null;
}
/**
 * A member's seat on a committee.
 *
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "CommitteeMembership".
 */
export interface CommitteeMembership {
  /**
   * eg 'C0' (chair), 'C1' (co-chair), 'VC', '01'
   */
  Position: string;
  CommitteeName: string;
  /**
   * eg 'Representative Genevieve Mina'
   */
  MemberName: string;
  MemberCode: string;
  MemberChamber: Chamber;
  CommitteeChamber: Chamber;
  CommitteeCode: string;
  /**
   * eg 'Resigned November 14, 2025', or ''
   */
  MemberComment: string;
}
/**
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "Bill".
 */
export interface Bill {
  /**
   * Space padded, eg 'HB  16'
   */
  BillNumber: string;
  /**
   * eg 'CSHB 16(STA) AM S(EFD FLD H)', or ''
   */
  BillName: string;
  ShortTitle: string;
  FullTitle?: string;
  StatusCode: string;
  /**
   * eg '(S) FIN' or 'VETOED BY GOVERNOR'
   */
  StatusText: string;
  StatusDate: IsoDate;
  StatusAndThen: unknown[];
  StatusSummaryCode: string;
  Flag1: string;
  Flag2: string;
  OnFloor: string;
  NotKnown: string;
  Filler: string;
  Lock: string;
  PartialVeto: boolean;
  Vetoed: boolean;
  /**
   * Link to the sponsor statement, or ''
   */
  SponsorUrl: string;
  CurrentCommittee?: Committee;
  Enacted?: Document;
  Documents: unknown[];
  AllMeetings: Meeting[];
  Meetings: Meeting[];
  ManifestErrors: Action[];
  Statutes: unknown[];
  /**
   * With the 'subjects' include, eg ['ELECTIONS']
   */
  Subjects?: string[];
  /**
   * With the 'sponsors' include.
   */
  Sponsors?: Sponsor[];
  /**
   * With the 'versions' include.
   */
  Versions?: BillVersion[];
  /**
   * With the 'actions' include.
   */
  Actions?: Action[];
  /**
   * With the 'fiscalnotes' include.
   */
  FiscalNotes?: FiscalNote[];
  /**
   * With the 'votes' include.
   */
  Votes?: Vote[];
}
/**
 * A journal entry about a bill.
 *
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "Action".
 */
export interface Action {
  /**
   * eg 'PREFILE RELEASED 1/10/25'
   */
  Text: string;
  Chamber: Chamber;
  JrnDate: IsoDate;
  JrnPage: string;
  Code: string;
  Seq: string;
  LinkActive: boolean;
  Document?: Document;
}
/**
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "Sponsor".
 */
export interface Sponsor {
  /**
   * eg 'HB  16'
   */
  BillRoot: string;
  SponsoringMember: Member | null;
  SponsoringCommittee: Committee | string | null;
  Chamber: Chamber;
  Requestor: string;
  /**
   * Space padded
   */
  Name: string;
  /**
   * eg '01'
   */
  SponsorSeq: string;
  isPrime: boolean;
}
/**
 * A legislator's seat in one legislature. The same person usually has a different Code in different legislatures.
 *
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "Member".
 */
export interface Member {
  /**
   * Three letter code, eg 'FRB'
   */
  Code: string;
  /**
   * Always 0 in practice.
   */
  UID: number;
  LastName: string;
  /**
   * '' if unset
   */
  MiddleName: string;
  FirstName: string;
  /**
   * eg 'Representative Robyn Niayuq Frier'
   */
  FormalName: string;
  /**
   * Space padded, eg 'Frier               '
   */
  ShortName: string;
  Chamber: Chamber;
  /**
   * House: '1'..'40'. Senate: 'A'..'T'.
   */
  District: string;
  Seat: string;
  /**
   * eg 'D', 'R', 'N', 'I'
   */
  Party: string;
  Phone: string;
  EMail: string;
  Building: string;
  Room: string;
  /**
   * eg 'Deceased; April 10, 2012', or ''
   */
  Comment: string;
  IsActive: boolean;
  IsMajority: boolean;
  Hometown?: string;
  SessionContact?: Contact;
  InterimContact?: Contact;
  /**
   * With the 'committees' include.
   */
  CommitteeMemberships?: CommitteeMembership[];
  /**
   * With the 'committees' include.
   */
  PastCommitteeMemberships?: CommitteeMembership[];
  /**
   * With the 'bills' include.
   */
  BillSponsorships?: Bill[];
  /**
   * With the 'votes' include.
   */
  Votes?: Vote[];
}
/**
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "Contact".
 */
export interface Contact {
  City?: string | null;
  Fax?: string | null;
  POBox?: string | null;
  Phone?: string | null;
  Room?: string | null;
  State?: string | null;
  Street?: string | null;
  Zip?: string | null;
  tollFree?: string | null;
}
/**
 * One legislator's choice on one roll call.
 *
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "Vote".
 */
export interface Vote {
  /**
   * Space padded: 'Y ' yea, 'N ' nay, 'E ' excused, 'A ' absent.
   */
  Vote: string;
  /**
   * Chamber + number, eg 'H0396'
   */
  VoteNum: string;
  /**
   * eg 'SB 113'. null when the vote isn't on a bill, eg a confirmation.
   */
  Bill: string | null;
  /**
   * Member code, eg 'FRB'
   */
  Member: string;
  MemberParty: string;
  MemberChamber: Chamber;
  /**
   * Space padded last name
   */
  MemberName: string;
  /**
   * Lines separated by \r\n, eg 'CSHB 16(STA)\r\nSecond Reading\r\nAmendment No. 1\r\n'
   */
  Title: string;
  VoteDate: IsoDate;
}
/**
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "BillVersion".
 */
export interface BillVersion {
  /**
   * eg 'A'
   */
  VersionLetter: string;
  Title: string;
  /**
   * eg 'CSHB 16(STA)'
   */
  Name: string;
  IntroDate: IsoDate;
  PassedHouse: string | null;
  PassedSenate: string | null;
  /**
   * eg '34-LS0217'
   */
  WorkOrder: string;
  /**
   * PDF link
   */
  Url: string | null;
  Mime: string | null;
  Encoding: string | null;
  Data: string | null;
}
/**
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "FiscalNote".
 */
export interface FiscalNote {
  /**
   * eg 'Fiscal Note 1'
   */
  Name: string;
  Preparer: string;
  /**
   * mm/dd/yyyy, eg '04/11/2025'
   */
  PublishedDate: string;
  Chamber: Chamber;
  /**
   * eg 'N'
   */
  FiscalImpact: string;
  Url: string | null;
  Mime: string | null;
  Encoding: string | null;
  Data: string | null;
}
/**
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "Journal".
 */
export interface Journal {
  Chamber: Chamber;
  JournalDate: IsoDate;
  StartPage: string;
  EndPage: string;
  /**
   * 'Y' or 'N'
   */
  IsDraft: string;
  VideoFiles: MediaFile[];
  AudioFiles: MediaFile[];
  Url: string | null;
  Mime: string | null;
  Encoding: string | null;
  Data: string | null;
}
/**
 * One regular or special session within a legislature.
 *
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "SessionDate".
 */
export interface SessionDate {
  StartDate: MsDate;
  EndDate: MsDate;
  /**
   * eg 10 for the 1st regular session
   */
  ID: number;
  /**
   * eg '1st Regular Session'
   */
  Title: string;
}
/**
 * A legislature (eg the 34th, 2025-2026). The API calls it a session.
 *
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "Session".
 */
export interface Session {
  /**
   * eg '34'
   */
  Number: string;
  /**
   * First year, eg '2025'
   */
  Year: string;
  SessionDates: SessionDate[];
  /**
   * With the 'journals' include.
   */
  Journals?: Journal[] | null;
  /**
   * Shape not yet observed (always null so far).
   */
  Laws?: {
    [k: string]: unknown;
  };
  /**
   * Shape not yet observed (always null so far).
   */
  Locations?: {
    [k: string]: unknown;
  };
  /**
   * Shape not yet observed (always null so far).
   */
  Subjects?: {
    [k: string]: unknown;
  };
  /**
   * Shape not yet observed (always null so far).
   */
  Requestors?: {
    [k: string]: unknown;
  };
  /**
   * Shape not yet observed (always null so far).
   */
  Sponsors?: {
    [k: string]: unknown;
  };
  /**
   * Shape not yet observed (always null so far).
   */
  Stats?: {
    [k: string]: unknown;
  };
  /**
   * Shape not yet observed (always null so far).
   */
  HouseStatus?: {
    [k: string]: unknown;
  };
  /**
   * Shape not yet observed (always null so far).
   */
  SenateStatus?: {
    [k: string]: unknown;
  };
}
/**
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "BillsResponse".
 */
export interface BillsResponse {
  Basis: {
    Bills: Bill[];
  };
}
/**
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "CommitteesResponse".
 */
export interface CommitteesResponse {
  Basis: {
    Committees: Committee[];
  };
}
/**
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "MeetingsResponse".
 */
export interface MeetingsResponse {
  Basis: {
    Meetings: Meeting[];
  };
}
/**
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "MembersResponse".
 */
export interface MembersResponse {
  Basis: {
    Members: Member[];
  };
}
/**
 * Note: the key is 'Session' (singular) and holds one object.
 *
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "SessionsResponse".
 */
export interface SessionsResponse {
  Basis: {
    Session: Session;
  };
}
