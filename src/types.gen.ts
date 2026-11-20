/* Generated from schema/basis.schema.json by scripts/gen.ts. Do not edit by hand. */

/**
 * 'H' for House, 'S' for Senate.
 *
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "Chamber".
 */
export type Chamber = 'H' | 'S';
/**
 * A date as yyyy-mm-dd, eg '2025-01-22'.
 *
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "IsoDate".
 */
export type IsoDate = string;
/**
 * A time of day as HH:MM:SS, eg '15:15:00'. JSON Schema's time format requires a timezone, which the API doesn't send.
 *
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "Time".
 */
export type Time = string;
/**
 * A Microsoft JSON date, eg '/Date(1737450000000)/' (milliseconds since 1970).
 *
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "MsDate".
 */
export type MsDate = string;
/**
 * A link to, or the inline content of, a document. Objects with document fields (bill versions, fiscal notes, media files, journals, minutes) include one of these shapes.
 *
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "Document".
 */
export type Document = DocumentEmpty | DocumentUrlOnly | DocumentFull;
/**
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "MediaFile".
 */
export type MediaFile = Document & {
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
};
/**
 * A link to, or the inline content of, a document. Objects with document fields (bill versions, fiscal notes, media files, journals, minutes) include one of these shapes.
 */
export type Document1 = DocumentEmpty | DocumentUrlOnly | DocumentFull;
/**
 * A committee meeting's minutes: an index of timestamps into the recording, plus a link to the PDF.
 *
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "Minutes".
 */
export type Minutes = Document & {
  /**
   * 'J' for joint committees, eg Legislative Budget & Audit
   */
  Chamber: 'H' | 'S' | 'J';
  MinutesDate: IsoDate;
  /**
   * Start time as HHMM, eg '1401'
   */
  MinutesTime: string;
  /**
   * eg 'FIN'
   */
  CommitteeCode: string;
  TimeStamps: MinutesTimeStamp[];
  /**
   * '/Date(-62135564400000)/' (year 1) when there's no video
   */
  VideoStart: string;
  /**
   * eg 'https://www.akleg.gov/ftr/2025/20250121/sccm/sccm_1401.mp3', or ''
   */
  AudioUrl: string;
  /**
   * '' when there's no video
   */
  VideoUrl: string;
};
/**
 * A link to, or the inline content of, a document. Objects with document fields (bill versions, fiscal notes, media files, journals, minutes) include one of these shapes.
 */
export type Document2 = DocumentEmpty | DocumentUrlOnly | DocumentFull;
/**
 * A link to, or the inline content of, a document. Objects with document fields (bill versions, fiscal notes, media files, journals, minutes) include one of these shapes.
 */
export type Document3 = DocumentEmpty | DocumentUrlOnly | DocumentFull;
/**
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "BillVersion".
 */
export type BillVersion = Document & {
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
  PassedHouse: IsoDate | null;
  PassedSenate: IsoDate | null;
  /**
   * eg '34-LS0217'
   */
  WorkOrder: string;
};
/**
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "FiscalNote".
 */
export type FiscalNote = Document & {
  /**
   * eg 'Fiscal Note 1'
   */
  Name: string;
  Preparer: string;
  /**
   * MM/DD/YYYY, eg '04/11/2025'
   */
  PublishedDate: string;
  Chamber: Chamber;
  /**
   * eg 'N'
   */
  FiscalImpact: string;
};
/**
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "Journal".
 */
export type Journal = Document & {
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
};

/**
 * A document with nothing set, eg a bill version with no PDF yet.
 *
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "DocumentEmpty".
 */
export interface DocumentEmpty {
  Url: null;
  Mime: null;
  Encoding: null;
  Data: null;
}
/**
 * A link only. What fulltext=urlonly returns, and the usual shape when fulltext isn't requested.
 *
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "DocumentUrlOnly".
 */
export interface DocumentUrlOnly {
  Url: string;
  Mime: null;
  Encoding: null;
  Data: null;
}
/**
 * Inline content, with fulltext=true. The link is usually set too.
 *
 * This interface was referenced by `BasisTypes`'s JSON-Schema
 * via the `definition` "DocumentFull".
 */
export interface DocumentFull {
  Url: string | null;
  /**
   * eg 'text/plain'
   */
  Mime: string;
  /**
   * eg 'base64'
   */
  Encoding: string;
  Data: string;
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
  Media?: Document1;
  /**
   * With the 'minutes' include; null when a meeting has no minutes.
   */
  Minutes?: Minutes | null;
  MeetingDate: IsoDate;
  MeetingTime: Time;
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
 * via the `definition` "MinutesTimeStamp".
 */
export interface MinutesTimeStamp {
  /**
   * eg 'Adopt Committee on Committees Report'
   */
  Text: string;
  /**
   * Space padded, eg '14:01:39  '
   */
  Time: string;
  /**
   * Seconds into the audio recording
   */
  AudioElapsed: number;
  /**
   * eg 'S' start, 'A' adjourn, 'T' topic, 'M' member speaking, 'B' and 'E'
   */
  Type: string;
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
  /**
   * Bills in the 12th to 17th legislatures have dates like 'Jul -10- 1' or 'Mar - 2- 1'.
   */
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
  Enacted?: Document2;
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
  Document?: Document3;
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
  /**
   * A few old votes have '-199- 0--0'.
   */
  VoteDate: IsoDate | '-199- 0--0';
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
   * Always null so far, even with its include. Typed null so the contract tests notice if that changes.
   */
  Laws?: null;
  /**
   * Always null so far, even with its include. Typed null so the contract tests notice if that changes.
   */
  Locations?: null;
  /**
   * Always null so far, even with its include. Typed null so the contract tests notice if that changes.
   */
  Subjects?: null;
  /**
   * Always null so far, even with its include. Typed null so the contract tests notice if that changes.
   */
  Requestors?: null;
  /**
   * Always null so far, even with its include. Typed null so the contract tests notice if that changes.
   */
  Sponsors?: null;
  /**
   * Always null so far, even with its include. Typed null so the contract tests notice if that changes.
   */
  Stats?: null;
  /**
   * Always null so far, even with its include. Typed null so the contract tests notice if that changes.
   */
  HouseStatus?: null;
  /**
   * Always null so far, even with its include. Typed null so the contract tests notice if that changes.
   */
  SenateStatus?: null;
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
