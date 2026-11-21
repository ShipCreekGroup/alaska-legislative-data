/* Generated from schema/basis.schema.json by scripts/gen.ts. Do not edit by hand. */
import * as z from 'zod/mini';
import type * as T from './types.gen.ts';

/** 'H' for House, 'S' for Senate. */
export const Chamber: z.ZodMiniType<T.Chamber> = z.enum(['H', 'S']);

/** A date as yyyy-mm-dd, eg '2025-01-22'. */
export const IsoDate: z.ZodMiniType<T.IsoDate> = z.iso.date();

/** A time of day as HH:MM:SS, eg '15:15:00'. JSON Schema's time format requires a timezone, which the API doesn't send. */
export const Time: z.ZodMiniType<T.Time> = z.string().check(z.regex(/^([01]\d|2[0-3]):[0-5]\d:[0-5]\d$/));

/** A Microsoft JSON date, eg '/Date(1737450000000)/' (milliseconds since 1970). */
export const MsDate: z.ZodMiniType<T.MsDate> = z.string().check(z.regex(/^\/Date\(-?\d+\)\/$/));

/** A document with nothing set, eg a bill version with no PDF yet. */
export const DocumentEmpty: z.ZodMiniType<T.DocumentEmpty> = z.strictObject({
  Url: z.null(),
  Mime: z.null(),
  Encoding: z.null(),
  Data: z.null(),
});

/** A link only. What fulltext=urlonly returns, and the usual shape when fulltext isn't requested. */
export const DocumentUrlOnly: z.ZodMiniType<T.DocumentUrlOnly> = z.strictObject({
  Url: z.string(),
  Mime: z.null(),
  Encoding: z.null(),
  Data: z.null(),
});

/** Inline content, with fulltext=true. The link is usually set too. */
export const DocumentFull: z.ZodMiniType<T.DocumentFull> = z.strictObject({
  Url: z.union([z.string(), z.null()]),
  Mime: z.string(),
  Encoding: z.string(),
  Data: z.string(),
});

/** A link to, or the inline content of, a document. Objects with document fields (bill versions, fiscal notes, media files, journals, minutes) include one of these shapes. */
export const Document: z.ZodMiniType<T.Document> = z.union([DocumentEmpty, DocumentUrlOnly, DocumentFull]);

/** A Warning or Error element. The API returns these as XML even when JSON was requested. */
export const Fault: z.ZodMiniType<T.Fault> = z.strictObject({ Code: z.string(), Description: z.string() });

export const Committee: z.ZodMiniType<T.Committee> = z.strictObject({
  get Chamber() {
    return Chamber;
  },
  Code: z.string(),
  Catagory: z.string(),
  Name: z.string(),
  MeetingDays: z.string(),
  Location: z.string(),
  StartTime: z.string(),
  EndTime: z.string(),
  Email: z.string(),
  get Meetings() {
    return z.optional(z.array(Meeting));
  },
  get Membership() {
    return z.optional(z.array(CommitteeMembership));
  },
  get PastMembership() {
    return z.optional(z.array(CommitteeMembership));
  },
  get BillSponsorships() {
    return z.optional(z.array(Bill));
  },
  get BillsInCommittee() {
    return z.optional(z.array(Bill));
  },
  get BillsNextReferral() {
    return z.optional(z.array(Bill));
  },
  get BillsReferredOutOfCommittee() {
    return z.optional(z.array(Bill));
  },
  get BillsReferredToCommittee() {
    return z.optional(z.array(Bill));
  },
});

/** A member's seat on a committee. */
export const CommitteeMembership: z.ZodMiniType<T.CommitteeMembership> = z.strictObject({
  Position: z.string(),
  CommitteeName: z.string(),
  MemberName: z.string(),
  MemberCode: z.string(),
  get MemberChamber() {
    return Chamber;
  },
  get CommitteeChamber() {
    return Chamber;
  },
  CommitteeCode: z.string(),
  MemberComment: z.string(),
});

export const MediaFile: z.ZodMiniType<T.MediaFile> = z.union([
  z.strictObject({
    Url: z.null(),
    Mime: z.null(),
    Encoding: z.null(),
    Data: z.null(),
    StartTime: z.union([z.string().check(z.regex(/^\/Date\(-?\d+\)\/$/)), z.null()]),
    Length: z.int(),
    MediaType: z.string(),
    Duration: z.int(),
  }),
  z.strictObject({
    Url: z.string(),
    Mime: z.null(),
    Encoding: z.null(),
    Data: z.null(),
    StartTime: z.union([z.string().check(z.regex(/^\/Date\(-?\d+\)\/$/)), z.null()]),
    Length: z.int(),
    MediaType: z.string(),
    Duration: z.int(),
  }),
  z.strictObject({
    Url: z.union([z.string(), z.null()]),
    Mime: z.string(),
    Encoding: z.string(),
    Data: z.string(),
    StartTime: z.union([z.string().check(z.regex(/^\/Date\(-?\d+\)\/$/)), z.null()]),
    Length: z.int(),
    MediaType: z.string(),
    Duration: z.int(),
  }),
]);

export const Meeting: z.ZodMiniType<T.Meeting> = z.strictObject({
  Id: z.optional(z.int()),
  get MediaFiles() {
    return z.array(MediaFile);
  },
  get AudioFiles() {
    return z.array(MediaFile);
  },
  get VideoFiles() {
    return z.array(MediaFile);
  },
  get Media() {
    return z.optional(Document);
  },
  get Minutes() {
    return z.optional(z.union([Minutes, z.null()]));
  },
  get MeetingDate() {
    return IsoDate;
  },
  get MeetingTime() {
    return Time;
  },
  MeetingTitle: z.string(),
  MeetingChangeText: z.string(),
  MeetingCanceled: z.boolean(),
  MeetingPostponed: z.boolean(),
  BillChangeText: z.string(),
  BillinCommittee: z.boolean(),
  Location: z.string(),
  MeetingSponsor: z.string(),
  SponsorType: z.string(),
  get Chamber() {
    return Chamber;
  },
  HasDocuments: z.boolean(),
  HasVideo: z.boolean(),
  HasAudio: z.boolean(),
  HasMedia: z.boolean(),
  HasMinutes: z.boolean(),
  HasSchedule: z.boolean(),
  Url: z.string(),
});

/** A committee meeting's minutes: an index of timestamps into the recording, plus a link to the PDF. */
export const Minutes: z.ZodMiniType<T.Minutes> = z.union([
  z.strictObject({
    Url: z.null(),
    Mime: z.null(),
    Encoding: z.null(),
    Data: z.null(),
    Chamber: z.enum(['H', 'S', 'J']),
    get MinutesDate() {
      return IsoDate;
    },
    MinutesTime: z.string().check(z.regex(/^\d{4}$/)),
    CommitteeCode: z.string(),
    get TimeStamps() {
      return z.array(MinutesTimeStamp);
    },
    get VideoStart() {
      return MsDate;
    },
    AudioUrl: z.string(),
    VideoUrl: z.string(),
  }),
  z.strictObject({
    Url: z.string(),
    Mime: z.null(),
    Encoding: z.null(),
    Data: z.null(),
    Chamber: z.enum(['H', 'S', 'J']),
    get MinutesDate() {
      return IsoDate;
    },
    MinutesTime: z.string().check(z.regex(/^\d{4}$/)),
    CommitteeCode: z.string(),
    get TimeStamps() {
      return z.array(MinutesTimeStamp);
    },
    get VideoStart() {
      return MsDate;
    },
    AudioUrl: z.string(),
    VideoUrl: z.string(),
  }),
  z.strictObject({
    Url: z.union([z.string(), z.null()]),
    Mime: z.string(),
    Encoding: z.string(),
    Data: z.string(),
    Chamber: z.enum(['H', 'S', 'J']),
    get MinutesDate() {
      return IsoDate;
    },
    MinutesTime: z.string().check(z.regex(/^\d{4}$/)),
    CommitteeCode: z.string(),
    get TimeStamps() {
      return z.array(MinutesTimeStamp);
    },
    get VideoStart() {
      return MsDate;
    },
    AudioUrl: z.string(),
    VideoUrl: z.string(),
  }),
]);

export const MinutesTimeStamp: z.ZodMiniType<T.MinutesTimeStamp> = z.strictObject({
  Text: z.string(),
  Time: z.string().check(z.regex(/^([01]\d|2[0-3]):[0-5]\d:[0-5]\d *$/)),
  AudioElapsed: z.int(),
  Type: z.string(),
});

export const Contact: z.ZodMiniType<T.Contact> = z.strictObject({
  City: z.optional(z.union([z.string(), z.null()])),
  Fax: z.optional(z.union([z.string(), z.null()])),
  POBox: z.optional(z.union([z.string(), z.null()])),
  Phone: z.optional(z.union([z.string(), z.null()])),
  Room: z.optional(z.union([z.string(), z.null()])),
  State: z.optional(z.union([z.string(), z.null()])),
  Street: z.optional(z.union([z.string(), z.null()])),
  Zip: z.optional(z.union([z.string(), z.null()])),
  tollFree: z.optional(z.union([z.string(), z.null()])),
});

/** A legislator's seat in one legislature. The same person usually has a different Code in different legislatures. */
export const Member: z.ZodMiniType<T.Member> = z.strictObject({
  Code: z.string(),
  UID: z.int(),
  LastName: z.string(),
  MiddleName: z.string(),
  FirstName: z.string(),
  FormalName: z.string(),
  ShortName: z.string(),
  get Chamber() {
    return Chamber;
  },
  District: z.string(),
  Seat: z.string(),
  Party: z.string(),
  Phone: z.string(),
  EMail: z.string(),
  Building: z.string(),
  Room: z.string(),
  Comment: z.string(),
  IsActive: z.boolean(),
  IsMajority: z.boolean(),
  Hometown: z.optional(z.string()),
  get SessionContact() {
    return z.optional(Contact);
  },
  get InterimContact() {
    return z.optional(Contact);
  },
  get CommitteeMemberships() {
    return z.optional(z.array(CommitteeMembership));
  },
  get PastCommitteeMemberships() {
    return z.optional(z.array(CommitteeMembership));
  },
  get BillSponsorships() {
    return z.optional(z.array(Bill));
  },
  get Votes() {
    return z.optional(z.array(Vote));
  },
});

/** One legislator's choice on one roll call. */
export const Vote: z.ZodMiniType<T.Vote> = z.strictObject({
  Vote: z.string(),
  VoteNum: z.string(),
  Bill: z.union([z.string(), z.null()]),
  Member: z.string(),
  MemberParty: z.string(),
  get MemberChamber() {
    return Chamber;
  },
  MemberName: z.string(),
  Title: z.string(),
  get VoteDate() {
    return z.union([IsoDate, z.literal('-199- 0--0')]);
  },
});

/** A journal entry about a bill. */
export const Action: z.ZodMiniType<T.Action> = z.strictObject({
  Text: z.string(),
  get Chamber() {
    return Chamber;
  },
  get JrnDate() {
    return IsoDate;
  },
  JrnPage: z.string(),
  Code: z.string(),
  Seq: z.string(),
  LinkActive: z.boolean(),
  get Document() {
    return z.optional(Document);
  },
});

export const Sponsor: z.ZodMiniType<T.Sponsor> = z.strictObject({
  BillRoot: z.string(),
  get SponsoringMember() {
    return z.union([Member, z.null()]);
  },
  get SponsoringCommittee() {
    return z.union([Committee, z.string(), z.null()]);
  },
  get Chamber() {
    return Chamber;
  },
  Requestor: z.string(),
  Name: z.string(),
  SponsorSeq: z.string(),
  isPrime: z.boolean(),
});

export const BillVersion: z.ZodMiniType<T.BillVersion> = z.union([
  z.strictObject({
    Url: z.null(),
    Mime: z.null(),
    Encoding: z.null(),
    Data: z.null(),
    VersionLetter: z.string(),
    Title: z.string(),
    Name: z.string(),
    get IntroDate() {
      return IsoDate;
    },
    get PassedHouse() {
      return z.union([IsoDate, z.null()]);
    },
    get PassedSenate() {
      return z.union([IsoDate, z.null()]);
    },
    WorkOrder: z.string(),
  }),
  z.strictObject({
    Url: z.string(),
    Mime: z.null(),
    Encoding: z.null(),
    Data: z.null(),
    VersionLetter: z.string(),
    Title: z.string(),
    Name: z.string(),
    get IntroDate() {
      return IsoDate;
    },
    get PassedHouse() {
      return z.union([IsoDate, z.null()]);
    },
    get PassedSenate() {
      return z.union([IsoDate, z.null()]);
    },
    WorkOrder: z.string(),
  }),
  z.strictObject({
    Url: z.union([z.string(), z.null()]),
    Mime: z.string(),
    Encoding: z.string(),
    Data: z.string(),
    VersionLetter: z.string(),
    Title: z.string(),
    Name: z.string(),
    get IntroDate() {
      return IsoDate;
    },
    get PassedHouse() {
      return z.union([IsoDate, z.null()]);
    },
    get PassedSenate() {
      return z.union([IsoDate, z.null()]);
    },
    WorkOrder: z.string(),
  }),
]);

export const FiscalNote: z.ZodMiniType<T.FiscalNote> = z.union([
  z.strictObject({
    Url: z.null(),
    Mime: z.null(),
    Encoding: z.null(),
    Data: z.null(),
    Name: z.string(),
    Preparer: z.string(),
    PublishedDate: z.string().check(z.regex(/^(0[1-9]|1[0-2])\/(0[1-9]|[12]\d|3[01])\/\d{4}$/)),
    get Chamber() {
      return Chamber;
    },
    FiscalImpact: z.string(),
  }),
  z.strictObject({
    Url: z.string(),
    Mime: z.null(),
    Encoding: z.null(),
    Data: z.null(),
    Name: z.string(),
    Preparer: z.string(),
    PublishedDate: z.string().check(z.regex(/^(0[1-9]|1[0-2])\/(0[1-9]|[12]\d|3[01])\/\d{4}$/)),
    get Chamber() {
      return Chamber;
    },
    FiscalImpact: z.string(),
  }),
  z.strictObject({
    Url: z.union([z.string(), z.null()]),
    Mime: z.string(),
    Encoding: z.string(),
    Data: z.string(),
    Name: z.string(),
    Preparer: z.string(),
    PublishedDate: z.string().check(z.regex(/^(0[1-9]|1[0-2])\/(0[1-9]|[12]\d|3[01])\/\d{4}$/)),
    get Chamber() {
      return Chamber;
    },
    FiscalImpact: z.string(),
  }),
]);

export const Bill: z.ZodMiniType<T.Bill> = z.strictObject({
  BillNumber: z.string(),
  BillName: z.string(),
  ShortTitle: z.string(),
  FullTitle: z.optional(z.string()),
  StatusCode: z.string(),
  StatusText: z.string(),
  get StatusDate() {
    return z.union([IsoDate, z.string().check(z.regex(/^[A-Z][a-z]{2} -[ \d]\d- \d$/))]);
  },
  StatusAndThen: z.array(z.unknown()),
  StatusSummaryCode: z.string(),
  Flag1: z.string(),
  Flag2: z.string(),
  OnFloor: z.string(),
  NotKnown: z.string(),
  Filler: z.string(),
  Lock: z.string(),
  PartialVeto: z.boolean(),
  Vetoed: z.boolean(),
  SponsorUrl: z.string(),
  get CurrentCommittee() {
    return z.optional(Committee);
  },
  get Enacted() {
    return z.optional(Document);
  },
  Documents: z.array(z.unknown()),
  get AllMeetings() {
    return z.array(Meeting);
  },
  get Meetings() {
    return z.array(Meeting);
  },
  get ManifestErrors() {
    return z.array(Action);
  },
  Statutes: z.array(z.unknown()),
  Subjects: z.optional(z.array(z.string())),
  get Sponsors() {
    return z.optional(z.array(Sponsor));
  },
  get Versions() {
    return z.optional(z.array(BillVersion));
  },
  get Actions() {
    return z.optional(z.array(Action));
  },
  get FiscalNotes() {
    return z.optional(z.array(FiscalNote));
  },
  get Votes() {
    return z.optional(z.array(Vote));
  },
});

export const Journal: z.ZodMiniType<T.Journal> = z.union([
  z.strictObject({
    Url: z.null(),
    Mime: z.null(),
    Encoding: z.null(),
    Data: z.null(),
    get Chamber() {
      return Chamber;
    },
    get JournalDate() {
      return IsoDate;
    },
    StartPage: z.string(),
    EndPage: z.string(),
    IsDraft: z.string(),
    get VideoFiles() {
      return z.array(MediaFile);
    },
    get AudioFiles() {
      return z.array(MediaFile);
    },
  }),
  z.strictObject({
    Url: z.string(),
    Mime: z.null(),
    Encoding: z.null(),
    Data: z.null(),
    get Chamber() {
      return Chamber;
    },
    get JournalDate() {
      return IsoDate;
    },
    StartPage: z.string(),
    EndPage: z.string(),
    IsDraft: z.string(),
    get VideoFiles() {
      return z.array(MediaFile);
    },
    get AudioFiles() {
      return z.array(MediaFile);
    },
  }),
  z.strictObject({
    Url: z.union([z.string(), z.null()]),
    Mime: z.string(),
    Encoding: z.string(),
    Data: z.string(),
    get Chamber() {
      return Chamber;
    },
    get JournalDate() {
      return IsoDate;
    },
    StartPage: z.string(),
    EndPage: z.string(),
    IsDraft: z.string(),
    get VideoFiles() {
      return z.array(MediaFile);
    },
    get AudioFiles() {
      return z.array(MediaFile);
    },
  }),
]);

/** One regular or special session within a legislature. */
export const SessionDate: z.ZodMiniType<T.SessionDate> = z.strictObject({
  get StartDate() {
    return MsDate;
  },
  get EndDate() {
    return MsDate;
  },
  ID: z.int(),
  Title: z.string(),
});

/** A legislature (eg the 34th, 2025-2026). The API calls it a session. */
export const Session: z.ZodMiniType<T.Session> = z.strictObject({
  Number: z.string(),
  Year: z.string(),
  get SessionDates() {
    return z.array(SessionDate);
  },
  get Journals() {
    return z.optional(z.union([z.array(Journal), z.null()]));
  },
  Laws: z.optional(z.null()),
  Locations: z.optional(z.null()),
  Subjects: z.optional(z.null()),
  Requestors: z.optional(z.null()),
  Sponsors: z.optional(z.null()),
  Stats: z.optional(z.null()),
  HouseStatus: z.optional(z.null()),
  SenateStatus: z.optional(z.null()),
});

export const BillsResponse: z.ZodMiniType<T.BillsResponse> = z.strictObject({
  Basis: z.strictObject({
    get Bills() {
      return z.array(Bill);
    },
  }),
});

export const CommitteesResponse: z.ZodMiniType<T.CommitteesResponse> = z.strictObject({
  Basis: z.strictObject({
    get Committees() {
      return z.array(Committee);
    },
  }),
});

export const MeetingsResponse: z.ZodMiniType<T.MeetingsResponse> = z.strictObject({
  Basis: z.strictObject({
    get Meetings() {
      return z.array(Meeting);
    },
  }),
});

export const MembersResponse: z.ZodMiniType<T.MembersResponse> = z.strictObject({
  Basis: z.strictObject({
    get Members() {
      return z.array(Member);
    },
  }),
});

/** Note: the key is 'Session' (singular) and holds one object. */
export const SessionsResponse: z.ZodMiniType<T.SessionsResponse> = z.strictObject({
  Basis: z.strictObject({
    get Session() {
      return Session;
    },
  }),
});
