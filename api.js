// @ts-check
/** @typedef {'H' | 'S'} Chamber */
/** @typedef {true | 'urlonly'} Fulltext Whether to return a {@link UrlDocument} or a {@link RawDocument}*/
/** @typedef {'GET' | 'HEAD'} HttpMethod */
/** @typedef {string} Party there is no restriction on this */
/** @typedef {'A ' | 'Y ' | 'N ' | 'E '} VoteChoice */
/** @typedef {string} MemberCode  eg 'BIS' for Click Bishop, 'BCH' for Tom Begich, etc */
/** @typedef {string} BillCode  eg 'HB   7' */
/** @typedef {string} CommitteeCode  a three letter code eg 'HSS' for Health and Social Services */
/** @typedef {string} yyyymmdd  A date in the format yyyy-mm-dd eg '2025-01-08' */

/**
 * @typedef {Object} ActionConstraints
 * @property {string} [code]
 * @property {string} [text]
 * @property {string} [date]
 * @property {string} [startdate]
 * @property {string} [enddate]
 */

/**
 * Filters that apply to a {@link Bill}
 * You can use this when using the {@link Bills} class to fetch bills,
 * but you can also use it when filtering the results for one of the related
 * endpoints, eg as a filter within a
 * {@link MemberQueries} when using the {@link Members} class,
 * or as a filter within a {@link VoteQueries} when using the {@link Votes} class.
 * 
 * @typedef {Object} BillConstraints
 * @property {string} [name] The bill name, eg 'CSHB 2(STA)(FLD H)'
 * @property {Chamber} [chamber] 'H' for House, 'S' for Senate
 * @property {BillCode} [bill] The bill number, eg 'HB  6'
 * @property {boolean} [onfloor] 
 * @property {string} [shorttitle] eg 'CONTRACTS: PROHIBIT ISRAEL DISCRIMINATION'
 * @property {string} [statustext] eg '014'
 * @property {string} [statuscode] eg 'FAILED(H) PERM FILED(H)'
 * @property {CommitteeCode} [committeecode] The code of the committee that the bill is 
 *   currently in, eg 'STA' for the 'State Affairs' committee.
 */

/**
 * @typedef {Object} CommitteeConstraints
 * @property {string} [category]
 * @property {Chamber} [chamber]
 * @property {string} [location]
 * @property {string} [name]
 * @property {string} [title]
 * @property {CommitteeCode} [code]
 */

/**
 * @typedef {Object} FiscalNoteConstraints
 * @property {Chamber} [chamber]
 * @property {string} [date]
 * @property {string} [startdate]
 * @property {string} [enddate]
 * @property {string} [startpage]
 * @property {string} [endpage]
 */

/**
 * @typedef {Object} JournalConstraints
 * @property {Chamber} [chamber]
 * @property {string} [date]
 * @property {string} [startdate]
 * @property {string} [enddate]
 * @property {string} [page]
 * @property {string} [startpage]
 * @property {string} [endpage]
 */

/**
 * @typedef {Object} MeetingConstraints
 * @property {string} [date]
 * @property {string} [startdate]
 * @property {string} [enddate]
 * @property {string} [details]
 */

/**
 * @typedef {Object} MemberConstraints
 * @property {string} [building]
 * @property {Chamber} [chamber]
 * @property {string} [comment]
 * @property {string} [district]
 * @property {Party} [party]
 * @property {string} [phone]
 * @property {string} [firstname]
 * @property {string} [lastname]
 * @property {boolean} [ismajority]
 * @property {MemberCode} [code] three letter code, eg 'BIS' for Click Bishop, 'BCH' for Tom Begich, etc
 */

/** @typedef {Object} MediaConstraints */

/**
 * @typedef {Object} MinuteConstraints
 * @property {Fulltext} [fulltext] Whether to return a {@link UrlDocument} or a {@link RawDocument}
 */

/** 
 * This is an almost-definitely incomplete list of subjects.
 * I got this from just looking at the subjects of 30 random bills.
 * @typedef {'ATTORNEY GENERAL' | 'INSURANCE' | 'INTERGOVERNMENTAL RELATIONS' | 'MEDICAL CARE' | 'FORESTRY' | 'FUNDS' | 'INVESTMENTS' | 'OIL & GAS' | 'PERMANENT FUND' | 'PUBLIC EMPLOYEES' | 'PUBLIC FINANCE' | 'RETIREMENT' | 'REVENUE' | 'RIGHTS' | 'SALARIES & ALLOWANCES' | 'ALIENS' | 'LICENSING' | 'MOTOR VEHICLES' | 'RESIDENCY' | 'TAXATION' | 'TRANSPORTATION' | 'WATER' | 'BOARDS & COMMISSIONS' | 'CONFLICT OF INTEREST' | 'ETHICS' | 'EXECUTIVE BRANCH' | 'GOVERNOR' | 'POLITICAL PARTIES' | 'PUBLIC EMPLOYEES' | 'PUBLIC FINANCE' | 'RETIREMENT' | 'REVENUE' | 'RIGHTS' | 'SALARIES & ALLOWANCES' | 'ALIENS' | 'LICENSING' | 'MOTOR VEHICLES' | 'RESIDENCY' | 'TAXATION' | 'TRANSPORTATION' | 'WATER' | 'ARTS & HUMANITIES' | 'EDUCATION' | 'SCHOOL DISTRICTS' | 'HEARINGS' | 'CRIMES' | 'DRUGS' | 'LEGISLATIVE COMMITTEES' | 'BONDS & BONDING' | 'CAPITAL PROJECTS' | 'ENERGY' | 'GOVERNMENT ORGANIZATION' | 'GOVERNOR' | 'HOUSING' | 'LEASES' | 'LIABILITY' | 'MUNICIPALITIES' | 'OIL & GAS' | 'PERMITS' | 'PIPELINES' | 'PLANNING' | 'PROCUREMENTS' | 'PUBLIC CORPORATIONS' | 'PUBLIC RECORDS' | 'PUBLIC UTILITIES' | 'TAXATION' | 'FEES'} Subject
 */

/**
 * @typedef {Object} SubjectConstraints
 * @property {Subject} [text]
 */

/**
 * @typedef {Object} VersionConstraints
 * @property {Fulltext} [fulltext] Whether to return a {@link UrlDocument} or a {@link RawDocument}
 */

/**
 * @typedef {Object} VoteConstraints
 * @property {string} [vote]
 * @property {string} [title]
 */

/**
 * @typedef {MemberConstraints} SponsorConstraints
 */

/**
 * Filters that you can use when selecting which {@link Bill} to fetch
 * using the {@link Bills} class.
 * @typedef {Object} BillQueries
 * @property {ActionConstraints} [actions]  Filter based on actions taken on this bill
 * @property {BillConstraints} [bills]  Filter based on attributes of this bill
 * @property {FiscalNoteConstraints} [fiscalnotes]  Filter based on fiscal notes of this bill
 * @property {SponsorConstraints} [sponsors]  Filter based on sponsors of this bill
 * @property {SubjectConstraints} [subjects]  Filter based on subjects of this bill
 * @property {VersionConstraints} [versions]  Filter based on versions of this bill
 * @property {VoteConstraints} [votes]  Filter based on votes on this bill
 */

/**
 * Filters that you can use when selecting which {@link Committee} to fetch
 * using the {@link Committees} class.
 * @typedef {Object} CommitteeQueries
 * @property {BillConstraints} [bills]  Filter based on bills in this committee
 * @property {CommitteeConstraints} [committees]  Filter based on attributes of this committee
 * @property {MeetingConstraints} [meetings]  Filter based on meetings of this committee
 * @property {MemberConstraints} [members]  Filter based on members of this committee
 */

/**
 * Filters that you can use when selecting which {@link FullMember} to fetch
 * using the {@link Members} class.
 * @typedef {Object} MemberQueries
 * @property {BillConstraints} [bills]  Filter based on bills sponsored by this member
 * @property {CommitteeConstraints} [committees]  Filter based on committees this member is a member of
 * @property {MemberConstraints} [members]  Filter based on attributes of this member
 * @property {VoteConstraints} [votes]  Filter based on votes this member has cast
 */

/**
 * Filters that you can use when selecting which {@link Meeting} to fetch
 * using the {@link Meetings} class.
 * @typedef {Object} MeetingQueries
 * @property {MediaConstraints} [media]  Filter based on media of this meeting
 * @property {MinuteConstraints} [minutes]  Filter based on minutes of this meeting
 * @property {MeetingConstraints} [meetings]  Filter based on attributes of this meeting
 */

/**
 * Filters that you can use when selecting which {@link Session} to fetch
 * using the {@link Sessions} class.
 * @typedef {Object} SessionQueries
 * @property {JournalConstraints} [journals]  Filter based on journals of this session
 */

/** 
 * @typedef {Object} BasicMember
 * @property {number} [UID] eg 0. IDK what this is for.
 * @property {MemberCode} Code eg 'BIS' for Click Bishop, 'BCH' for Tom Begich, etc
 * @property {string} FirstName
 * @property {string} MiddleName eg '' if not provided
 * @property {string} LastName
 * @property {string} EMail eg 'Representative.Carl.Gatto@akleg.gov'
 * @property {string} Phone eg '4652487'
 * @property {string} FormalName eg 'Representative Carl Gatto'
 * @property {string} ShortName eg 'Gatto          ', unclear what the padding is for.
 * @property {Chamber} Chamber eg 'H'
 * @property {string} District eg '13'  
 * @property {string} Seat eg 'Gatto'
 * @property {Party} Party eg 'R'
 * @property {string} Building eg 'CAPITOL'
 * @property {string} Room eg '118'
 * @property {string} Comment eg 'Deceased; April 10, 2012'. is '' if not set
 * @property {boolean} IsActive this appears to 
 * @property {boolean} IsMajority
 * */

/**
 * The membership of a {@link Member} in a {@link Committee}
 * @typedef {Object} Membership
 * @property {string} Position eg 'VC' or '01'
 * @property {string} CommitteeName eg 'COMMITTEE ON COMMITTEES'
 * @property {string} MemberName eg 'Representative Alan Austerman'
 * @property {MemberCode} MemberCode eg 'AUS'
 * @property {Chamber} MemberChamber eg 'H'
 * @property {Chamber} CommitteeChamber eg 'H'
 * @property {CommitteeCode} CommitteeCode eg 'CCM'
 * @property {string} MemberComment eg 'House Majority Leader'
 * */

/**
 * A {@link Member} with all their memberships and sponsorships.
 * @typedef {BasicMember} FullMember
 * @extends {BasicMember}
 * @property {Membership[]} [CommitteeMemberships]
 * @property {Membership[]} [PastCommitteeMemberships]
 * @property {Bill[]} [BillSponsorships]
 * @property {BillVote[]} [Votes]
 * */

/** 
 * The sponsorship of a {@link Bill} by a {@link Member}
 * @typedef {Object} Sponsorship
 * @property {BillCode} [BillRoot] eg 'HB   7'
 * @property {BasicMember} [SponsoringMember]
 * @property {CommitteeCode | null} [SponsoringCommittee] eg 'HSS' for Health and Social Services
 * @property {Chamber} [Chamber] eg 'H'
 * @property {string} [Requestor]
 * @property {string} [Name] eg 'Keller         '
 * @property {string} [SponsorSeq] eg '01'
 * @property {boolean} [isPrime]
 * */

/**
 * A record of a vote on a {@link Bill}.
 * @typedef {Object} BillVote
 * @property {VoteChoice} [Vote]
 * @property {string} [VoteNum] eg 'H0040'
 * @property {BillCode | null} Bill eg 'HB   7'
 * @property {MemberCode} Member eg 'HEO
 * @property {Party} MemberParty eg 'D'
 * @property {Chamber} MemberChamber eg 'H'
 * @property {string} MemberName eg 'Herron         '
 * @property {string} Title eg 'CSHB 7(JUD)\r\nThird Reading\r\nFinal Passage\r\n'
 * @property {string} VoteDate eg '2011-02-28'
 * */

/**
 * An action taken on a {@link Bill}.
 * @typedef {Object} Action
 * @property {string} Text
 * @property {Chamber} Chamber
 * @property {string} [JrnDate]
 * @property {string} [JrnPage]
 * @property {string} [Code]
 * @property {string} [Seq]
 * @property {boolean} [LinkActive]
 */

/**
 * A version of a {@link Bill}.
 * @typedef {Document} Version
 * @extends {Document}
 * @property {string} Title eg 'An Act relating to the registration fee for noncommercial trailers and to the motor vehicle tax for trailers.'
 * @property {BillCode} Name eg 'HB 10'
 * @property {string} [VersionLetter] eg 'A'
 * @property {string} [IntroDate] eg '2011-01-18'
 * @property {string | null} [PassedHouse] eg '2011-01-25'
 * @property {string | null} [PassedSenate] eg '2011-01-25'
 * @property {string} [WorkOrder] eg '27-LS0091'
 */

/**
 * @typedef {Document} FiscalNote
 * @extends {Document}
 * @property {string} Name eg 'Fiscal Note 1'
 * @property {string} Preparer eg 'Health & Social Services - Dept.'
 * @property {string} PublishedDate eg '03/16/2011'
 * @property {Chamber} Chamber eg 'H'
 * @property {string} FiscalImpact eg 'N'
 */

/**
 * Information about a committee.
 * @typedef {Object} Committee
 * @property {Chamber} Chamber eg 'S' or 'H'
 * @property {string} Code eg 'HSS'
 * @property {string} Catagory note the typo. eg 'Standing Committee'
 * @property {string} Name eg 'HEALTH & SOCIAL SERVICES'
 * @property {string} MeetingDays eg 'M W F' or 'M T W TH F'
 * @property {string} Location eg 'JNUCAP205'
 * @property {string} StartTime eg '1:30 PM'
 * @property {string} EndTime eg '000000' (I assume means null)
 * @property {string} Email eg 'Senate.Health.And.Social.Services@akleg.gov'
 * */

/** 
 * A document where the data is stored as a URL.
 * @typedef {Object} UrlDocument
 * @property {string} Url eg 'https://www.akleg.gov/PDF/27/F/HB0007-1-2-021411-LAW-N.PDF'
 * @property {null} Mime
 * @property {null} Encoding
 * @property {null} Data
 * */
/** 
 * A document where the data is directly stored as bytes.
 * @typedef {Object} RawDocument
 * @property {null} Url
 * @property {string} Mime eg 'application/pdf'
 * @property {string} Encoding eg 'base64'
 * @property {string} Data eg '...'
 * */
/** 
 * @typedef {RawDocument | UrlDocument} Document
 * */

/** 
 * @typedef {Object} Bill 
 * @property {Object[]} [Documents]
 * @property {boolean} [PartialVeto]
 * @property {boolean} [Vetoed]
 * @property {Committee} [CurrentCommittee]
 * @property {BillCode} BillNumber eg 'HB   1'
 * @property {string} BillName eg 'CSHB 1(JUD)'
 * @property {string} ShortTitle eg 'POLICY FOR SECURING HEALTH CARE SERVICES'
 * @property {string} StatusCode eg '002'
 * @property {string} StatusSummaryCode eg ' '
 * @property {string} StatusText eg '(S) HSS'
 * @property {yyyymmdd} StatusDate eg '2011-04-12'
 * @property {string[]} StatusAndThen eg ['JUD'] no idea what this is
 * @property {string} Flag1 eg 'H' IDK what this is
 * @property {string} Flag2 eg ' ' IDK what this is
 * @property {string} OnFloor eg ' '
 * @property {string} NotKnown eg ' '
 * @property {string} Filler eg ' '
 * @property {string} Lock eg ' '
 * @property {Sponsorship[]} [Sponsors]
 * @property {Version[]} [Versions]
 * @property {FiscalNote[]} [FiscalNotes]
 * @property {BillVote[]} [Votes]
 * @property {Document} [Enacted]
 * @property {Subject[]} [Subjects] eg ['ATTORNEY GENERAL', 'INSURANCE']
 * @property {Action[]} [Actions]
 * @property {any[]} [ManifestErrors]
 * @property {string} [SponsorUrl]
 * @property {any[]} [Statutes]
*/

/**
 * @typedef {Object} FetchArgs
 * @property {string} url
 * @property {Record<string, string>} headers
 * @property {HttpMethod} [method]
 */
/**
 * @typedef {Object} FetchResponse
 * @property {string} payload
 * @property {Record<string, string>} headers
 */
/** @typedef {(args: FetchArgs) => Promise<FetchResponse>} Fetcher */
/**
 * A {@link Fetcher} that uses the web standard fetch() API.
 * @param {FetchArgs} args
 * @returns {Promise<FetchResponse>}
 */
export async function webFetch(args) {
  const response = await fetch(args.url, { headers: args.headers, method: args.method });
  return {
    payload: await response.text(),
    headers: Object.fromEntries(response.headers.entries()),
  };
}

/** 
 * @typedef {Object} Logger 
 * @property {function(any): void} error
 * @property {function(any): void} warn
 * @property {function(any): void} info
 * @property {function(any): void} debug
 * */

/**
 * A logger that logs everything to console.error, such that it can be piped to stdout.
 * @returns {Logger}
 */
export function defaultLogger() {
  return {
    error: (...args) => console.error(`${new Date().toISOString()} ERROR:`, ...args),
    warn: (...args) => console.error(`${new Date().toISOString()} WARN:`, ...args),
    info: (...args) => console.error(`${new Date().toISOString()} INFO:`, ...args),
    debug: (...args) => console.error(`${new Date().toISOString()} DEBUG:`, ...args),
  };
}

/**
 * @typedef {Object} IntoConfig
 * @property {string} [baseUrl] - The base URL for the API
 * @property {Fetcher} [fetcher] - The fetcher function to use for the API requests.
 *   This is useful if you are in eg a Google Apps Script environment where
 *   the web standard fetch API is not available, and you need to use the
 *   Google Apps Script UrlFetch API instead.
 * @property {Logger} [logger] - Where this library will log errors and warnings.
 */

/**
 * The Config class is used to configure the API client.
 * @example
 * const config = new Config({ baseUrl: 'https://www.akleg.gov/publicservice/basis', fetcher: myCustomFetcher });
 */
export class Config {
  /**
   * @param {IntoConfig} [config]
   */
  constructor(config) {
    config = config || {};
    this.baseUrl = config.baseUrl || 'https://www.akleg.gov/publicservice/basis';
    this.baseUrl = this.baseUrl.replace(/\/$/, ''); // Remove trailing slash if present
    this.fetcher = config.fetcher || webFetch;
    this.logger = config.logger || defaultLogger();
  }
}

/**
 * @typedef {Object} BaseParams
 * @property {number} [session] If not provided, will not filter by session.
 * @property {Chamber} [chamber]
 * @property {string} [range] This is a little complex:
 *  - '10' to get the first 10 results
 *  - '10..20' to get results 10 through 20
 *  - '..30' to get the last 30 results
 */
/** @typedef {BaseParams & {queries?: BillQueries}} BillsParams */
/** @typedef {BaseParams & {queries?: CommitteeQueries}} CommitteesParams */
/** @typedef {BaseParams & {queries?: MeetingQueries}} MeetingsParams */
/** @typedef {BaseParams & {queries?: MemberQueries}} MembersParams */
/** @typedef {BaseParams & {queries?: SessionQueries}} SessionsParams */

/**
 * The Bills class is a wrapper around the bills section of the API.
 *
 * @example get HB 38:
 * const bills = new Bills({ queries: { bills: { name: '*38' } } });
 * const nResults = await bills.count();
 * const data = await bills.fetch();
 * console.log(nResults);
 * console.log(data);
 * 
 * @example get all bills sponsored by a rep from the 13th district:
 * const bills = new Bills({ queries: { sponsors: { district: '13' } } });
 * const nResults = await bills.count();
 * const data = await bills.fetch();
 * console.log(nResults);
 * console.log(data);
 */
export class Bills {
  /**
   * @param {BillsParams} [params]
   * @param {IntoConfig} [config]
   */
  constructor(params, config) {
    this.params = params;
    this.config = new Config(config);
  }

  /**
   * @returns {FetchArgs}
   */
    fetchArgs() {
      return buildArgs('bills', this.params, this.config.baseUrl);
    }

  /**
   * @returns {Promise<Bill[]>}
   */
  async fetch() {
    return (await _data(this.fetchArgs(), this.config)).Bills;
  }

  /**
   * @returns {Promise<number>}
   */
  async count() {
    return _count(this.fetchArgs(), this.config);
  }
}

/**
 * The Committees class is a wrapper around the committees section of the API.
 *
 * @example
 * const committees = new Committees({ queries: { Committees: { name: '*38' } } });
 * const nResults = await committees.count();
 * const data = await committees.fetch();
 * console.log(nResults);
 * console.log(data);
 */
export class Committees {
  /**
   * @param {CommitteesParams} [params]
   * @param {IntoConfig} [config]
   */
  constructor(params, config) {
    this.params = params;
    this.config = new Config(config);
  }

  /**
   * @returns {FetchArgs}
   */
    fetchArgs() {
      return buildArgs('committees', this.params, this.config.baseUrl);
    }

  /**
   * @returns {Promise<any[]>}
   */
  async fetch() {
    return (await _data(this.fetchArgs(), this.config)).Committees;
  }

  /**
   * @returns {Promise<number>}
   */
  async count() {
    return _count(this.fetchArgs(), this.config);
  }
}

/**
 * The Meetings class is a wrapper around the meetings section of the API.
 */
export class Meetings {
  /**
   * @param {MeetingsParams} [params]
   * @param {IntoConfig} [config]
   */
  constructor(params, config) {
    this.params = params;
    this.config = new Config(config);
  }

  /**
   * @returns {FetchArgs}
   */
  fetchArgs() {
    return buildArgs('meetings', this.params, this.config.baseUrl);
  }
  
  /**
   * @returns {Promise<any[]>}
   */
  async fetch() {
    return (await _data(this.fetchArgs(), this.config)).Meetings;
  }

  /**
   * @returns {Promise<number>}
   */
  async count() {
    return _count(this.fetchArgs(), this.config);
  }
}

/**
 * The Members class is a wrapper around the members section of the API.
 */
export class Members {
  /**
   * @param {MembersParams} [params]
   * @param {IntoConfig} [config]
   */
  constructor(params, config) {
    this.params = params;
    this.config = new Config(config);
  }

  /**
   * @returns {FetchArgs}
   */
  fetchArgs() {
    return buildArgs('members', this.params, this.config.baseUrl);
  }

  /**
   * @returns {Promise<FullMember[]>} Depending on what queries you included in
   * the {@link MemberQueries}, the returned {@link FullMember} objects will have different fields:
   * - Iff you include the {@link CommitteeConstraints} option in the {@link MemberQueries},
   *   the 'CommitteeMemberships' and 'PastCommitteeMemberships' fields will be present.
   * - Iff you include the {@link BillConstraints} option in the {@link MemberQueries},
   *   the 'BillSponsorships' field will be present.
   * - Iff you include the {@link VoteConstraints} option in the {@link MemberQueries},
   *   the 'Votes' field will be present.
   */
  async fetch() {
    return (await _data(this.fetchArgs(), this.config)).Members;
  }

  /**
   * @returns {Promise<number>}
   */
  async count() {
    return _count(this.fetchArgs(), this.config);
  }
}

/**
 * The Sessions class is a wrapper around the sessions section of the API.
 */
export class Sessions {
  /**
   * @param {SessionsParams} [params]
   * @param {IntoConfig} [config]
   */
  constructor(params, config) {
    this.params = params;
    this.config = new Config(config);
  }

  /**
   * @returns {FetchArgs}
   */
  fetchArgs() {
    return buildArgs('sessions', this.params, this.config.baseUrl);
  }

  /**
   * @returns {Promise<any[]>}
   */
  async fetch() {
    return (await _data(this.fetchArgs(), this.config)).Sessions;
  }

  /**
   * @returns {Promise<number>}
   */
  async count() {
    return _count(this.fetchArgs(), this.config);
  }
}

/**
 * Builds the arguments for an HTTP request.
 * 
 * This is a low-level function that is normally not needed to be called directly.
 * But sometimes, you need to build the arguments for an HTTP request manually,
 * for example when you are using a custom fetcher.
 * 
 * @param {'bills' | 'committees' | 'meetings' | 'members' | 'sessions'} section
 * @param {BaseParams & {queries?: Record<string, any>}} [params]
 * @param {string} [baseUrl]
 * @returns {FetchArgs}
 */
export function buildArgs(section, params, baseUrl) {
  if (!params) {
    params = {};
  }
  if (!baseUrl) {
    baseUrl = new Config().baseUrl;
  }
  /** @type {{ json: string, session?: string, chamber?: Chamber }} */
  const urlParams = { json: 'true' };
  if (params.session) {
    urlParams.session = params.session.toString();
  }
  if (params.chamber) {
    urlParams.chamber = params.chamber;
  }
  const headerString = _queriesToHeaderString(params.queries);

  const queryString = new URLSearchParams(urlParams).toString();
  const url = `${baseUrl}/${section}${queryString ? '?' + queryString : ''}`;

  const headers = {
    'X-Alaska-Legislature-Basis-Version': '1.4',
    'Accept-Encoding': 'gzip;q=1.0',
  };
  if (headerString) {
    headers['X-Alaska-Legislature-Basis-Query'] = headerString;
  }
  if (params.range) {
    headers['X-Alaska-Query-ResultRange'] = params.range;
  }

  return {url, headers};
}

/**
 * @param {FetchArgs} args
 * @param {Config} config
 * @returns {Promise<any>}
 */
async function _data(args, config) {
  args = {...args, method: 'GET'};
  config.logger.debug(args);
  const response = await config.fetcher(args);
  let parsed;
  try {
    parsed = JSON.parse(response.payload);
  } catch (e) {
    config.logger.error(response.payload);
    throw e;
  }
  return parsed.Basis;
}

/**
 * @param {FetchArgs} args
 * @param {Config} config
 * @returns {Promise<number>}
 */
async function _count(args, config) {
  args = { ...args, method: 'HEAD' };
  config.logger.debug(args);
  const response = await config.fetcher(args);
  return Number(response.headers['x-alaska-query-count']);
}

/**
 * @param {Record<string, any> | undefined} queries
 * @returns {string | null}
 */
function _queriesToHeaderString(queries) {
  if (!queries) {
    return null;
  }
  const headerStrings = [];
  for (const [include, constraints] of Object.entries(queries)) {
    let constraintsString = Object.entries(constraints).map(([k, v]) => `${k}=${v}`).join(';');
    headerStrings.push(`${include};${constraintsString}`);
  }
  return headerStrings.join(',');
}