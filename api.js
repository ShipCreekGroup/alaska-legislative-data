// @ts-check
/** @typedef {'H' | 'S'} Chamber */

/** @typedef {true | 'urlonly'} Fulltext */

/**
 * @typedef {Object} ActionConstraints
 * @property {string} [code]
 * @property {string} [text]
 * @property {string} [date]
 * @property {string} [startdate]
 * @property {string} [enddate]
 */

/**
 * @typedef {Object} BillConstraints
 * @property {string} [name] The bill name, eg 'CSHB 2(STA)(FLD H)'
 * @property {Chamber} [chamber] 'H' for House, 'S' for Senate
 * @property {string} [bill] The bill number, eg 'HB  6'
 * @property {boolean} [onfloor] 
 * @property {string} [shorttitle] eg 'CONTRACTS: PROHIBIT ISRAEL DISCRIMINATION'
 * @property {string} [statustext] eg '014'
 * @property {string} [statuscode] eg 'FAILED(H) PERM FILED(H)'
 * @property {string} [committeecode] The code of the committee that the bill is 
 *   currently in, eg 'STA' for the 'State Affairs' committee.
 */

/**
 * @typedef {Object} CommitteeConstraints
 * @property {string} [category]
 * @property {Chamber} [chamber]
 * @property {string} [location]
 * @property {string} [name]
 * @property {string} [title]
 * @property {string} [code]
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
 * @property {string} [party]
 * @property {string} [phone]
 * @property {string} [firstname]
 * @property {string} [lastname]
 * @property {boolean} [ismajority]
 * @property {string} [code] three letter code, eg 'BIS' for Click Bishop, 'BCH' for Tom Begich, etc
 */

/** @typedef {Object} MediaConstraints */

/**
 * @typedef {Object} MinuteConstraints
 * @property {Fulltext} [fulltext]
 */

/**
 * @typedef {Object} SubjectConstraints
 * @property {string} [text]
 */

/**
 * @typedef {Object} VersionConstraints
 * @property {Fulltext} [fulltext]
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
 * @typedef {Object} BillQueries
 * @property {ActionConstraints} [Actions]
 * @property {BillConstraints} [Bills]
 * @property {FiscalNoteConstraints} [Fiscalnotes]
 * @property {SponsorConstraints} [Sponsors]
 * @property {SubjectConstraints} [Subjects]
 * @property {VersionConstraints} [Versions]
 * @property {VoteConstraints} [Votes]
 */

/**
 * @typedef {Object} CommitteeQueries
 * @property {BillConstraints} [Bills]
 * @property {CommitteeConstraints} [Committees]
 * @property {MeetingConstraints} [Meetings]
 * @property {MemberConstraints} [Members]
 */

/**
 * @typedef {Object} MemberQueries
 * @property {BillConstraints} [Bills]
 * @property {CommitteeConstraints} [Committees]
 * @property {MemberConstraints} [Members]
 * @property {VoteConstraints} [Votes]
 */

/**
 * @typedef {Object} MeetingQueries
 * @property {MediaConstraints} [Media]
 * @property {MinuteConstraints} [Minutes]
 * @property {MeetingConstraints} [Meetings]
 */

/**
 * @typedef {Object} SessionQueries
 * @property {JournalConstraints} [Journals]
 */

/**
 * @typedef {Object} BaseOptions
 * @property {number} [session]
 * @property {Chamber} [chamber]
 * @property {string} [range]
 */

/** @typedef {BaseOptions & {queries?: BillQueries}} GetBillsOptions */
/** @typedef {BaseOptions & {queries?: MeetingQueries}} GetMeetingsOptions */
/** @typedef {BaseOptions & {queries?: MemberQueries}} GetMembersOptions */
/** @typedef {BaseOptions & {queries?: CommitteeQueries}} GetCommitteesOptions */
/** @typedef {BaseOptions & {queries?: SessionQueries}} GetSessionsOptions */

/** @typedef {'GET' | 'HEAD'} Method */

/**
 * @typedef {Object} FetchArgs
 * @property {string} url
 * @property {Record<string, string>} headers
 * @property {Method} [method]
 */

/**
 * @typedef {Object} FetchResponse
 * @property {string} payload
 * @property {Record<string, string>} headers
 */

/** @typedef {(args: FetchArgs) => Promise<FetchResponse>} Fetcher */

/**
 * A fetcher that uses the web standard fetch() API.
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
 * 
 * @typedef {Object} IntoConfig
 * @property {string} [baseUrl] - The base URL for the API
 * @property {Fetcher} [fetcher] - The fetcher function to use for the API requests.
 *   This is useful if you are in eg a Google Apps Script environment where
 *   the web standard fetch API is not available, and you need to use the
 *   Google Apps Script UrlFetch API instead.
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
 * @param {BaseOptions & {queries?: Record<string, any>}} [options]
 * @param {string} [baseUrl]
 * @returns {FetchArgs}
 */
export function buildArgs(section, options, baseUrl) {
  if (!options) {
    options = {};
  }
  if (!baseUrl) {
    baseUrl = new Config().baseUrl;
  }
  /** @type {{ json: string, session?: string, chamber?: Chamber }} */
  let params = { json: 'true' };
  if (options.session) {
    params.session = options.session.toString();
  }
  if (options.chamber) {
    params.chamber = options.chamber;
  }
  const headerString = _queriesToHeaderString(options.queries);

  const queryString = new URLSearchParams(params).toString();
  const url = `${baseUrl}/${section}${queryString ? '?' + queryString : ''}`;

  const headers = {
    'X-Alaska-Legislature-Basis-Version': '1.4',
    'Accept-Encoding': 'gzip;q=1.0',
  };
  if (headerString) {
    headers['X-Alaska-Legislature-Basis-Query'] = headerString;
  }
  if (options.range) {
    headers['X-Alaska-Query-ResultRange'] = options.range;
  }

  return { url, headers};
}

/**
 * The Bills class is a wrapper around the bills section of the API.
 *
 * @example
 * const bills = new Bills({ queries: { Bills: { name: '*38' } } });
 * const nResults = await bills.count();
 * const data = await bills.fetch();
 * console.log(nResults);
 * console.log(data);
 */
export class Bills {
  /**
   * @param {GetBillsOptions} [options]
   * @param {IntoConfig} [config]
   */
  constructor(options, config) {
    this.options = options;
    this.config = new Config(config);
  }

  /**
   * @returns {FetchArgs}
   */
    fetchArgs() {
      return buildArgs('bills', this.options, this.config.baseUrl);
    }

  /**
   * @returns {Promise<any[]>}
   */
  async fetch() {
    return (await _data(this.fetchArgs(), this.config.fetcher)).Bills;
  }

  /**
   * @returns {Promise<number>}
   */
  async count() {
    return _count(this.fetchArgs(), this.config.fetcher);
  }
}

/**
 * @param {FetchArgs} args
 * @param {Fetcher} fetcher
 * @returns {Promise<any>}
 */
async function _data(args, fetcher) {
  args = {...args, method: 'GET'}
  const response = await fetcher(args);
  return JSON.parse(response.payload).Basis;
}

/**
 * @param {FetchArgs} args
 * @param {Fetcher} fetcher
 * @returns {Promise<number>}
 */
async function _count(args, fetcher) {
  args = { ...args, method: 'HEAD' };
  const response = await fetcher(args);
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