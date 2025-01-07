// @ts-check
/**
 * @typedef {'H' | 'S'} Chamber
 */

/**
 * @typedef {true | 'urlonly'} Fulltext
 */

/**
 * @typedef {Object} Actions
 * @property {string} [code]
 * @property {string} [text]
 * @property {string} [date]
 * @property {string} [startdate]
 * @property {string} [enddate]
 */

/**
 * @typedef {Object} Bills
 * @property {string} [name]
 * @property {Chamber} [chamber]
 * @property {string} [bill]
 * @property {boolean} [onfloor]
 * @property {string} [shorttitle]
 * @property {string} [statustext]
 * @property {string} [statuscode]
 * @property {string} [committeecode]
 */

/**
 * @typedef {Object} Committees
 * @property {string} [category]
 * @property {Chamber} [chamber]
 * @property {string} [location]
 * @property {string} [name]
 * @property {string} [title]
 * @property {string} [code]
 */

/**
 * @typedef {Object} FiscalNotes
 * @property {Chamber} [chamber]
 * @property {string} [date]
 * @property {string} [startdate]
 * @property {string} [enddate]
 * @property {string} [startpage]
 * @property {string} [endpage]
 */

/**
 * @typedef {Object} Journals
 * @property {Chamber} [chamber]
 * @property {string} [date]
 * @property {string} [startdate]
 * @property {string} [enddate]
 * @property {string} [page]
 * @property {string} [startpage]
 * @property {string} [endpage]
 */

/**
 * @typedef {Object} Meetings
 * @property {string} [date]
 * @property {string} [startdate]
 * @property {string} [enddate]
 * @property {string} [details]
 */

/**
 * @typedef {Object} Members
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

/**
 * @typedef {Object} Media
 */

/**
 * @typedef {Object} Minutes
 * @property {Fulltext} [fulltext]
 */

/**
 * @typedef {Object} Subjects
 * @property {string} [text]
 */

/**
 * @typedef {Object} Versions
 * @property {Fulltext} [fulltext]
 */

/**
 * @typedef {Object} Votes
 * @property {string} [vote]
 * @property {string} [title]
 */

/**
 * @typedef {Members} Sponsors
 */

/**
 * @typedef {Object} BillQueries
 * @property {Actions} [Actions]
 * @property {Bills} [Bills]
 * @property {FiscalNotes} [Fiscalnotes]
 * @property {Members} [Sponsors]
 * @property {Subjects} [Subjects]
 * @property {Versions} [Versions]
 * @property {Votes} [Votes]
 */

/**
 * @typedef {Object} CommitteeQueries
 * @property {Bills} [Bills]
 * @property {Committees} [Committees]
 * @property {Meetings} [Meetings]
 * @property {Members} [Members]
 */

/**
 * @typedef {Object} MemberQueries
 * @property {Bills} [Bills]
 * @property {Committees} [Committees]
 * @property {Members} [Members]
 * @property {Votes} [Votes]
 */

/**
 * @typedef {Object} MeetingQueries
 * @property {Media} [Media]
 * @property {Minutes} [Minutes]
 * @property {Meetings} [Meetings]
 */

/**
 * @typedef {Object} SessionQueries
 * @property {Journals} [Journals]
 */

/**
 * @typedef {Object} BaseOptions
 * @property {number} [session]
 * @property {Chamber} [chamber]
 * @property {string} [range]
 */

/**
 * @typedef {BaseOptions & {queries?: BillQueries}} GetBillsOptions
 */

/**
 * @typedef {BaseOptions & {queries?: MeetingQueries}} GetMeetingsOptions
 */

/**
 * @typedef {BaseOptions & {queries?: MemberQueries}} GetMembersOptions
 */

/**
 * @typedef {BaseOptions & {queries?: CommitteeQueries}} GetCommitteesOptions
 */

/**
 * @typedef {BaseOptions & {queries?: SessionQueries}} GetSessionsOptions
 */

/**
 * @typedef {(url: string, headers: Record<string, string>) => Promise<string | Record<string, any>>} Fetcher
 */

/**
 * @typedef {BaseOptions & {queries?: Record<string, any>}} GenericOptions
 */

export class Client {
  /**
   * @param {string} [baseUrl] - The base URL for the API
   * @param {Fetcher} [fetcher] - The fetcher function to use for the API requests.
   *   This is useful if you are in eg a Google Apps Script environment where
   *   the web standard fetch API is not available, and you need to use the
   *   Google Apps Script UrlFetch API instead.
   */
  constructor(baseUrl, fetcher) {
    baseUrl = baseUrl || 'https://www.akleg.gov/publicservice/basis';
    this.baseUrl = baseUrl.replace(/\/$/, ''); // Remove trailing slash if present
    this.fetcher = fetcher || this.defaultFetch;
  }

  /**
   * @param {string} url
   * @param {Record<string, string>} headers
   * @returns {Promise<any>}
   */
  async defaultFetch(url, headers) {
    const response = await fetch(url, { headers });
    return await response.json();
  }

  /**
   * @returns {Record<string, string>}
   */
  get baseHeaders() {
    return {
      'X-Alaska-Legislature-Basis-Version': '1.4',
      'Accept-Encoding': 'gzip;q=1.0',
    };
  }

  /**
   * @param {'bills' | 'committees' | 'meetings' | 'members' | 'sessions'} section
   * @param {GenericOptions} [options]
   * @returns {Promise<any>}
   */
  async request(section, options) {
    if (!options) {
      options = {};
    }
    /** @type {{ json: string, session?: string, chamber?: Chamber }} */
    let params = { json: 'true' };
    if (options.session) {
      params.session = options.session.toString();
    }
    if (options.chamber) {
      params.chamber = options.chamber;
    }
    const headerString = queriesToHeaderString(options.queries);

    const queryString = new URLSearchParams(params).toString();
    const url = `${this.baseUrl}/${section}${queryString ? '?' + queryString : ''}`;

    const headers = this.baseHeaders;
    if (headerString) {
      headers['X-Alaska-Legislature-Basis-Query'] = headerString;
    }
    if (options.range) {
      headers['X-Alaska-Query-ResultRange'] = options.range;
    }

    console.debug(url);
    console.debug(headers);
    let response = await this.fetcher(url, headers);
    if (typeof response === 'string') {
      response = JSON.parse(response);
    }
    return response.Basis;
  }

  /**
   * @param {GetBillsOptions} [options]
   * @returns {Promise<any>}
   */
  async getBills(options) {
    const bills = await this.request('bills', options);
    return bills.Bills;
  }

  /**
   * @param {GetMeetingsOptions} [options]
   * @returns {Promise<any>}
   */
  async getMeetings(options) {
    const meetings = await this.request('meetings', options);
    return meetings.Meetings;
  }

  /**
   * @param {GetMembersOptions} [options]
   * @returns {Promise<any>}
   */
  async getMembers(options) {
    const members = await this.request('members', options);
    return members.Members;
  }

  /**
   * @param {GetCommitteesOptions} [options]
   * @returns {Promise<any>}
   */
  async getCommittees(options) {
    const committees = await this.request('committees', options);
    return committees.Committees;
  }

  /**
   * @param {GetSessionsOptions} [options]
   * @returns {Promise<any>}
   */
  async getSessions(options) {
    const sessions = await this.request('sessions', options);
    return sessions.Sessions;
  }
}

/**
 * @param {Record<string, any> | undefined} queries
 * @returns {string | null}
 */
function queriesToHeaderString(queries) {
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