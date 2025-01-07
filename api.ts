export type Chamber = 'H' | 'S';
/* including fulltext means the entire text is included in the response,
 * otherwise only the url is included. */
export type Fulltext = true | 'urlonly';

export interface Actions {
  code?: string;
  text?: string;
  date?: string;
  startdate?: string;
  enddate?: string;
}

export interface Bills {
  name?: string;
  chamber?: Chamber;
  bill?: string;
  onfloor?: boolean;
  shorttitle?: string;
  statustext?: string;
  statuscode?: string;
  committeecode?: string;
}

export interface Committees {
  category?: string;
  chamber?: Chamber;
  location?: string;
  name?: string;
  title?: string;
  code?: string;
}

export interface FiscalNotes {
  chamber?: Chamber;
  date?: string;
  startdate?: string;
  enddate?: string;
  startpage?: string;
  endpage?: string;
}

export interface Journals {
  chamber?: Chamber;
  date?: string;
  startdate?: string;
  enddate?: string;
  page?: string;
  startpage?: string;
  endpage?: string;
}

export interface Meetings {
  date?: string;
  startdate?: string;
  enddate?: string;
  details?: string;
}

export interface Members {
  building?: string;
  chamber?: Chamber;
  comment?: string;
  district?: string;
  party?: string;
  phone?: string;
  firstname?: string;
  lastname?: string;
  ismajority?: boolean;
  /* three letter code, eg 'BIS' for Click Bishop, 'BCH' for Tom Begich, etc */
  code?: string;
}

// There actually are no constraints for media, you can only choose to include it or not
export interface Media {}

export interface Minutes {
  fulltext?: Fulltext;
}

export interface Subjects {
  text?: string;
}

export interface Versions {
  fulltext?: Fulltext;
}

export interface Votes {
  vote?: string;
  title?: string;
}

export interface Sponsors extends Members {}

export interface BillQueries {
  Actions?: Actions;
  Bills?: Bills;
  Fiscalnotes?: FiscalNotes;
  Sponsors?: Members;
  Subjects?: Subjects;
  Versions?: Versions;
  Votes?: Votes;
}

export interface CommitteeQueries {
  Bills?: Bills;
  Committees?: Committees;
  Meetings?: Meetings;
  Members?: Members;
}

export interface MemberQueries {
  Bills?: Bills;
  Committees?: Committees;
  Members?: Members;
  Votes?: Votes;
}

export interface MeetingQueries {
  Media?: Media;
  Minutes?: Minutes;
  Meetings?: Meetings;
}

export interface SessionQueries {
  Journals?: Journals;
}

export interface BaseOptions {
  session?: number;
  chamber?: Chamber;
  range?: string;
}
export interface GetBillsOptions extends BaseOptions {
  queries?: BillQueries;
}

export interface GetMeetingsOptions extends BaseOptions {
  queries?: MeetingQueries;
}

export interface GetMembersOptions extends BaseOptions {
  queries?: MemberQueries;
}

export interface GetCommitteesOptions extends BaseOptions {
  queries?: CommitteeQueries;
}

export interface GetSessionsOptions extends BaseOptions {
  queries?: SessionQueries;
}

type Fetcher = (url: string, headers: Record<string, string>) => Promise<string | Record<string, any>>;

export class Client {
  private baseUrl: string;
  private fetcher: Fetcher;

  /**
   * @param baseUrl - The base URL for the API
   * @param fetcher - The fetcher function to use for the API requests.
   *   This is useful if you are in eg a Google Apps Script environment where
   *   the web standard fetch API is not available, and you need to use the
   *   Google Apps Script UrlFetch API instead.
   */
  constructor(baseUrl?: string, fetcher?: Fetcher) {
    baseUrl = baseUrl || 'https://www.akleg.gov/publicservice/basis';
    this.baseUrl = baseUrl.replace(/\/$/, ''); // Remove trailing slash if present
    this.fetcher = fetcher || this.defaultFetch;
  }

  private async defaultFetch(url: string, headers: Record<string, string>) {
    const response = await fetch(url, {headers});
    return await response.json();
  }

  get baseHeaders() : Record<string, string> {
    return {
      // need >=1.4 to be able to use json=true query parameter
      'X-Alaska-Legislature-Basis-Version': '1.4',
      'Accept-Encoding': 'gzip;q=1.0',
    };
  }

  private async request(
    section: "bills" | "committees" | "meetings" |"members" | "sessions",
    options?: BaseOptions,
    headerStrings?: string[]
  ){
    if (!options) {
      options = {};
    }
    if (!headerStrings) {
      headerStrings = [];
    }
    let params: Record<string, string> = {json: 'true'};
    if (options.session) {
      params.session = options.session.toString();
    }
    if (options.chamber) {
      params.chamber = options.chamber;
    }

    const queryString = new URLSearchParams(params).toString();
    const url = `${this.baseUrl}/${section}${queryString ? '?' + queryString : ''}`;

    const headers = this.baseHeaders;
    headerStrings.forEach(headerString => {
      headers['X-Alaska-Legislature-Basis-Query'] = headerString;
    });
    if (options.range) {
      headers['X-Alaska-Query-ResultRange'] = options.range;
    }
    
    console.debug(url);
    console.debug(headers);
    let response = await this.fetcher(url, headers);
    if (typeof response === 'string') {
      response = JSON.parse(response) as Record<string, any>;
    }
    return response.Basis;
  }

  /**
   * Retrieves bill information from the Alaska Legislature API
   * @param [options] - Optional parameters for the request
   * @returns Promise<Array<Bills>> Array of bill objects matching the query parameters
   */
  async getBills(options?: GetBillsOptions) {
    const headerStrings = queriesToHeaderStrings(options?.queries);
    const bills = await this.request('bills', options, headerStrings);
    return bills.Bills;
  }

  async getMeetings(options?: GetMeetingsOptions) {
    const headerStrings = queriesToHeaderStrings(options?.queries);
    const meetings = await this.request('meetings', options, headerStrings);
    return meetings.Meetings;
  }

  async getMembers(options?: GetMembersOptions) {
    const headerStrings = queriesToHeaderStrings(options?.queries);
    const members = await this.request('members', options, headerStrings);
    return members.Members;
  }

  async getCommittees(options?: GetCommitteesOptions) {
    const headerStrings = queriesToHeaderStrings(options?.queries);
    const committees = await this.request('committees', options, headerStrings);
    return committees.Committees;
  }

  async getSessions(options?: GetSessionsOptions) {
    const headerStrings = queriesToHeaderStrings(options?.queries);
    const sessions = await this.request('sessions', options, headerStrings);
    return sessions.Sessions;
  }
} 

function queriesToHeaderStrings(queries: Record<string, any> | undefined) {
  if (!queries) {
    return [];
  }
  const headerStrings: string[] = [];
  for (const [include, constraints] of Object.entries(queries)) {
    // for example, include="Sponsors", and constraints={building: "100", chamber: "H"}
    let constraintsString = Object.entries(constraints).map(([k, v]) => `${k}=${v}`).join(';');
    headerStrings.push(`${include};${constraintsString}`);
  }
  return headerStrings;
}
