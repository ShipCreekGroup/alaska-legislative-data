const e = encodeURIComponent;
export const paths = {
  bill: (id: string) => `/bills/${e(id)}`,
  version: (id: string) => `/bill-versions/${e(id)}`,
  vote: (id: string) => `/votes/${e(id)}`,
  member: (id: string) => `/members/${e(id)}`,
  person: (id: string) => `/people/${e(id)}`,
  legislature: (n: number) => `/legislatures/${n}`,
};
