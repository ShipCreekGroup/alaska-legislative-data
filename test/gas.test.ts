// The Apps Script build must run as a plain (non-module) script with only UrlFetchApp available.
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import vm from 'node:vm';
import { buildGasScript } from '../scripts/build-gas.ts';
import { fixture } from './helpers.ts';

describe('Google Apps Script build', () => {
  it('runs with UrlFetchApp and no fetch/setTimeout/URLSearchParams', async () => {
    const requests: Array<[string, { method: string; headers: Record<string, string> }]> = [];
    const sandbox: Record<string, unknown> = {
      UrlFetchApp: {
        fetch(url: string, params: { method: string; headers: Record<string, string> }) {
          requests.push([url, params]);
          return {
            getResponseCode: () => 200,
            getContentText: () => fixture('members.json'),
            getAllHeaders: () => ({ 'X-Alaska-Query-Count': '5' }),
          };
        },
      },
      JSON,
      Promise,
    };
    vm.createContext(sandbox);
    vm.runInContext(buildGasScript(), sandbox);
    const result = await vm.runInContext(
      `(async () => {
        const client = new BasisClient({ fetch: urlFetchAppFetch() });
        const members = await client.members({ session: 34, query: { members: { party: 'D' } } });
        return [members.length, members[0].Code, await client.count('members', { session: 34 })];
      })()`,
      sandbox,
    );
    assert.deepEqual([...(result as unknown[])], [5, 'ALR', 5]); // spread: the array comes from another realm
    assert.equal(requests[0]![1].headers['X-Alaska-Legislature-Basis-Query'], 'members;party=D');
    assert.equal(requests[0]![1].method, 'get');
  });
});
