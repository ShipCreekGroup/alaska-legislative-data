// Markdown copies of record pages, for AI agents: the same facts as the HTML page with none of
// the layout, at <page path>.md (or the page path with "Accept: text/markdown").
// Unlike the HTML pages, lists are not paginated: a legislator's file has every vote they cast.
import { getDoc, type Kind } from './store';
import {
  CHOICE,
  SITE_URL,
  akleg,
  chamberName,
  legFull,
  legName,
  legYears,
  memberTitle,
  memberTitleShort,
  partyName,
  partyPlural,
  voteMotion,
  voteVersion,
} from '~/lib/format';
import { paths } from '~/lib/paths';
import { billSummary, byParty, memberSummary, personSummary, versionSummary, voteSummary } from '~/lib/summaries';
import type { Bill, BillVersion, Choice, Legislature, Member, Person, Vote } from '~/lib/types';

const u = (path: string) => SITE_URL + path;
const cell = (s: string | number | null | undefined) =>
  String(s ?? '')
    .replace(/\|/g, '\\|')
    .replace(/\s*\n\s*/g, ' ');
function table(head: string[], rows: (string | number | null | undefined)[][]): string {
  return [
    `| ${head.join(' | ')} |`,
    `| ${head.map(() => '---').join(' | ')} |`,
    ...rows.map((r) => `| ${r.map(cell).join(' | ')} |`),
  ].join('\n');
}
const link = (text: string | null | undefined, path: string) => `[${cell(text)}](${u(path)})`;
function header(title: string, path: string, facts: [string, string | null | undefined][]): string {
  return [
    `# ${title}`,
    '',
    ...facts.filter(([, v]) => v).map(([k, v]) => `- ${k}: ${v}`),
    `- Web page: ${u(path)}`,
    `- JSON: ${u(path.replace(/^\/([^/]+)\//, '/data/$1/') + '.json')}`,
    `- Source: Alaska Legislative Data by Ship Creek Group (${SITE_URL}), MIT license`,
    '',
  ].join('\n');
}

function bill(b: Bill): string {
  const path = paths.bill(b.id);
  return [
    header(`${b.number}: ${b.title ?? ''}`.trim(), path, [
      ['Bill ID', `\`${b.id}\``],
      ['Legislature', `${legFull(b.legislature)} (${legYears(b.legislature)}), ${u(paths.legislature(b.legislature))}`],
      ['Subjects', b.subjects?.join(', ')],
      ['Vetoed', b.vetoed ? 'yes' : null],
      ['Status date', b.statusDate],
      ['Official page', akleg.bill(b.legislature, b.number)],
    ]),
    billSummary(b),
    '',
    `## Versions (${b.versions.length})`,
    '',
    b.versions.length
      ? table(
          ['Version', 'Introduced', 'Passed House', 'Passed Senate', 'Full text', 'PDF'],
          b.versions.map((v) => [
            v.name,
            v.introDate,
            v.passedHouse,
            v.passedSenate,
            link(`version ${v.letter}`, paths.version(v.id)),
            v.pdfUrl,
          ]),
        )
      : 'None recorded.',
    '',
    `## Floor votes (${b.votes.length})`,
    '',
    b.votes.length
      ? table(
          ['Date', 'Chamber', 'Motion', 'Yea', 'Nay', 'Excused', 'Absent', 'Roll call'],
          b.votes.map((v) => [
            v.date,
            chamberName(v.chamber),
            voteMotion(v.title),
            v.y,
            v.n,
            v.e,
            v.a,
            link(`vote ${v.number}`, paths.vote(v.id)),
          ]),
        )
      : 'None recorded.',
    '',
  ].join('\n');
}

function version(v: BillVersion): string {
  return [
    header(`${v.billNumber} version ${v.letter}: ${v.billTitle ?? v.title}`, paths.version(v.id), [
      ['Version ID', `\`${v.id}\``],
      ['Bill', `${link(v.billNumber, paths.bill(v.billId))} (\`${v.billId}\`)`],
      ['Name', v.name],
      ['Title', v.title],
      ['Introduced', v.introDate],
      ['Passed House', v.passedHouse],
      ['Passed Senate', v.passedSenate],
      ['Official PDF', v.pdfUrl],
    ]),
    versionSummary(v),
    '',
    '## Full text',
    '',
    v.fullText ? '```text\n' + v.fullText.replace(/```/g, "'''") + '\n```' : 'Not available. See the official PDF.',
    '',
  ].join('\n');
}

const ORDER: Choice[] = ['Y', 'N', 'E', 'A'];
function vote(v: Vote): string {
  return [
    header(
      `Alaska ${chamberName(v.chamber)} roll call ${v.number}: ${v.billNumber ? v.billNumber + ', ' : ''}${voteMotion(v.title)}`,
      paths.vote(v.id),
      [
        ['Vote ID', `\`${v.id}\``],
        ['Date', v.date],
        ['Legislature', legFull(v.legislature)],
        [
          'Bill',
          v.billId
            ? `${link(`${v.billNumber} ${v.billTitle ?? ''}`.trim(), paths.bill(v.billId))} (\`${v.billId}\`)`
            : 'none',
        ],
        ['Version voted on', voteVersion(v.title)],
        [
          'Amendment',
          v.amendment ? `No. ${v.amendment}${v.subAmendment ? `, sub-amendment ${v.subAmendment}` : ''}` : null,
        ],
        ['Description', v.description],
        ['Result', `${v.y} yea, ${v.n} nay, ${v.e} excused, ${v.a} absent`],
      ],
    ),
    voteSummary(v),
    '',
    '## By party',
    '',
    table(
      ['Party', ...ORDER.map((c) => CHOICE[c])],
      byParty(v).map(([p, r]) => [partyPlural(p === '?' ? null : p), ...ORDER.map((c) => r[c])]),
    ),
    '',
    '## How each legislator voted',
    '',
    table(
      ['Legislator', 'Party', 'District', 'Caucus', 'Vote'],
      [...v.choices]
        .sort((a, b) => ORDER.indexOf(a.choice) - ORDER.indexOf(b.choice) || a.lastName.localeCompare(b.lastName))
        .map((c) => [
          link(`${memberTitleShort(v.chamber)} ${c.name}`, paths.member(c.memberId)),
          c.party,
          c.district,
          c.majority == null ? '' : c.majority ? 'majority' : 'minority',
          CHOICE[c.choice],
        ]),
    ),
    '',
  ].join('\n');
}

function member(m: Member): string {
  return [
    header(`${memberTitle(m.chamber)} ${m.name}, ${legFull(m.legislature)}`, paths.member(m.id), [
      ['Member ID', `\`${m.id}\``],
      ['Person', `${link(m.name, paths.person(m.personId))} (\`${m.personId}\`)`],
      ['Party', partyName(m.party)],
      ['Chamber and district', `${chamberName(m.chamber)} District ${m.district ?? 'unknown'}`],
      ['Caucus', m.majority == null ? null : m.majority ? 'majority' : 'minority'],
      ['Member code', m.code],
      ['Official page', m.code ? akleg.legislator(m.legislature, m.code) : null],
    ]),
    memberSummary(m),
    '',
    `## Voting record (${m.votes.length} roll calls, newest first)`,
    '',
    table(
      ['Date', 'Bill', 'Motion', 'Their vote', 'Yea', 'Nay', 'Roll call'],
      m.votes.map((v) => [
        v.date,
        v.billNumber,
        voteMotion(v.title),
        CHOICE[v.choice],
        v.y,
        v.n,
        link('vote', paths.vote(v.id)),
      ]),
    ),
    '',
  ].join('\n');
}

function person(p: Person): string {
  return [
    header(p.name, paths.person(p.id), [['Person ID', `\`${p.id}\``]]),
    personSummary(p),
    '',
    '## Terms',
    '',
    table(
      ['Legislature', 'Chamber', 'District', 'Party', 'Caucus', 'Yea', 'Nay', 'Missed', 'Voting record'],
      p.terms.map((t) => [
        `${legName(t.legislature)} (${legYears(t.legislature)})`,
        chamberName(t.chamber),
        t.district,
        partyName(t.party),
        t.majority == null ? '' : t.majority ? 'majority' : 'minority',
        t.y,
        t.n,
        t.missed,
        link('term page', paths.member(t.id)),
      ]),
    ),
    '',
  ].join('\n');
}

function legislature(l: Legislature): string {
  return [
    header(`${legFull(l.number)} (${legYears(l.number)})`, paths.legislature(l.number), [
      ['Bills', String(l.bills.length)],
      ['Roll-call votes', String(l.votes.length)],
      ['Members', String(l.members.length)],
    ]),
    '## Members',
    '',
    table(
      ['Legislator', 'Chamber', 'District', 'Party', 'Caucus'],
      l.members.map((m) => [
        link(m.name, paths.member(m.id)),
        chamberName(m.chamber),
        m.district,
        m.party,
        m.majority == null ? '' : m.majority ? 'majority' : 'minority',
      ]),
    ),
    '',
    '## Bills',
    '',
    table(
      ['Bill', 'Title', 'Subjects', 'Status date'],
      l.bills.map((b) => [link(b.number, paths.bill(b.id)), b.title, (b.subjects ?? []).join(', '), b.statusDate]),
    ),
    '',
    '## Roll-call votes (newest first)',
    '',
    table(
      ['Date', 'Chamber', 'Bill', 'Motion', 'Yea', 'Nay', 'Roll call'],
      l.votes.map((v) => [
        v.date,
        chamberName(v.chamber),
        v.billNumber,
        voteMotion(v.title),
        v.y,
        v.n,
        link(`vote ${v.number}`, paths.vote(v.id)),
      ]),
    ),
    '',
  ].join('\n');
}

const RENDER: Record<Kind, (doc: never) => string> = {
  bills: bill,
  'bill-versions': version,
  votes: vote,
  members: member,
  people: person,
  legislatures: legislature,
};

export async function renderMarkdown(kind: Kind, id: string): Promise<string | null> {
  const doc = await getDoc<never>(kind, kind === 'legislatures' ? Number(id) : id);
  return doc === null ? null : RENDER[kind](doc);
}

/** Which record, if any, a request is asking for as Markdown. */
export function markdownTarget(url: URL, accept: string | null): { kind: Kind; id: string; htmlPath: string } | null {
  const m = /^\/(bills|bill-versions|votes|members|people|legislatures)\/([^/]+)$/.exec(url.pathname);
  if (!m) return null;
  const isMd = m[2]!.endsWith('.md');
  if (!isMd && !(accept ?? '').includes('text/markdown')) return null;
  const raw = isMd ? m[2]!.slice(0, -3) : m[2]!;
  return { kind: m[1] as Kind, id: decodeURIComponent(raw), htmlPath: `/${m[1]}/${raw}` };
}
