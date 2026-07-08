---
name: alaska-legislative-data
description: Search and analyze the Alaska State Legislature database — bills, votes, roll-call choices, legislators, and full bill text across legislatures 12–34 (1981–2026).
---

# Alaska Legislative Data

A DuckDB database of the Alaska State Legislature: bills, roll-call votes, individual legislator choices, members, people, and full bill text. Covers legislatures **12–34** (roughly 1981–2026; leg 34 = 2025–2026, the current one). Vote records are richest from the ~19th legislature onward.

## Setup

The database ships as a release asset, scraped daily by a cron job. Download and extract once per session:

```bash
curl -sL https://install.duckdb.org | sh   # if duckdb CLI not present
curl -sL -o /tmp/export.zip https://github.com/ShipCreekGroup/alaska-legislative-data/releases/latest/download/export.zip
cd /tmp && unzip -o export.zip   # -> /tmp/export/ak_leg.duckdb (+ csv copies of each table)
```

```bash
duckdb /tmp/export/ak_leg.duckdb -readonly -box <<'SQL'
SELECT ...;
SQL
```

## Schema

Six tables. Prefixed column names (e.g. every `bills` column starts with `Bill`). Row counts as of July 2026.

**bills** (~24.9k) — one row per bill per legislature.
`BillId` (PK, e.g. `34:HB 225`) · `LegislatureNumber` (12–34) · `BillNumber` (`HB 225`) · `BillName` (usually NULL) · `BillShortTitle` (use this as the title) · `BillStatusDate` · `BillSubjects` (**VARCHAR[]** array of topic tags) · `BillVetoed` (bool, nullable).

**billVersions** (~13k, ~7k bills) — text of each bill version.
`BillVersionId` · `BillId` (FK→bills) · `BillVersionLetter` (A, B…) · `BillVersionTitle` · `BillVersionName` · intro/passed dates · `BillVersionPdfUrl` · `BillVersionFullText` (can be large — don't `SELECT *` this across many rows).

**votes** (~21.6k) — one row per roll call.
`VoteId` (PK, e.g. `34:H:411`) · `BillId` (FK→bills, **nullable** — non-bill votes like judicial confirmations aren't tied to a bill) · `VoteNumber` · `VoteDate` · `VoteTitle` (contains `\n`) · `LegislatureNumber` · `VoteChamber` (`H`/`S`) · `VoteDescription` · `VoteBillAmendmentNumber` (decimal encoding: `1.0` = amendment 1, `1.1` = 1st sub-amendment to amendment 1, `2.0` = amendment 2; sortable).

**choices** (~678k) — one row per legislator per roll call. This is the roll-call detail.
`ChoiceId` · `VoteId` (FK→votes) · `MemberId` (FK→members) · `Choice` ∈ `Y` (yea), `N` (nay), `E` (excused), `A` (absent).

**members** (~2.1k) — one row per person per legislature (their seat that term).
`MemberId` (PK) · `LegislatureNumber` · `PersonId` (FK→people) · `MemberChamber` (`H`/`S`) · `MemberDistrict` (House = number 1–40; Senate = letter A–T) · `MemberIsMajority` (bool — in the majority caucus) · `MemberParty` (`D`,`R`,`I`,`L`,`N`=nonpartisan/undeclared, NULL) · `MemberCode` (3-letter code, see below) · `MemberFormerCodes` (semicolon-separated earlier codes).

**people** (~600) — one row per real person.
`PersonId` (PK, e.g. `Neal Foster:26`) · full/first/last/middle/nick/suffix names.

### Join graph
```
people ─PersonId→ members ─MemberId→ choices ←VoteId─ votes ─BillId→ bills ←BillId─ billVersions
```
- Party/district/chamber live on **members** and are **per-legislature** — always join choices→members and filter/group by the member's term, not the person. Don't assume a legislator's current party/majority status held historically.
- One person can hold **two memberships in a single legislature** — `MemberId` includes the chamber precisely because members sometimes switch House↔Senate mid-term (appointed to fill a vacancy). So don't assume one row per person per legislature when someone switched chambers.

### Identifying people (important)
- **`PersonId` is the only stable key for a real person across terms — always use it, not names.** Format is `{FullName}:{first legislature they held office}` (e.g. `Neal Foster:26`). It is *not* a per-name counter; the suffix is their first legislature. It happens to separate two people who share a name only because they first took office in different years (there are two "Neal Foster"s and two "Mike Miller"s).
- Names are unreliable for lookup: they repeat, and they change (marriage, nickname → legal name). To find someone by name, query `people`, eyeball the matches, grab the `PersonId`, then join through `members`.
- **Name changes:** the `PersonId` string is frozen at the person's *original* name even after they change it, and `PersonFullName` may show either the old or new name depending on when the data was scraped. So a `WHERE PersonLastName = 'Frier'` can silently miss that person's earlier records. Prefer `PersonId`.

### Member codes and name changes
- `MemberCode` is a 3-letter code that is unique only *within* one legislature. It is **not stable for a person across legislatures and not unique across them**, and it can even change mid-legislature when a member changes their name (e.g. Robyn Burke → Frier in the 34th: code went `BUR` → `FRB`). Never use `MemberCode` as a person key — use `PersonId`.
- When a code changes, `MemberCode` holds the current one and `MemberFormerCodes` holds the earlier one(s), semicolon-separated. **You normally don't need to touch these** — the pipeline already resolves every vote (cast under whichever code was current at the time) to the single correct `MemberId`, so joining `choices → members → people` counts all of a member's votes correctly across a name change automatically.

## Common queries

**Find bills by subject/topic** (subjects is an array — use `list_contains` or `UNNEST`):
```sql
SELECT BillId, BillShortTitle, BillStatusDate
FROM bills
WHERE LegislatureNumber = 34 AND list_contains(BillSubjects, 'EDUCATION')
ORDER BY BillStatusDate DESC;
```

**Vote tally by party for one roll call:**
```sql
SELECT c.Choice, m.MemberParty, count(*) n
FROM choices c JOIN members m ON c.MemberId = m.MemberId
WHERE c.VoteId = '34:H:411'
GROUP BY 1,2 ORDER BY 1,2;
```

**How a named legislator voted on final-passage votes** (career-spanning via PersonId):
```sql
SELECT v.VoteDate, b.BillNumber, b.BillShortTitle, c.Choice
FROM people p
JOIN members m  ON m.PersonId = p.PersonId
JOIN choices c  ON c.MemberId = m.MemberId
JOIN votes v    ON v.VoteId   = c.VoteId
LEFT JOIN bills b ON b.BillId  = v.BillId
WHERE p.PersonFullName = 'Neal Foster'
  AND v.VoteTitle ILIKE '%Final Passage%'
ORDER BY v.VoteDate DESC;
```

**Who voted YES on a given bill's passage:**
```sql
SELECT p.PersonFullName, m.MemberParty, m.MemberDistrict
FROM votes v
JOIN choices c ON c.VoteId = v.VoteId
JOIN members m ON m.MemberId = c.MemberId
JOIN people  p ON p.PersonId = m.PersonId
WHERE v.BillId = '34:HB 176' AND c.Choice = 'Y'
  AND v.VoteTitle ILIKE '%Final Passage%';
```

**Search bill full text:**
```sql
SELECT bv.BillId, b.BillShortTitle, bv.BillVersionPdfUrl
FROM billVersions bv JOIN bills b ON b.BillId = bv.BillId
WHERE bv.BillVersionFullText ILIKE '%carbon%' AND b.LegislatureNumber = 34;
```

**Find contentious (close-margin) votes on a topic** — "contentious" = a roll call decided by a narrow margin. Tally each vote with `count(*) FILTER`, keep the contested ones (`n > 0`), and rank by `abs(y-n)`:
```sql
WITH tally AS (
  SELECT v.VoteId, v.BillId, v.VoteChamber, v.VoteDate,
         replace(v.VoteTitle, chr(10), ' ') AS title,
         count(*) FILTER (WHERE c.Choice='Y') AS y,
         count(*) FILTER (WHERE c.Choice='N') AS n
  FROM votes v
  JOIN choices c ON c.VoteId = v.VoteId
  JOIN bills   b ON b.BillId  = v.BillId
  WHERE b.LegislatureNumber >= 33
    AND list_has_any(b.BillSubjects, ['ENERGY','HOUSING','PERMANENT FUND','UTILITIES','WAGES','FOOD'])
  GROUP BY 1,2,3,4,5
)
SELECT VoteDate::date, BillId, VoteChamber, y, n, title
FROM tally
WHERE n > 0 AND abs(y - n) <= 5   -- tighten/loosen this to taste
ORDER BY abs(y - n), VoteDate DESC;
```
Lessons that make the results usable:
- **`list_has_any(BillSubjects, [...])` beats chained `list_contains` ORs** for matching any of several subject tags.
- **Budget/appropriation bills swamp the close-vote list and are hard to interpret.** The omnibus operating/capital budgets (e.g. `HB 39`, `HB 53`, `HB 263`, `SB 214`) carry nearly every subject tag, so their dozens of *amendment* roll calls dominate any topic filter. For substantive, readable results, restrict to decisive votes with `VoteTitle ILIKE '%Final Passage%' OR '%Concur%' OR '%Third Reading%'`, and/or exclude the big approp bills.
- **The modern House is split ~20–20**, so a genuinely contested House vote is often a literal `20`–`20` tie — don't treat `abs(y-n)=0` as a data error.
- **Add a party breakdown to turn a close vote into accountability signal** — join `choices → members` and `GROUP BY MemberParty` (do this per-`VoteId`, and remember party is per-legislature; see the join-graph notes). Votes where a caucus splits internally, or where a member crosses party lines, are the most revealing.

## Gotchas
- **Identify people by `PersonId`, never by name** — names change and repeat, and `PersonId` survives name changes (see "Identifying people" above).
- Member codes are not person keys; vote joins across a name change already resolve correctly through `choices → members` (see "Member codes and name changes").
- One person can have two `members` rows in one legislature (House↔Senate switch mid-term).
- `votes.BillId` is nullable (non-bill votes like judicial confirmations) — use `LEFT JOIN bills`.
- Subjects are arrays, not strings; `list_contains(BillSubjects, 'TAG')`, and `UNNEST` to list/count tags. Tags are uppercase (e.g. `'FISH & GAME (BOTH)'`).
- Titles hold real newlines (`\n`); match with `ILIKE '%...%'` rather than exact equality.
- Bill number prefixes are mostly `HB`/`SB`/`HJR`/`SJR`/`HCR`/`SCR`/`HR`/`SR`, but 4-letter prefixes exist too (e.g. `HSCR`, `SSCR`). Match on prefix loosely if filtering by bill type.
- `(LegislatureNumber, BillNumber)` is unique, so `BillId` = `'{leg}:{BillNumber}'` uniquely identifies a bill.
- Members only reliably go back to the **10th legislature**; earlier terms (1st–9th) are hand-curated and sparser — districts are often blank, and party/other fields may be NULL.
- To confirm the current/latest legislature, `SELECT max(LegislatureNumber) FROM bills;` rather than hardcoding.
