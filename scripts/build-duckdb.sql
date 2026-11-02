-- Packs the CSVs written by `akleg scrape` into a single DuckDB file.
-- Run from the export directory:
--   cd export && duckdb ak_leg.duckdb < ../scripts/build-duckdb.sql
-- Table and column names/types match the database published before the
-- TypeScript rewrite.

CREATE OR REPLACE TABLE people AS
SELECT * FROM read_csv('people.csv', header = true, delim = ',', quote = '"', escape = '"', allow_quoted_nulls = false, columns = {
  'PersonId': 'VARCHAR', 'PersonFullName': 'VARCHAR', 'PersonFirstName': 'VARCHAR', 'PersonLastName': 'VARCHAR',
  'PersonMiddleName': 'VARCHAR', 'PersonNickName': 'VARCHAR', 'PersonSuffix': 'VARCHAR'
});

CREATE OR REPLACE TABLE members AS
SELECT * FROM read_csv('members.csv', header = true, delim = ',', quote = '"', escape = '"', allow_quoted_nulls = false, columns = {
  'MemberId': 'VARCHAR', 'LegislatureNumber': 'INTEGER', 'PersonId': 'VARCHAR', 'MemberChamber': 'VARCHAR',
  'MemberDistrict': 'VARCHAR', 'MemberIsMajority': 'BOOLEAN', 'MemberParty': 'VARCHAR', 'MemberCode': 'VARCHAR',
  'MemberFormerCodes': 'VARCHAR'
});

CREATE OR REPLACE TABLE bills AS
SELECT * REPLACE (
  -- Subjects are written as '[A, B]'; no subject contains ', '.
  CASE WHEN BillSubjects IS NULL THEN NULL
       WHEN BillSubjects = '[]' THEN []::VARCHAR[]
       ELSE string_split(BillSubjects[2:-2], ', ') END AS BillSubjects
)
FROM read_csv('bills.csv', header = true, delim = ',', quote = '"', escape = '"', allow_quoted_nulls = false, columns = {
  'BillId': 'VARCHAR', 'LegislatureNumber': 'INTEGER', 'BillNumber': 'VARCHAR', 'BillName': 'VARCHAR',
  'BillShortTitle': 'VARCHAR', 'BillStatusDate': 'DATE', 'BillSubjects': 'VARCHAR', 'BillVetoed': 'BOOLEAN'
});

CREATE OR REPLACE TABLE votes AS
SELECT *
FROM read_csv('votes.csv', header = true, delim = ',', quote = '"', escape = '"', allow_quoted_nulls = false, columns = {
  'VoteId': 'VARCHAR', 'BillId': 'VARCHAR', 'VoteNumber': 'INTEGER', 'VoteDate': 'DATE', 'VoteTitle': 'VARCHAR',
  'LegislatureNumber': 'INTEGER', 'VoteChamber': 'VARCHAR', 'VoteDescription': 'VARCHAR',
  'VoteBillAmendmentNumber': 'DECIMAL(9,1)'
});

CREATE OR REPLACE TABLE choices AS
SELECT * FROM read_csv('choices.csv', header = true, delim = ',', quote = '"', escape = '"', allow_quoted_nulls = false, columns = {
  'ChoiceId': 'VARCHAR', 'VoteId': 'VARCHAR', 'MemberId': 'VARCHAR', 'Choice': 'VARCHAR'
});

CREATE OR REPLACE TABLE billVersions AS
SELECT * FROM read_csv('bill_versions.csv', header = true, delim = ',', quote = '"', escape = '"', allow_quoted_nulls = false, max_line_size = 100000000, columns = {
  'BillVersionId': 'VARCHAR', 'BillId': 'VARCHAR', 'BillVersionLetter': 'VARCHAR', 'BillVersionTitle': 'VARCHAR',
  'BillVersionName': 'VARCHAR', 'BillVersionIntroDate': 'DATE', 'BillVersionPassedHouse': 'DATE',
  'BillVersionPassedSenate': 'DATE', 'BillVersionWorkOrder': 'VARCHAR', 'BillVersionPdfUrl': 'VARCHAR',
  'BillVersionFullText': 'VARCHAR'
});

-- Same NOT NULL constraints as before.
ALTER TABLE people ALTER PersonId SET NOT NULL;
ALTER TABLE people ALTER PersonFullName SET NOT NULL;
ALTER TABLE people ALTER PersonFirstName SET NOT NULL;
ALTER TABLE people ALTER PersonLastName SET NOT NULL;
ALTER TABLE members ALTER MemberId SET NOT NULL;
ALTER TABLE members ALTER LegislatureNumber SET NOT NULL;
ALTER TABLE members ALTER PersonId SET NOT NULL;
ALTER TABLE members ALTER MemberChamber SET NOT NULL;
ALTER TABLE bills ALTER BillId SET NOT NULL;
ALTER TABLE bills ALTER LegislatureNumber SET NOT NULL;
ALTER TABLE bills ALTER BillNumber SET NOT NULL;
ALTER TABLE votes ALTER VoteId SET NOT NULL;
ALTER TABLE votes ALTER VoteNumber SET NOT NULL;
ALTER TABLE votes ALTER LegislatureNumber SET NOT NULL;
ALTER TABLE votes ALTER VoteChamber SET NOT NULL;
ALTER TABLE votes ALTER VoteDescription SET NOT NULL;
ALTER TABLE choices ALTER ChoiceId SET NOT NULL;
ALTER TABLE choices ALTER VoteId SET NOT NULL;
ALTER TABLE choices ALTER MemberId SET NOT NULL;
ALTER TABLE billVersions ALTER BillVersionId SET NOT NULL;
ALTER TABLE billVersions ALTER BillId SET NOT NULL;
ALTER TABLE billVersions ALTER BillVersionLetter SET NOT NULL;
ALTER TABLE billVersions ALTER BillVersionTitle SET NOT NULL;
ALTER TABLE billVersions ALTER BillVersionName SET NOT NULL;
ALTER TABLE billVersions ALTER BillVersionIntroDate SET NOT NULL;
ALTER TABLE billVersions ALTER BillVersionWorkOrder SET NOT NULL;
ALTER TABLE billVersions ALTER BillVersionPdfUrl SET NOT NULL;
