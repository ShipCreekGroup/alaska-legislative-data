-- Packs the CSVs written by `akleg build` into one DuckDB file, and writes
-- each table as Parquet too. Run from the export directory:
--   cd export && duckdb ak_leg.duckdb < ../scripts/build-duckdb.sql
--
-- Each table X is stored as X_history: every version of every row, with the
-- dates it was valid (ValidFrom inclusive, ValidTo exclusive; ValidTo NULL
-- means current). X itself is a view of the current rows.
-- IDs are text with a type prefix, eg 'bil_34th_HB0016' (see src/tables/ids.ts).

CREATE OR REPLACE TABLE people_history AS
SELECT * FROM read_csv('people_history.csv', header = true, delim = ',', quote = '"', escape = '"', allow_quoted_nulls = false, columns = {
  'PersonId': 'VARCHAR', 'PersonFullName': 'VARCHAR', 'PersonFirstName': 'VARCHAR', 'PersonLastName': 'VARCHAR',
  'PersonMiddleName': 'VARCHAR', 'PersonNickName': 'VARCHAR', 'PersonSuffix': 'VARCHAR', 'ValidFrom': 'DATE', 'ValidTo': 'DATE'
});

CREATE OR REPLACE TABLE members_history AS
SELECT * FROM read_csv('members_history.csv', header = true, delim = ',', quote = '"', escape = '"', allow_quoted_nulls = false, columns = {
  'MemberId': 'VARCHAR', 'LegislatureNumber': 'INTEGER', 'PersonId': 'VARCHAR', 'MemberChamber': 'VARCHAR',
  'MemberDistrict': 'VARCHAR', 'MemberIsMajority': 'BOOLEAN', 'MemberParty': 'VARCHAR', 'MemberCode': 'VARCHAR',
  'MemberFormerCodes': 'VARCHAR', 'ValidFrom': 'DATE', 'ValidTo': 'DATE'
});

CREATE OR REPLACE TABLE bills_history AS
SELECT * REPLACE (
  -- Subjects are written as '[A, B]'; no subject contains ', '.
  CASE WHEN BillSubjects IS NULL THEN NULL
       WHEN BillSubjects = '[]' THEN []::VARCHAR[]
       ELSE string_split(BillSubjects[2:-2], ', ') END AS BillSubjects
)
FROM read_csv('bills_history.csv', header = true, delim = ',', quote = '"', escape = '"', allow_quoted_nulls = false, columns = {
  'BillId': 'VARCHAR', 'LegislatureNumber': 'INTEGER', 'BillNumber': 'VARCHAR', 'BillName': 'VARCHAR',
  'BillShortTitle': 'VARCHAR', 'BillStatusDate': 'DATE', 'BillSubjects': 'VARCHAR', 'BillVetoed': 'BOOLEAN', 'ValidFrom': 'DATE', 'ValidTo': 'DATE'
});

CREATE OR REPLACE TABLE votes_history AS
SELECT *
FROM read_csv('votes_history.csv', header = true, delim = ',', quote = '"', escape = '"', allow_quoted_nulls = false, columns = {
  'VoteId': 'VARCHAR', 'BillId': 'VARCHAR', 'VoteNumber': 'INTEGER', 'VoteDate': 'DATE', 'VoteTitle': 'VARCHAR',
  'LegislatureNumber': 'INTEGER', 'VoteChamber': 'VARCHAR', 'VoteDescription': 'VARCHAR',
  'VoteAmendmentNumber': 'INTEGER', 'VoteSubAmendmentNumber': 'INTEGER', 'VoteAmendmentId': 'VARCHAR', 'ValidFrom': 'DATE', 'ValidTo': 'DATE'
});

CREATE OR REPLACE TABLE choices_history AS
SELECT * FROM read_csv('choices_history.csv', header = true, delim = ',', quote = '"', escape = '"', allow_quoted_nulls = false, columns = {
  'VoteId': 'VARCHAR', 'MemberId': 'VARCHAR', 'Choice': 'VARCHAR', 'ValidFrom': 'DATE', 'ValidTo': 'DATE'
});

CREATE OR REPLACE TABLE billVersions_history AS
SELECT * FROM read_csv('bill_versions_history.csv', header = true, delim = ',', quote = '"', escape = '"', allow_quoted_nulls = false, max_line_size = 100000000, columns = {
  'BillVersionId': 'VARCHAR', 'BillId': 'VARCHAR', 'BillVersionLetter': 'VARCHAR', 'BillVersionTitle': 'VARCHAR',
  'BillVersionName': 'VARCHAR', 'BillVersionIntroDate': 'DATE', 'BillVersionPassedHouse': 'DATE',
  'BillVersionPassedSenate': 'DATE', 'BillVersionWorkOrder': 'VARCHAR', 'BillVersionPdfUrl': 'VARCHAR',
  'BillVersionFullText': 'VARCHAR', 'ValidFrom': 'DATE', 'ValidTo': 'DATE'
});

-- NOT NULL constraints.
ALTER TABLE people_history ALTER PersonId SET NOT NULL;
ALTER TABLE people_history ALTER PersonFullName SET NOT NULL;
ALTER TABLE people_history ALTER PersonFirstName SET NOT NULL;
ALTER TABLE people_history ALTER PersonLastName SET NOT NULL;
ALTER TABLE members_history ALTER MemberId SET NOT NULL;
ALTER TABLE members_history ALTER LegislatureNumber SET NOT NULL;
ALTER TABLE members_history ALTER PersonId SET NOT NULL;
ALTER TABLE members_history ALTER MemberChamber SET NOT NULL;
ALTER TABLE bills_history ALTER BillId SET NOT NULL;
ALTER TABLE bills_history ALTER LegislatureNumber SET NOT NULL;
ALTER TABLE bills_history ALTER BillNumber SET NOT NULL;
ALTER TABLE votes_history ALTER VoteId SET NOT NULL;
ALTER TABLE votes_history ALTER VoteNumber SET NOT NULL;
ALTER TABLE votes_history ALTER LegislatureNumber SET NOT NULL;
ALTER TABLE votes_history ALTER VoteChamber SET NOT NULL;
ALTER TABLE votes_history ALTER VoteDescription SET NOT NULL;
ALTER TABLE choices_history ALTER VoteId SET NOT NULL;
ALTER TABLE choices_history ALTER MemberId SET NOT NULL;
ALTER TABLE billVersions_history ALTER BillVersionId SET NOT NULL;
ALTER TABLE billVersions_history ALTER BillId SET NOT NULL;
ALTER TABLE billVersions_history ALTER BillVersionLetter SET NOT NULL;
ALTER TABLE billVersions_history ALTER BillVersionTitle SET NOT NULL;
ALTER TABLE billVersions_history ALTER BillVersionName SET NOT NULL;
ALTER TABLE billVersions_history ALTER BillVersionIntroDate SET NOT NULL;
ALTER TABLE billVersions_history ALTER BillVersionWorkOrder SET NOT NULL;
ALTER TABLE billVersions_history ALTER BillVersionPdfUrl SET NOT NULL;
ALTER TABLE people_history ALTER ValidFrom SET NOT NULL;
ALTER TABLE members_history ALTER ValidFrom SET NOT NULL;
ALTER TABLE bills_history ALTER ValidFrom SET NOT NULL;
ALTER TABLE votes_history ALTER ValidFrom SET NOT NULL;
ALTER TABLE choices_history ALTER ValidFrom SET NOT NULL;
ALTER TABLE billVersions_history ALTER ValidFrom SET NOT NULL;

-- Current rows.
CREATE OR REPLACE VIEW people AS SELECT * EXCLUDE (ValidFrom, ValidTo) FROM people_history WHERE ValidTo IS NULL;
CREATE OR REPLACE VIEW members AS SELECT * EXCLUDE (ValidFrom, ValidTo) FROM members_history WHERE ValidTo IS NULL;
CREATE OR REPLACE VIEW bills AS SELECT * EXCLUDE (ValidFrom, ValidTo) FROM bills_history WHERE ValidTo IS NULL;
CREATE OR REPLACE VIEW votes AS SELECT * EXCLUDE (ValidFrom, ValidTo) FROM votes_history WHERE ValidTo IS NULL;
CREATE OR REPLACE VIEW choices AS SELECT * EXCLUDE (ValidFrom, ValidTo) FROM choices_history WHERE ValidTo IS NULL;
CREATE OR REPLACE VIEW billVersions AS SELECT * EXCLUDE (ValidFrom, ValidTo) FROM billVersions_history WHERE ValidTo IS NULL;

-- Parquet copies, for DuckDB-WASM and other tools.
COPY people_history TO 'people_history.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY (FROM people) TO 'people.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY members_history TO 'members_history.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY (FROM members) TO 'members.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY bills_history TO 'bills_history.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY (FROM bills) TO 'bills.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY votes_history TO 'votes_history.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY (FROM votes) TO 'votes.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY choices_history TO 'choices_history.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY (FROM choices) TO 'choices.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY billVersions_history TO 'bill_versions_history.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY (FROM billVersions) TO 'bill_versions.parquet' (FORMAT parquet, COMPRESSION zstd);
