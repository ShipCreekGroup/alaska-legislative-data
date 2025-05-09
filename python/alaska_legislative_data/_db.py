from __future__ import annotations

import functools
import os
from pathlib import Path
from typing import Annotated, get_type_hints
from urllib.parse import urlparse

import dotenv
import duckdb
import ibis
from ibis import ir
from ibis.backends.duckdb import Backend as DuckDBBackend
from ibis.backends.sql import BaseBackend as SQLBackend

LEGISLATURE_NUMBER_TYPE = "!int16"


class TableSchema:
    @classmethod
    def ibis_schema(cls) -> ibis.Schema:
        schema_dict = {}
        for attr_name, attr_value in get_type_hints(cls, include_extras=True).items():
            raw_type = attr_value.__metadata__[0]
            schema_dict[attr_name] = ibis.dtype(raw_type)
        return ibis.schema(schema_dict)


class LegislatureSchema(TableSchema):
    """"""

    LegislatureNumber: Annotated[ir.IntegerColumn, LEGISLATURE_NUMBER_TYPE]
    """eg 31 for the 31st legislature."""
    LegislatureStartYear: Annotated[ir.IntegerColumn, "!int16"]
    """eg 2021 for the 31st legislature."""
    LegislatureEndYear: Annotated[ir.IntegerColumn, "!int16"]
    """eg 2022 for the 31st legislature."""


class LegislatureTable(ibis.Table, LegislatureSchema):
    pass


class LegislatureSessionSchema(TableSchema):
    """A session of the Alaska legislature."""

    LegislatureSessionId: Annotated[ir.StringColumn, "!string"]
    """Of the form '{LegislatureNumber}:{LegislatureSessionCode}'."""
    LegislatureNumber: Annotated[ir.IntegerColumn, LEGISLATURE_NUMBER_TYPE]
    """eg 31 for the 31st legislature. Reference to the Legislature table."""
    LegislatureSessionCode: Annotated[ir.StringColumn, "int8"]
    """eg '10' for the first regular session, '20' for the second regular session,
    '11' for the first special session, '21' for the second special session, etc."""
    LegislatureSessionTitle: Annotated[ir.StringColumn, "string"]
    """eg '1st Regular Session'"""
    LegislatureSessionStartDate: Annotated[ir.DateColumn, "date"]
    """eg '2021-01-19' for the first session of the 31st legislature."""
    LegislatureSessionEndDate: Annotated[ir.DateColumn, "date"]
    """eg '2021-05-19' for the first session of the 31st legislature."""


class LegislatureSessionTable(ibis.Table, LegislatureSessionSchema):
    pass


class PersonSchema(TableSchema):
    """A person in the Alaska legislature. They may be shared between multiple Memberships."""

    PersonId: Annotated[ir.StringColumn, "!string"]
    """A unique ID in the format "{FullName}>:{first session they gained office}".

    Eg "John Doe:31" for John Doe who first was elected to the 31st legislature.
    The idea here is that this will be stable, unique, human-readable,
    and adding new people after future elections will be straightforward."""
    FullName: Annotated[ir.StringColumn, "!string"]
    FirstName: Annotated[ir.StringColumn, "!string"]
    LastName: Annotated[ir.StringColumn, "!string"]
    MiddleName: Annotated[ir.StringColumn, "string"]
    NickName: Annotated[ir.StringColumn, "string"]
    Suffix: Annotated[ir.StringColumn, "string"]


class PersonTable(ibis.Table, PersonSchema):
    pass


class MemberSchema(TableSchema):
    """A person's membership in the Alaska legislature.

    The combo of PersonId and LegislatureNumber is unique.
    """

    MemberId: Annotated[ir.StringColumn, "!string"]
    """Unique id for this membership of the form '{LegislatureNumber}:{Chamber}:{District}:{PersonId}'.
    
    Using just the LegislatureNumber and PersonId is inadequate,
    as there are some instances of someone switching from the house
    to the senate when they are appointed to fill a vacancy.
    Theoretically we COULD just use the District, but for the 7th legislature,
    "H Meland:7" does this switch between the H and S,
    BUT for data this old, we don't actually know the district,
    which leads to both memberships having the ID of `7::H Meland:7`.
    By including the Chamber, we end up with the IDs of `7:H::H Meland:7`
    and `7:S::H Meland:7`, which are unique.
    """
    LegislatureNumber: Annotated[ir.IntegerColumn, LEGISLATURE_NUMBER_TYPE]
    """eg 31 for the 31st legislature."""
    PersonId: Annotated[ir.StringColumn, "!string"]
    """Reference to the Person table."""
    MemberCode: Annotated[ir.StringColumn, "string"]
    """Three letter code that is unique to each person in a given legislature.
    
    It is NOT stable with a person across legislatures,
    and it is NOT unique across legislatures.
    This can be null, for really old legislatures.
    """
    Chamber: Annotated[ir.StringColumn, "string"]
    """eg 'H' or 'S' for house or senate."""
    District: Annotated[ir.StringColumn, "string"]
    """eg 'A' or 'T' (for senate districts) or '1' or '40' (for house districts) """
    Party: Annotated[ir.StringColumn, "string"]
    """eg 'R' or 'D'."""
    IsMajority: Annotated[ir.BooleanColumn, "boolean"]
    """True if this person is in the majority party."""
    IsActive: Annotated[ir.BooleanColumn, "boolean"]
    """not sure what this means"""
    Comment: Annotated[ir.BooleanColumn, "string"]
    """human-readable details like "resigned on 3/14/2022" or "Speaker of the House"."""
    Phone: Annotated[ir.StringColumn, "string"]
    """eg '465-XXXX'"""
    EMail: Annotated[ir.StringColumn, "string"]
    Building: Annotated[ir.StringColumn, "string"]
    Room: Annotated[ir.StringColumn, "string"]


class MemberTable(ibis.Table, MemberSchema):
    pass


class BillSchema(TableSchema):
    """A bill in the Alaska legislature."""

    BillId: Annotated[ir.StringColumn, "!string"]
    """Of the form '{LegislatureNumber}:{BillNumber}'."""
    LegislatureNumber: Annotated[ir.IntegerColumn, LEGISLATURE_NUMBER_TYPE]
    BillNumber: Annotated[ir.StringColumn, "!string"]
    BillName: Annotated[ir.StringColumn, "string"]
    Documents: Annotated[ir.ArrayColumn, "array<json>"]
    PartialVeto: Annotated[ir.BooleanColumn, "boolean"]
    Vetoed: Annotated[ir.BooleanColumn, "boolean"]
    ShortTitle: Annotated[ir.StringColumn, "string"]
    StatusCode: Annotated[ir.StringColumn, "string"]
    StatusText: Annotated[ir.StringColumn, "string"]
    Flag1: Annotated[ir.StringColumn, "string"]
    """"G", "H", "X", "S", or NULL"""
    Flag2: Annotated[ir.IntegerColumn, "uint8"]
    """0 to 7"""
    StatusDate: Annotated[ir.DateColumn, "date"]
    StatusAndThen: Annotated[ir.ArrayColumn, "array<json>"]
    StatusSummaryCode: Annotated[ir.StringColumn, "string"]
    OnFloor: Annotated[ir.StringColumn, "string"]
    # NotKnown: Annotated[ir.StringColumn, "string"]  # This is always null
    Filler: Annotated[ir.StringColumn, "string"]
    Lock: Annotated[ir.StringColumn, "string"]
    """
    "H", "S", "B", NULL, or "\x00" (guessing an encoding error meaning NULL)
    """
    AllMeetings: Annotated[ir.ArrayColumn, "array<json>"]
    Meetings: Annotated[ir.ArrayColumn, "array<json>"]
    Subjects: Annotated[ir.ArrayColumn, "array<string>"]
    """Tags added by the librarians, eg "EDUCATION", "HEALTHCARE", "FISH"."""
    ManifestErrors: Annotated[ir.ArrayColumn, "array<json>"]
    Statutes: Annotated[ir.ArrayColumn, "array<json>"]
    CurrentCommittee: Annotated[
        ir.StructColumn,
        "struct<Chamber: string, Code: string, Catagory: string, Name: string, MeetingDays: string, Location: string, StartTime: string, EndTime: string, Email: string>",
    ]


class BillTable(ibis.Table, BillSchema):
    pass


class BillVersionSchema(TableSchema):
    """A version of a bill in the Alaska legislature."""

    BillVersionId: Annotated[ir.StringColumn, "!string"]
    """Of the form '{LegislatureNumber}:{BillNumber}:{BillVersionLetter}'."""
    BillId: Annotated[ir.StringColumn, "!string"]
    """Reference to the Bill table."""
    BillVersionLetter: Annotated[ir.StringColumn, "!string"]
    """eg 'A' or 'B'"""
    BillVersionTitle: Annotated[ir.StringColumn, "!string"]
    """eg 'An Act relating to the Alaska Pioneers' Home and the Alaska Veterans' Home.'"""
    BillVersionName: Annotated[ir.StringColumn, "!string"]
    """eg 'HB 1 A'"""
    BillVersionIntroDate: Annotated[ir.DateColumn, "date"]
    """eg '2021-01-19'"""
    BillVersionPassedHouse: Annotated[ir.DateColumn, "date"]
    BillVersionPassedSenate: Annotated[ir.DateColumn, "date"]
    BillVersionWorkOrder: Annotated[ir.StringColumn, "!string"]
    BillVersionPdfUrl: Annotated[ir.StringColumn, "!string"]
    BillVersionFullText: Annotated[ir.StringColumn, "string"]


class BillVersionTable(ibis.Table, BillVersionSchema):
    pass


class VoteSchema(TableSchema):
    VoteId: Annotated[ir.StringColumn, "!string"]
    LegislatureNumber: Annotated[ir.IntegerColumn, LEGISLATURE_NUMBER_TYPE]
    VoteChamber: Annotated[ir.StringColumn, "!string"]
    """Eg 'H' or 'S'"""
    VoteNumber: Annotated[ir.StringColumn, "!uint16"]
    """eg '132'"""
    VoteDate: Annotated[ir.StringColumn, "date"]
    """This is very rarely null"""
    VoteTitle: Annotated[ir.StringColumn, "!string"]
    BillId: Annotated[ir.StringColumn, "string"]
    """Reference to the Bill table. Might be NULL, because some votes are not on bills.
    
    Eg the vote to confirm a judge."""
    VoteBillAmendmentNumber: Annotated[ir.DecimalColumn, "decimal(9,1)"]
    """If this vote is for an amendment of a bill, what is the amendment number?
    
    1.0 = first amendment
    1.1 = 1st amendment to the first amendment
    1.2 = 2nd amendment to the first amendment
    2.0 = 2nd amendment
    etc.
    """


class VoteTable(ibis.Table, VoteSchema):
    pass


class ChoiceSchema(TableSchema):
    """A Member's choice on a Vote."""

    ChoiceId: Annotated[ir.StringColumn, "!string"]
    VoteId: Annotated[ir.StringColumn, "!string"]
    """Reference to the Vote table."""
    MemberId: Annotated[ir.StringColumn, "!string"]
    """Reference to the Member table."""
    Choice: Annotated[ir.StringColumn, "!string"]
    """Y, N, A, E, or NULL"""


class ChoiceTable(ibis.Table, ChoiceSchema):
    pass


class BackendMixin:
    def __init__(
        self, db: SQLBackend | str | Path, *, check_structure: bool = True, **kwargs
    ):
        if isinstance(db, BackendMixin):
            db = db._db
        if not isinstance(db, SQLBackend):
            db = ibis.connect(db, **kwargs)
        self._db = db
        if check_structure:
            _assert_structure_matches(self._db, DDL)

    def __getattr__(self, name):
        return getattr(self._db, name)

    @functools.cached_property
    def Legislature(self) -> LegislatureTable:
        """Table of legislatures. Each person may have multiple `LegislatureSessions`"""
        return self.table("legislatures")

    @functools.cached_property
    def LegislatureSession(self) -> LegislatureSessionTable:
        """Table of `LegislativeSession`s."""
        return self.table("legislature_sessions")

    @functools.cached_property
    def Person(self) -> PersonTable:
        """Table of people. Each person may have multiple `Member`s."""
        return self.table("people")

    @functools.cached_property
    def Member(self) -> MemberTable:
        """Table of memberships (combo of `Person` and legislature)."""
        return self.table("members")

    @functools.cached_property
    def Bill(self) -> BillTable:
        """Table of bills."""
        return self.table("bills")

    @functools.cached_property
    def BillVersion(self) -> BillVersionTable:
        """Table of bills versions."""
        return self.table("billVersions")

    @functools.cached_property
    def Vote(self) -> VoteTable:
        """Table of roll call votes (where each member's choice is recorded)."""
        return self.table("votes")

    @functools.cached_property
    def Choice(self) -> ChoiceTable:
        """Table of choices on `Vote`s by `Member`s."""
        return self.table("choices")

    # This is needed so that when you do `db.table("foo")`, the resulting table
    # thinks it's backend is self._db, not self.
    def table(self, *args, **kwargs):
        return self._db.table(*args, **kwargs)

    @classmethod
    def create_tables(cls, db: DuckDBBackend | duckdb.DuckDBPyConnection) -> None:
        if isinstance(db, DuckDBBackend):
            conn = db.con
        else:
            conn = db
        conn.sql(DDL)


class Backend(BackendMixin, DuckDBBackend):
    pass


SCHEMAS = {
    "legislatures": LegislatureSchema.ibis_schema(),
    "legislature_sessions": LegislatureSessionSchema.ibis_schema(),
    "people": PersonSchema.ibis_schema(),
    "members": MemberSchema.ibis_schema(),
    "bills": BillSchema.ibis_schema(),
    "votes": VoteSchema.ibis_schema(),
    "choices": ChoiceSchema.ibis_schema(),
}

DDL = """
BEGIN TRANSACTION;
CREATE TABLE legislatures(
    LegislatureNumber SMALLINT PRIMARY KEY CHECK (LegislatureNumber > 0 AND LegislatureNumber < 100),
    LegislatureStartYear SMALLINT NOT NULL,
    LegislatureEndYear SMALLINT NOT NULL
);

CREATE TABLE legislature_sessions(
    LegislatureSessionId VARCHAR PRIMARY KEY,
    LegislatureNumber SMALLINT NOT NULL REFERENCES legislatures(LegislatureNumber),
    LegislatureSessionCode TINYINT,
    LegislatureSessionTitle VARCHAR,
    LegislatureSessionStartDate DATE,
    LegislatureSessionEndDate DATE
);

CREATE TABLE people(
    PersonId VARCHAR PRIMARY KEY CHECK (PersonId LIKE '%:%'),
    FullName VARCHAR NOT NULL,
    FirstName VARCHAR NOT NULL,
    LastName VARCHAR NOT NULL,
    MiddleName VARCHAR,
    NickName VARCHAR,
    Suffix VARCHAR
);

CREATE TABLE members(
    MemberId VARCHAR PRIMARY KEY CHECK (MemberId = CONCAT(LegislatureNumber, ':', Chamber, ':', District, ':', PersonId)),
    LegislatureNumber SMALLINT NOT NULL REFERENCES legislatures(LegislatureNumber),
    PersonId VARCHAR NOT NULL REFERENCES people(PersonId),
    MemberCode VARCHAR,
    Chamber VARCHAR CHECK (Chamber IN ('H', 'S')),
    District VARCHAR,
    Party VARCHAR,
    IsMajority BOOLEAN,
    IsActive BOOLEAN,
    "Comment" VARCHAR,
    Phone VARCHAR,
    EMail VARCHAR,
    Building VARCHAR,
    Room VARCHAR
);

CREATE TABLE bills(
    BillId VARCHAR PRIMARY KEY CHECK (BillId = CONCAT(LegislatureNumber, ':', BillNumber)),
    LegislatureNumber SMALLINT NOT NULL REFERENCES legislatures(LegislatureNumber),
    BillNumber VARCHAR NOT NULL,
    BillName VARCHAR,
    Documents JSON [],
    PartialVeto BOOLEAN,
    Vetoed BOOLEAN,
    ShortTitle VARCHAR,
    StatusCode VARCHAR,
    StatusText VARCHAR,
    Flag1 VARCHAR,
    Flag2 UTINYINT,
    StatusDate DATE,
    StatusAndThen JSON [],
    StatusSummaryCode VARCHAR,
    OnFloor VARCHAR,
    Filler VARCHAR,
    "Lock" VARCHAR,
    AllMeetings JSON [],
    Meetings JSON [],
    Subjects VARCHAR [],
    ManifestErrors JSON [],
    Statutes JSON [],
    CurrentCommittee STRUCT(
        Chamber VARCHAR,
        Code VARCHAR,
        Catagory VARCHAR,
        "Name" VARCHAR,
        MeetingDays VARCHAR,
        "Location" VARCHAR,
        StartTime VARCHAR,
        EndTime VARCHAR,
        Email VARCHAR
    )
);

CREATE TABLE bill_versions(
    BillVersionId VARCHAR PRIMARY KEY,
    BillId VARCHAR NOT NULL REFERENCES bills(BillId),
    BillVersionLetter VARCHAR NOT NULL CHECK (BillVersionLetter IN ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z')),
    BillVersionTitle VARCHAR NOT NULL,
    BillVersionName VARCHAR NOT NULL,
    BillVersionIntroDate DATE NOT NULL,
    BillVersionPassedHouse DATE,
    BillVersionPassedSenate DATE,
    BillVersionWorkOrder VARCHAR NOT NULL,
    BillVersionPdfUrl VARCHAR NOT NULL,
    BillVersionFullText VARCHAR,
);

CREATE TABLE votes(
    VoteId VARCHAR PRIMARY KEY,
    LegislatureNumber SMALLINT NOT NULL REFERENCES legislatures(LegislatureNumber),
    VoteChamber VARCHAR NOT NULL CHECK (VoteChamber IN ('H', 'S')),
    VoteNumber USMALLINT NOT NULL,
    VoteDate DATE,
    VoteTitle VARCHAR NOT NULL,
    BillId VARCHAR REFERENCES bills(BillId),
    VoteBillAmendmentNumber DECIMAL(9, 1)
);

CREATE TABLE choices(
    ChoiceId VARCHAR PRIMARY KEY,
    VoteId VARCHAR NOT NULL REFERENCES votes(VoteId),
    MemberId VARCHAR NOT NULL REFERENCES members(MemberId),
    Choice VARCHAR NOT NULL CHECK (Choice IN ('Y', 'N', 'A', 'E'))
);
COMMIT;
"""


def get_db_structure(backend: SQLBackend) -> dict[str, ibis.Schema]:
    """Get the structure of the database from the SQL DDL."""
    return {
        table_name: backend.table(table_name).schema()
        for table_name in backend.list_tables()
    }


def _assert_structure_matches(backend: SQLBackend, sql: str = DDL):
    tmp = ibis.duckdb.connect(":memory:")
    tmp.con.execute(sql)
    expected_structure = get_db_structure(tmp)
    actual_structure = get_db_structure(backend)
    missing_tables = set(expected_structure) - set(actual_structure)
    extra_tables = set(actual_structure) - set(expected_structure)
    mismatches = {
        table_name: (expected_structure[table_name], actual_structure[table_name])
        for table_name in expected_structure
        if table_name in actual_structure
        and expected_structure[table_name] != actual_structure[table_name]
    }
    if missing_tables or extra_tables or mismatches:
        raise AssertionError(
            f"Missing tables: {missing_tables}\n"
            f"Extra tables: {extra_tables}\n"
            f"Mismatches: {mismatches}"
        )


def get_db(
    url: str | Backend | None = None,
) -> Backend:
    """Get a database connection."""
    if isinstance(url, Backend):
        return url
    if url is None:
        dotenv.load_dotenv()
        url = os.environ.get("DATABASE_URL")
    backend: DuckDBBackend = ibis.duckdb.connect()
    attach_postgres(backend, url, name="postgres", schema="vote_tracker")
    backend.raw_sql("USE postgres")
    # backend.raw_sql("SET search_path TO vote_tracker;")
    return Backend(backend, check_structure=False)


def attach_postgres(
    ddb_backend: DuckDBBackend,
    url: str,
    *,
    name: str | None = None,
    skip_if_exists: bool = False,
    schema: str | None = None,
) -> str:
    """Attach a PostgreSQL instance to a duckdb connection.

    This is useful for importing tables from PostgreSQL into duckdb.

    Parameters
    ----------
    ddb_backend:
        The duckdb backend to attach to.
    url:
        The connection string to the PostgreSQL instance.
    name:
        The name to attach as (the catalog name).
        If not provided, a unique one will be generated.
    skip_if_exists:
        Whether to skip the attachment if it already exists.
    schema:
        The schema to attach to in the PostgreSQL instance.

    Returns
    -------
    str
        The name of the attached catalog.
    """
    spec = urlparse(url)
    if name is None:
        name = ibis.util.gen_name(spec.hostname)
    ine = "IF NOT EXISTS" if skip_if_exists else ""
    options = ["TYPE postgres"]
    if schema is not None:
        options.append(f"SCHEMA '{schema}'")
    options_str = ", ".join(options)
    ddb_backend.raw_sql(f"""ATTACH {ine} '{url}' AS "{name}" ({options_str});""")
    return name
