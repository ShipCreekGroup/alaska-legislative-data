from __future__ import annotations

from pathlib import Path
from typing import Annotated, get_type_hints

import ibis
from ibis import ir
from ibis.backends.duckdb import Backend as DuckdbBackend

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
    LegislatureStartYear: Annotated[ir.IntegerColumn, "int16"]
    """eg 2021 for the 31st legislature."""
    LegislatureEndYear: Annotated[ir.IntegerColumn, "int16"]
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
    """Unique identifier for this membership. Of the form '{LegislatureNumber}:{PersonId}'."""
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


class VoteSchema(TableSchema):
    VoteId: Annotated[ir.StringColumn, "!string"]
    LegislatureNumber: Annotated[ir.IntegerColumn, LEGISLATURE_NUMBER_TYPE]
    VoteNum: Annotated[ir.StringColumn, "!string"]
    """eg 'HR  132'"""
    VoteDate: Annotated[ir.StringColumn, "!date"]
    VoteTitle: Annotated[ir.StringColumn, "!string"]
    BillId: Annotated[ir.StringColumn, "string"]
    """Reference to the Bill table. Might be NULL, because some votes are not on bills.
    
    Eg the vote to confirm a judge."""


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


class Backend(DuckdbBackend):
    def __init__(self, db: DuckdbBackend | str | Path):
        if isinstance(db, Backend):
            db = db._db
        if not isinstance(db, DuckdbBackend):
            db = ibis.duckdb.connect(db)
        if not isinstance(db, DuckdbBackend):
            raise TypeError(f"Expected DuckdbBackend, got {type(db)}")
        if isinstance(db, Backend):
            raise TypeError("Backend should not be nested")
        self.create_tables(db)
        self._db = db

    def __getattr__(self, name):
        return getattr(self._db, name)

    @property
    def Legislature(self) -> LegislatureTable:
        """Table of legislatures. Each person may have multiple `LegislatureSessions`"""
        return self.table("legislatures")

    @property
    def LegislatureSession(self) -> LegislatureSessionTable:
        """Table of `LegislativeSession`s."""
        return self.table("legislature_sessions")

    @property
    def Person(self) -> PersonTable:
        """Table of people. Each person may have multiple `Member`s."""
        return self.table("people")

    @property
    def Member(self) -> MemberTable:
        """Table of memberships (combo of `Person` and legislature)."""
        return self.table("members")

    @property
    def Bill(self) -> BillTable:
        """Table of bills."""
        return self.table("bills")

    @property
    def Vote(self) -> VoteTable:
        """Table of roll call votes (where each member's choice is recorded)."""
        return self.table("votes")

    @property
    def Choice(self) -> ChoiceTable:
        """Table of choices on `Vote`s by `Member`s."""
        return self.table("choices")

    # This is needed so that when you do `db.table("foo")`, the resulting table
    # thinks it's backend is self._db, not self.
    def table(self, *args, **kwargs):
        return self._db.table(*args, **kwargs)

    @classmethod
    def create_tables(cls, db: DuckdbBackend) -> None:
        if "legislatures" not in db.tables:
            db.create_table("legislatures", schema=LegislatureSchema.ibis_schema())
        if "legislature_sessions" not in db.tables:
            db.create_table(
                "legislature_sessions", schema=LegislatureSessionSchema.ibis_schema()
            )
        if "people" not in db.tables:
            db.create_table("people", schema=PersonSchema.ibis_schema())
        if "members" not in db.tables:
            db.create_table("members", schema=MemberSchema.ibis_schema())
        if "bills" not in db.tables:
            db.create_table("bills", schema=BillSchema.ibis_schema())
        if "votes" not in db.tables:
            db.create_table("votes", schema=VoteSchema.ibis_schema())
        if "choices" not in db.tables:
            db.create_table("choices", schema=ChoiceSchema.ibis_schema())
