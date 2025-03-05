import dataclasses
import datetime
import sys
from pathlib import Path

import ibis
from ibis import _
from ibis.expr import types as ir


@dataclasses.dataclass
class ParsedTables:
    members: ibis.Table
    bills: ibis.Table
    choices: ibis.Table
    legislatures: ibis.Table

    def __repr__(self):
        return f"<ParsedTables members={self.members.count().execute()}, bills={self.bills.count().execute()}, choices={self.choices.count().execute()}, legislatures={self.legislatures.count().execute()}>"

    @classmethod
    def from_parquets(cls, directory: str | Path) -> "ParsedTables":
        directory = Path(directory)
        members = ibis.read_parquet(directory / "members.parquet")
        bills = ibis.read_parquet(directory / "bills.parquet")
        choices = ibis.read_parquet(directory / "choices.parquet")
        legislatures = ibis.read_parquet(directory / "legislatures.parquet")
        return cls(members, bills, choices, legislatures)

    def to_parquets(self, directory: str | Path):
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        self.members.to_parquet(directory / "members.parquet")
        self.bills.to_parquet(directory / "bills.parquet")
        self.choices.to_parquet(directory / "choices.parquet")
        self.legislatures.to_parquet(directory / "legislatures.parquet")


def parse_scraped(directory: str | Path) -> ParsedTables:
    """Given a dir of raw json files from the API, parse them into ibis tables."""
    directory = Path(directory)
    members = ibis.read_json(directory / "members/*.json")
    bills = ibis.read_json(directory / "bills/*.json")
    choices = ibis.read_json(directory / "choices/*.json")
    legislatures = ibis.read_json(directory / "legislatures.json")

    members = clean_members(members)
    bills = clean_bills(bills)
    choices = clean_choices(choices)
    legislatures = clean_legislatures(legislatures)

    return ParsedTables(members, bills, choices, legislatures)


def clean_members(t: ibis.Table) -> ibis.Table:
    t = _fix_strings(t)
    t = t.rename(
        MemberCode="Code",
    )
    schema = {
        "LegislatureNumber": "int16",
        "MemberCode": "string",
        # "UID": "int64",  # always 0
        "LastName": "string",
        "MiddleName": "string",
        "FirstName": "string",
        "FormalName": "string",
        "ShortName": "string",
        "SessionContact": "struct<tollFree: json, Street: json, Room: json, City: json, State: json, Zip: json, Phone: json, Fax: json, POBox: json>",
        "InterimContact": "struct<tollFree: json, Street: json, Room: json, City: json, State: json, Zip: json, Phone: json, Fax: json, POBox: json>",
        "Chamber": "string",
        "District": "string",
        "Seat": "string",
        "Party": "string",
        "Phone": "string",
        "EMail": "string",
        "Building": "string",
        "Room": "string",
        "Comment": "string",
        "IsActive": "boolean",
        "IsMajority": "boolean",
    }
    t = t.select(schema.keys())
    t = t.cast(schema)
    t = t.distinct()
    return t


def clean_choices(t: ibis.Table) -> ibis.Table:
    t = _fix_strings(t)
    t = t.mutate(VoteDate=t.VoteDate.nullif("-199- 0--0"))
    t = t.rename(
        BillNumber="Bill",
        MemberCode="Member",
        LegislatureNumber="Session",
        Choice="Vote",
        VoteTitle="Title",
    )
    schema = {
        "LegislatureNumber": "int16",
        "VoteNum": "string",
        "VoteDate": "date",
        "VoteTitle": "string",
        "BillNumber": "string",
        "MemberCode": "string",
        "Choice": "string",
        # "MemberParty": "string",  # This is redundant, can be derived from Member
        # "MemberChamber": "string",  # This is redundant, can be derived from Member
        # "MemberName": "string",  # This is redundant, can be derived from Member
    }
    t = t.select(schema.keys())
    t = t.cast(schema)
    t = t.distinct()
    return t


def clean_bills(t: ibis.Table) -> ibis.Table:
    t = _fix_strings(t)
    t = t.mutate(StatusDate=_parse_StatusDate(t.StatusDate))
    t = t.rename(
        LegislatureNumber="Session",
    )
    schema = {
        "LegislatureNumber": "int16",
        "BillNumber": "string",
        "BillName": "string",
        "Documents": "array<json>",
        "PartialVeto": "boolean",
        "Vetoed": "boolean",
        "ShortTitle": "string",
        "StatusCode": "string",
        "StatusText": "string",
        "Flag1": "string",  # "G", "H", "X", "S", or NULL
        "Flag2": "uint8",  # numbers 0 to 7
        "StatusDate": "date",
        "StatusAndThen": "array<json>",
        "StatusSummaryCode": "string",
        "OnFloor": "string",
        # "NotKnown": "string",  # This is always null
        "Filler": "string",
        "Lock": "string",  # "H", "S", "B", NULL, or "\x00" (guessing an encoding error meaning NULL)
        "AllMeetings": "array<json>",
        "Meetings": "array<json>",
        "Subjects": "array<string>",
        "ManifestErrors": "array<json>",
        "Statutes": "array<json>",
        "CurrentCommittee": "struct<Chamber: string, Code: string, Catagory: string, Name: string, MeetingDays: string, Location: string, StartTime: string, EndTime: string, Email: string>",
    }
    t = t.select(schema.keys())
    t = t.cast(schema)
    t = t.distinct()
    t = t.order_by("LegislatureNumber", "BillNumber")
    t = t.cache()
    assert t.LegislatureNumber.notnull().all().execute()
    assert t.BillNumber.notnull().all().execute()
    t = t.mutate(
        # using session number and personId is inadequate, as there are some
        # instances of a person being in both the Senate and House in the same
        # session
        BillId=_.LegislatureNumber.cast(str) + ":" + _.BillNumber,
    ).relocate("BillId")
    assert t.BillId.value_counts(name="n").n.max().execute() == 1
    return t


def clean_legislatures(t: ibis.Table) -> ibis.Table:
    t = _fix_strings(t)
    t = t.rename(LegislatureNumber="Number")

    @ibis.udf.scalar.python(signature=(("string",), "date"))
    def parse_date(s: str) -> datetime.date:
        # "/Date(726742800000)/" to date
        n_millis = int(s[6:-2])
        return datetime.date.fromtimestamp(n_millis / 1000)

    def fix_session_date(sd: ir.StructValue) -> ir.StructValue:
        return ibis.struct(
            {
                "ID": sd.ID,
                "Title": sd.Title,
                "StartDate": parse_date(sd.StartDate),
                "EndDate": parse_date(sd.EndDate),
            }
        )

    t = t.mutate(SessionDates=t.SessionDates.map(fix_session_date))
    schema = {
        "LegislatureNumber": "int16",
        "Year": "int16",
        "SessionDates": "array<struct<ID: int64, Title: string, StartDate: date, EndDate: date>>",
        "Journals": "array<json>",
        # "Laws": "json",         # The rest of these fields are always null,
        # "Locations": "json",    # I think because is the union of all the schemas for
        # "Subjects": "json",     # bills, meetings, etc
        # "Requestors": "json",   # So let's just ignore them for now.
        # "Sponsors": "json",
        # "Stats": "json",
        # "HouseStatus": "json",
        # "SenateStatus": "json",
    }
    t = t.select(schema.keys())
    t = t.cast(schema)
    t = t.distinct()
    return t


def _fix_strings(t: ibis.Table) -> ibis.Table:
    """Strip whitespace from all string columns in the table."""
    string_cols = [c for c in t.schema() if t.schema()[c].is_string()]
    return t.mutate(
        {c: t[c].strip().replace("\r\n", "\n").nullif("") for c in string_cols}
    )


def _parse_StatusDate(s: ir.StringValue) -> ir.DateValue:
    # For most legislatures is a well formed YYYY-MM-DD string.
    # But for legislatures 12, 13,15, 16, 17, it is eg "Jul -10- 1".
    # The "10" is a day of 1-31, and the " 1" is either " 1" (7900 times)
    # or " 2" (1 time). So IDK exactly what that represents,
    # possibly the session number?

    # The month codes are mostly in the beginning of the year, which makes sense:
    # ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━┓
    # ┃ StatusMonth ┃ StringSlice(StatusDate, 3) ┃ n     ┃
    # ┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━┩
    # │ int8        │ string                     │ int64 │
    # ├─────────────┼────────────────────────────┼───────┤
    # │           1 │ Jan                        │   912 │
    # │           2 │ Feb                        │  1268 │
    # │           3 │ Mar                        │  1244 │
    # │           4 │ Apr                        │  1459 │
    # │           5 │ May                        │  1618 │
    # │           6 │ Jun                        │  1095 │
    # │           7 │ Jul                        │   286 │
    # │           8 │ Aug                        │    16 │
    # │          12 │ Dec                        │     3 │
    # └─────────────┴────────────────────────────┴───────┘
    # For now, since for these "Jul -10- 1" values
    # 1. I don't know the year (could be either one of the 2 session years)
    # 2. I don't know what the " 1" or " 2" means
    # I'll just return NULL for these values.
    return s.try_cast("date")


if __name__ == "__main__":
    parse_scraped(sys.argv[1]).to_parquets(sys.argv[2])
