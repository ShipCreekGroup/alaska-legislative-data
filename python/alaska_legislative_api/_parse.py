import dataclasses
import datetime
import sys
from pathlib import Path

import ibis
from ibis.expr import types as ir


@dataclasses.dataclass
class ParsedTables:
    members: ibis.Table
    bills: ibis.Table
    votes: ibis.Table
    sessions: ibis.Table

    def __repr__(self):
        return f"<ParsedTables members={self.members.count().execute()}, bills={self.bills.count().execute()}, votes={self.votes.count().execute()}, sessions={self.sessions.count().execute()}>"

    @classmethod
    def from_parquets(cls, directory: str | Path) -> "ParsedTables":
        directory = Path(directory)
        members = ibis.read_parquet(directory / "members.parquet")
        bills = ibis.read_parquet(directory / "bills.parquet")
        votes = ibis.read_parquet(directory / "votes.parquet")
        sessions = ibis.read_parquet(directory / "sessions.parquet")
        return cls(members, bills, votes, sessions)

    def to_parquets(self, directory: str | Path):
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        self.members.to_parquet(directory / "members.parquet")
        self.bills.to_parquet(directory / "bills.parquet")
        self.votes.to_parquet(directory / "votes.parquet")
        self.sessions.to_parquet(directory / "sessions.parquet")


def parse_scraping(directory: str | Path) -> ParsedTables:
    """Given a dir of raw json files from the API, parse them into ibis tables."""
    directory = Path(directory)
    members = ibis.read_json(directory / "members/*.json")
    bills = ibis.read_json(directory / "bills/*.json")
    votes = ibis.read_json(directory / "votes/*.json")
    sessions = ibis.read_json(directory / "sessions.json")

    members = clean_members(members)
    bills = clean_bills(bills)
    votes = clean_votes(votes)
    sessions = clean_sessions(sessions)

    return ParsedTables(members, bills, votes, sessions)


def clean_members(t: ibis.Table) -> ibis.Table:
    t = _fix_strings(t)
    schema = {
        "Session": "int16",
        "Code": "string",
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
    return t


def clean_votes(t: ibis.Table) -> ibis.Table:
    t = _fix_strings(t)
    t = t.mutate(VoteDate=t.VoteDate.nullif("-199- 0--0"))
    schema = {
        "Session": "int16",
        "VoteNum": "string",
        "Member": "string",
        "VoteDate": "date",
        "Vote": "string",
        "Bill": "string",
        # "MemberParty": "string",  # This is redundant, can be derived from Member
        # "MemberChamber": "string",  # This is redundant, can be derived from Member
        # "MemberName": "string",  # This is redundant, can be derived from Member
        "Title": "string",
    }
    t = t.select(schema.keys())
    t = t.cast(schema)
    return t


def clean_bills(t: ibis.Table) -> ibis.Table:
    t = _fix_strings(t)
    t = t.mutate(StatusDate=_parse_StatusDate(t.StatusDate))
    schema = {
        "Session": "int16",
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
    return t


def clean_sessions(t: ibis.Table) -> ibis.Table:
    t = _fix_strings(t)
    t = t.rename(Session="Number")

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
        "Session": "int16",
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
    return t


def _fix_strings(t: ibis.Table) -> ibis.Table:
    """Strip whitespace from all string columns in the table."""
    string_cols = [c for c in t.schema() if t.schema()[c].is_string()]
    return t.mutate(
        {c: t[c].strip().replace("\r\n", "\n").nullif("") for c in string_cols}
    )


def _parse_StatusDate(s: ir.StringValue) -> ir.DateValue:
    # For most sessions is a well formed YYYY-MM-DD string.
    # But for sessions 12, 13,15, 16, 17, it is eg "Jul -10- 1".
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
    parse_scraping(sys.argv[1]).to_parquets(sys.argv[2])
