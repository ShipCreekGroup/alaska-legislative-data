import datetime

import ibis
from ibis import _
from ibis.expr import types as ir

from alaska_legislative_data import _db


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
    t = t.mutate(
        VoteDate=t.VoteDate.nullif("-199- 0--0"),
        VoteBillAmendmentNumber=_parse_amendment_numbers(t.Title),
    )
    t = t.rename(
        BillNumber="Bill",
        MemberCode="Member",
        Choice="Vote",
        VoteTitle="Title",
    )
    schema = {
        "LegislatureNumber": "int16",
        "VoteNum": "string",
        "VoteDate": "date",
        "VoteTitle": "string",
        "BillNumber": "string",
        "VoteBillAmendmentNumber": "decimal(9, 1)",
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


def clean_and_split_legislatures_into_sessions(
    t: ibis.Table,
) -> tuple[_db.LegislatureTable, _db.LegislatureSessionTable]:
    t = _fix_strings(t)

    @ibis.udf.scalar.python(signature=(("string",), "date"))
    def parse_date(s: str) -> datetime.date:
        # "/Date(726742800000)/" to date
        n_millis = int(s[6:-2])
        return datetime.date.fromtimestamp(n_millis / 1000)

    def expand_session(sd: ir.StructValue) -> dict:
        return {
            "LegislatureSessionCode": sd.ID,
            "LegislatureSessionTitle": sd.Title,
            "LegislatureSessionStartDate": parse_date(sd.StartDate),
            "LegislatureSessionEndDate": parse_date(sd.EndDate),
        }

    sessions = t.select(
        LegislatureNumber=_.LegislatureNumber,
        **expand_session(t.SessionDates.unnest()),
        # SessionDates=t.SessionDates.map(expand_session),
    )
    sessions = sessions.mutate(
        LegislatureSessionId=_.LegislatureNumber.cast(str)
        + ":"
        + _.LegislatureSessionCode.cast(str)
    )
    leg = t.select(
        LegislatureNumber=_.LegislatureNumber,
        LegislatureStartYear=_.Year.cast("int16"),
        LegislatureEndYear=_.Year.cast("int16") + 1,
    )
    sessions = sessions.select(_db.LegislatureSessionSchema.ibis_schema().keys()).cast(
        _db.LegislatureSessionSchema.ibis_schema()
    )
    leg = leg.select(_db.LegislatureSchema.ibis_schema().keys()).cast(
        _db.LegislatureSchema.ibis_schema()
    )
    leg = leg.order_by(_.LegislatureNumber)
    sessions = sessions.order_by(_.LegislatureSessionStartDate)
    return leg, sessions


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


def _parse_amendment_numbers(vote_title: ibis.ir.StringValue) -> ibis.ir.DecimalValue:
    # returning as a decimal means that sorting works, and we can split
    # on "." to get the root and sub-amendment numbers.
    # IDK, maybe we want to switch keeping the fields separate,
    # but then that requires piping multiple fields through all downstream processing.
    root = vote_title.re_extract(r"Amendment No. (\d+)$", 1).nullif("")
    sub = vote_title.re_extract(r"Amendment No. (\d+) to Amendment No. \d+$", 1).nullif(
        ""
    )
    implicit_sub1 = vote_title.re_search(r"Amendment to Amendment No. \d+").ifelse(
        "1", "0"
    )
    sub = sub.fill_null(implicit_sub1)
    return (root + "." + sub).cast("decimal(9, 1)")


def _test_parse_amendment_numbers(inp, exp):
    res = _parse_amendment_numbers(ibis.literal(inp, type="string")).execute()
    # convert nan to None
    if res != res:
        res = None
    if res is not None:
        res = float(res)
    assert res == exp, (exp, res)


_test_parse_amendment_numbers("foo", None)
_test_parse_amendment_numbers("Amendment No. 1", 1.0)
_test_parse_amendment_numbers("Amendment No. 2", 2.0)
_test_parse_amendment_numbers("Amendment No. 1 to Amendment No. 3", 3.1)
_test_parse_amendment_numbers("Amendment No. 2 to Amendment No. 3", 3.2)
_test_parse_amendment_numbers("Amendment to Amendment No. 3", 3.1)
