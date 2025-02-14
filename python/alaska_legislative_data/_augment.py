from __future__ import annotations

import dataclasses
import logging
import sys
from pathlib import Path

import ibis
from ibis import _

from alaska_legislative_data._parse import ParsedTables

logger = logging.getLogger(__name__)

# note the /pub ending and the format=csv query param
_URL_PEOPLE = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRBkr9cSna3m4_64VgdGN3PIP9BgFw4wLi3k0dQn5peGY-I3kqAPY8r77xHKl-KHm0rTuJVMy3I8Qml/pub?output=csv&gid=925126040&cachebuster=0"
_URL_MEMBERS_1_TO_9 = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRBkr9cSna3m4_64VgdGN3PIP9BgFw4wLi3k0dQn5peGY-I3kqAPY8r77xHKl-KHm0rTuJVMy3I8Qml/pub?output=csv&gid=49484443&cachebuster=0"
_URL_MEMBERS_10_PLUS = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRBkr9cSna3m4_64VgdGN3PIP9BgFw4wLi3k0dQn5peGY-I3kqAPY8r77xHKl-KHm0rTuJVMy3I8Qml/pub?output=csv&gid=0&cachebuster=0"


@dataclasses.dataclass
class CuratedData:
    members_1_to_9: ibis.Table
    members_10_plus: ibis.Table
    people: ibis.Table

    @classmethod
    def from_gsheets(cls) -> CuratedData:
        logger.info("Reading curated data from Google Sheets")
        return cls(
            members_1_to_9=_read_gsheet(_URL_MEMBERS_1_TO_9),
            members_10_plus=_read_gsheet(_URL_MEMBERS_10_PLUS),
            people=_read_gsheet(_URL_PEOPLE),
        )


@dataclasses.dataclass(frozen=True, kw_only=True)
class AugmentedTables:
    legislatures: ibis.Table
    people: ibis.Table
    bills: ibis.Table
    votes: ibis.Table
    members: ibis.Table
    choices: ibis.Table

    def __repr__(self):
        return f"<AugmentedTables members={self.members.count().execute()}, bills={self.bills.count().execute()}, votes={self.votes.count().execute()}, legislatures={self.legislatures.count().execute()}, people={self.people.count().execute()}, choices={self.choices.count().execute()}>"

    def to_parquets(self, directory: str | Path):
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        self.members.to_parquet(directory / "members.parquet")
        self.bills.to_parquet(directory / "bills.parquet")
        self.votes.to_parquet(directory / "votes.parquet")
        self.choices.to_parquet(directory / "choices.parquet")
        self.legislatures.to_parquet(directory / "legislatures.parquet")
        self.people.to_parquet(directory / "people.parquet")

    @classmethod
    def from_parquets(cls, directory: str | Path) -> AugmentedTables:
        directory = Path(directory)
        members = ibis.read_parquet(directory / "members.parquet")
        bills = ibis.read_parquet(directory / "bills.parquet")
        votes = ibis.read_parquet(directory / "votes.parquet")
        choices = ibis.read_parquet(directory / "choices.parquet")
        legislatures = ibis.read_parquet(directory / "legislatures.parquet")
        people = ibis.read_parquet(directory / "people.parquet")
        return cls(
            members=members,
            bills=bills,
            votes=votes,
            choices=choices,
            legislatures=legislatures,
            people=people,
        )


def augment_parsed(
    parsed: ParsedTables, curated: CuratedData | None = None
) -> AugmentedTables:
    """Add in our hand-curated data to the parsed tables.

    The curated data lives at
    https://docs.google.com/spreadsheets/d/1kErTlfIW_5F5MmlvBohTPtpPW0uXoNeq_neXrVHwhE8/edit?gid=0#gid=0

    The raw data has two problems:

    ## The table of members is incomplete, it only goes back to the 10th legislature.

    This is pretty simple, we have a hand-curated table of members for the 1st to 9th legislatures.
    So we just union the two tables together.

    ## It is impossible to track an individual member across legislatures.

    ie (this is made up) Mike Miller has the code MIL in the 33rd legislature,
    but in the 32nd legislature he has the code MIM.
    So a person's code is not stable between legislatures.
    In addition, a person's name is not adequate for identifying a person.
    Eg there might be two people with the last name "Smith" in the 32nd legislature.
    There are also two people with the name Mike Miller throughout the history
    of the Alaska Legislature. So even using full name is inadequate.
    Also, people's name can change, eg "Al" -> "Albert".

    So, the only way I could think of solving this is with a handwritten
    lookup table from (Session, Code) to PersonId.
    """
    if curated is None:
        curated = CuratedData.from_gsheets()
    members = _augment_members(parsed.members, curated)
    bills = _augment_bills(parsed.bills)
    votes, choices = split_choices(
        choices_raw=parsed.choices, bills=bills, members=members
    )
    return AugmentedTables(
        members=members,
        bills=bills,
        votes=votes,
        choices=choices,
        legislatures=parsed.legislatures,
        people=curated.people,
    )


def _augment_members(
    members: ibis.Table, curated: CuratedData | None = None
) -> ibis.Table:
    if curated is None:
        curated = CuratedData.from_gsheets()
    logger.info("Augmenting parsed tables with info from Google Sheets")
    members_1_to_9 = curated.members_1_to_9
    members_10_plus = curated.members_10_plus
    logger.info("Done reading from from Google Sheets")
    assert (
        (members.group_by("LegislatureNumber", "MemberCode").agg(n=_.count()).n == 1)
        .all()
        .execute()
    )

    members_with_person_id = members.left_join(
        members_10_plus.select("LegislatureNumber", "MemberCode", "PersonId"),
        ["LegislatureNumber", "MemberCode"],
    ).drop("LegislatureNumber_right", "MemberCode_right")
    members_1_to_9 = members_1_to_9.select(members_with_person_id.columns).cast(
        members_with_person_id.schema()
    )
    members_1_to_9 = members_1_to_9.anti_join(
        members_with_person_id, ("LegislatureNumber", "PersonId")
    )  # remove anyone from the 1-9 table who is in the 10+ table
    all_members = ibis.union(members_with_person_id, members_1_to_9)
    all_members = all_members.mutate(
        MemberId=_.LegislatureNumber.cast(str) + ":" + _.Chamber + ":" + _.PersonId,
    )
    all_members = all_members.relocate("MemberId", "PersonId")
    all_members = all_members.cache()
    assert (all_members.group_by("MemberId").agg(n=_.count()).n == 1).all().execute()
    return all_members


def _augment_bills(bills: ibis.Table) -> ibis.Table:
    assert bills.LegislatureNumber.notnull().all().execute()
    assert bills.BillNumber.notnull().all().execute()
    bills = (
        bills.mutate(
            # using session number and personId is inadequate, as there are some
            # instances of a person being in both the Senate and House in the same
            # session
            BillId=_.LegislatureNumber.cast(str) + ":" + _.BillNumber,
        )
        .relocate("BillId")
        .order_by("BillId")
    )
    assert bills.BillId.value_counts(name="n").n.max().execute() == 1
    return bills


def _add_bill_id(choices: ibis.Table, bills: ibis.Table) -> ibis.Table:
    bill_id_lookup = bills.select("LegislatureNumber", "BillNumber", "BillId")
    assert (
        (
            bill_id_lookup.group_by("LegislatureNumber", "BillNumber")
            .agg(n=_.count())
            .n
            == 1
        )
        .all()
        .execute()
    )
    missing = choices.filter(
        _.LegislatureNumber.notnull(),
        _.BillNumber.notnull(),
        _.LegislatureNumber.notin(bill_id_lookup.LegislatureNumber),
        _.BillNumber.notin(bill_id_lookup.BillNumber),
    )
    assert missing.count().execute() == 0
    return choices.left_join(bill_id_lookup, ["LegislatureNumber", "BillNumber"]).drop(
        "LegislatureNumber_right", "BillNumber_right"
    )


def _add_member_id(choices: ibis.Table, members: ibis.Table) -> ibis.Table:
    member_id_lookup = members.select("LegislatureNumber", "MemberCode", "MemberId")
    member_id_lookup = member_id_lookup.filter(
        _.LegislatureNumber.notnull(),
        _.MemberCode.notnull(),
    )
    assert (
        (
            member_id_lookup.group_by("LegislatureNumber", "MemberCode")
            .agg(n=_.count())
            .n
            == 1
        )
        .all()
        .execute()
    )
    # check that every (LegislatureNumber, MemberCode) combo is votes
    # is also present in bill_id_lookup
    missing = choices.filter(
        _.LegislatureNumber.notnull(),
        _.MemberCode.notnull(),
        _.LegislatureNumber.notin(member_id_lookup.LegislatureNumber),
        _.MemberCode.notin(member_id_lookup.MemberCode),
    )
    assert missing.count().execute() == 0
    return choices.left_join(
        member_id_lookup, ["LegislatureNumber", "MemberCode"]
    ).drop("LegislatureNumber_right", "MemberCode_right")


def split_choices(
    *, choices_raw: ibis.Table, bills: ibis.Table, members: ibis.Table
) -> tuple[ibis.Table, ibis.Table]:
    choices_raw = _add_bill_id(choices_raw, bills)
    choices_raw = _add_member_id(choices_raw, members)

    choices_raw = choices_raw.mutate(
        VoteId=_.LegislatureNumber.cast(str) + ":" + _.VoteNum
    )
    choices_raw = choices_raw.mutate(
        ChoiceId=_.VoteId + ":" + _.MemberId.re_replace(r"^\d+:", "")
    )

    votes = (
        choices_raw.select(
            "VoteId",
            "LegislatureNumber",
            "VoteNum",
            "VoteDate",
            "VoteTitle",
            "BillId",
        )
        .distinct()
        .order_by("VoteId")
    )
    choices = (
        choices_raw.select(
            "ChoiceId",
            "VoteId",
            "MemberId",
            "Choice",
        )
        .distinct()
        .order_by("ChoiceId")
    )
    return votes, choices


def _read_gsheet(url):
    logger.info(f"Reading {url}")
    return ibis.read_csv(url, delim=",").cache()


if __name__ == "__main__":
    augment_parsed(ParsedTables.from_parquets(sys.argv[1])).to_parquets(sys.argv[2])
