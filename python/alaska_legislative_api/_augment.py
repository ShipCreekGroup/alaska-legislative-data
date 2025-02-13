import dataclasses
import sys
from pathlib import Path

import ibis

from alaska_legislative_api._parse import ParsedTables

# note the /pub ending and the format=csv query param
_URL_PEOPLE = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRBkr9cSna3m4_64VgdGN3PIP9BgFw4wLi3k0dQn5peGY-I3kqAPY8r77xHKl-KHm0rTuJVMy3I8Qml/pub?single=true&output=csv&gid=925126040"
_URL_MEMBERS_1_TO_9 = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRBkr9cSna3m4_64VgdGN3PIP9BgFw4wLi3k0dQn5peGY-I3kqAPY8r77xHKl-KHm0rTuJVMy3I8Qml/pub?single=true&output=csv&gid=49484443"
_URL_MEMBERS_10_PLUS = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRBkr9cSna3m4_64VgdGN3PIP9BgFw4wLi3k0dQn5peGY-I3kqAPY8r77xHKl-KHm0rTuJVMy3I8Qml/pub?single=true&output=csv&gid=0"


@dataclasses.dataclass
class AugmentedTables:
    members: ibis.Table
    bills: ibis.Table
    votes: ibis.Table
    sessions: ibis.Table
    people: ibis.Table

    def __repr__(self):
        return f"<AugmentedTables members={self.members.count().execute()}, bills={self.bills.count().execute()}, votes={self.votes.count().execute()}, sessions={self.sessions.count().execute()}, people={self.people.count().execute()}>"

    def to_parquets(self, directory: str | Path):
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        self.members.to_parquet(directory / "members.parquet")
        self.bills.to_parquet(directory / "bills.parquet")
        self.votes.to_parquet(directory / "votes.parquet")
        self.sessions.to_parquet(directory / "sessions.parquet")
        self.people.to_parquet(directory / "people.parquet")

    @classmethod
    def from_parquets(cls, directory: str | Path) -> "AugmentedTables":
        directory = Path(directory)
        members = ibis.read_parquet(directory / "members.parquet")
        bills = ibis.read_parquet(directory / "bills.parquet")
        votes = ibis.read_parquet(directory / "votes.parquet")
        sessions = ibis.read_parquet(directory / "sessions.parquet")
        people = ibis.read_parquet(directory / "people.parquet")
        return cls(members, bills, votes, sessions, people)


def augment_parsed(parsed: ParsedTables) -> AugmentedTables:
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
    So a person's code is not stable between sessions.
    In addition, a person's name is not adequate for identifying a person.
    Eg there might be two people with the last name "Smith" in the 32nd legislature.
    There are also two people with the name Mike Miller throughout the history
    of the Alaska Legislature. So even using full name is inadequate.
    Also, people's name can change, eg "Al" -> "Albert".

    So, the only way I could think of solving this is with a handwritten
    lookup table from (Session, Code) to PersonID.
    """
    people = ibis.read_csv(_URL_PEOPLE).cache()
    members_1_to_9 = ibis.read_csv(_URL_MEMBERS_1_TO_9).cache()
    members_10_plus = ibis.read_csv(_URL_MEMBERS_10_PLUS).cache()
    members_with_person_id = parsed.members.left_join(
        members_10_plus.select("Session", "Code", "PersonID"),
        ["Session", "Code"],
    ).drop("Session_right", "Code_right")
    members_1_to_9 = members_1_to_9.select(members_with_person_id.columns).cast(
        members_with_person_id.schema()
    )
    all_members = ibis.union(members_with_person_id, members_1_to_9)
    all_members = all_members.relocate("PersonID")
    all_members = all_members.cache()
    return AugmentedTables(
        members=all_members,
        bills=parsed.bills,
        votes=parsed.votes,
        sessions=parsed.sessions,
        people=people,
    )


if __name__ == "__main__":
    augment_parsed(ParsedTables.from_parquets(sys.argv[1])).to_parquets(sys.argv[2])
