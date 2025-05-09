"""Tools to updating our hand-curated tables of people and members.

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

import logging
from pathlib import Path

import ibis

from alaska_legislative_data import _scrape

_HERE = Path(__file__).parent

logger = logging.getLogger(__name__)


def download_members_from_api(
    dest: str | Path | None = _HERE / "members.csv.raw",
) -> ibis.Table:
    member_dicts = _scrape.scrape_members()
    members = ibis.memtable(member_dicts)
    logger.info(f"Downloaded {members.count().execute()} members")
    members = members.order_by("LegislatureNumber", "Chamber", "District", "PersonId")
    if dest is not None:
        logger.info(f"Saving mambers to {dest}")
        members.to_csv(dest)
    return members


def read_members(
    path: str | Path = _HERE / "members.csv", *, backend: ibis.BaseBackend = ibis
) -> ibis.Table:
    logger.info(f"Reading members from {path}")
    members = backend.read_csv(path)
    members = members.mutate(
        # see _db.py for the methodology
        MemberId=members.LegislatureNumber.cast(str)
        + ":"
        + members.MemberChamber
        + ":"
        + members.MemberDistrict.fill_null("")
        + ":"
        + members.PersonId
    ).relocate("MemberId")
    return members


def read_people(
    path: str | Path = _HERE / "people.csv", *, backend: ibis.BaseBackend = ibis
) -> ibis.Table:
    logger.info(f"Reading people from {path}")
    return backend.read_csv(path)
