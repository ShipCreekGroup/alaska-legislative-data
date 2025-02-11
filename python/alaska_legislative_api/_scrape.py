import json
from collections.abc import Iterable
from pathlib import Path
from typing import Literal, TypeAlias, TypedDict

from alaska_legislative_api import _low

Cache: TypeAlias = bool | Literal["previous-sessions"]


def scrape(directory: str | Path, *, cache: Cache = "previous-sessions") -> None:
    """
    Scrape the Alaska Legislative API data and save it to the specified directory.

    Args:
        directory (str): The directory where the scraped data will be saved.
    """
    d = Path(directory)
    sessions_path = d / "sessions.json"
    sessions = scrape_sessions(sessions_path, cache)

    session_numbers = [int(s["Number"]) for s in sessions]

    members_path = d / "members"
    members = scrape_members(session_numbers, members_path, cache)

    bills_path = d / "bills"
    scrape_bills(session_numbers, bills_path, cache)

    votes_path = d / "votes"
    scrape_votes(members, votes_path, cache)


def scrape_sessions(p: Path, cache: Cache = True) -> list[dict]:
    if not cache or not p.exists():
        sessions = do_scrape_sessions()
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(_low.json.dumps(sessions, indent=4))
    else:
        sessions = json.loads(p.read_text())
    return sessions


def do_scrape_sessions() -> list[dict]:
    """
    Get the list of available sessions.

    Returns:
        list[int]: A list of available session numbers.
    """
    session_number = 0
    entered_range = False
    sessions = []
    while True:
        session_number += 1
        try:
            s = _low.session(
                queries=[
                    # "laws",
                    # "journals",  # this makes each response too large
                    # "sponsors",
                    "locations",
                    "subjects",
                    "requestors",
                    "stats",
                    "housestatus",
                    "senatestatus",
                ],
                session=session_number,
            )
        except _low.DataUnimplementedError:
            if not entered_range:
                continue
            else:
                break
        except ValueError as e:
            if "Invalid Session Number" in str(e):
                break
        s = {**s, "Number": int(s["Number"])}
        sessions.append(s)
        entered_range = True
    return sessions


def scrape_members(sessions: list[int], d: Path, cache) -> list[dict]:
    result = []
    for session_number in sessions:
        p = d / f"members_{session_number}.json"
        if _need_refresh(session_number, sessions, cache, p):
            try:
                members = scrape_members_of_session(session_number)
            except _low.DataUnimplementedError:
                # TODO: do something so we don't continually try re-scraping
                continue
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(_low.json.dumps(members, indent=4))
        else:
            members = json.loads(p.read_text())
        result.extend(members)
    return result


def scrape_members_of_session(session_number: int) -> list[dict]:
    m = _low.members(session=session_number)
    return [{**member, "Session": session_number} for member in m]


def scrape_bills(sessions: list[int], d: Path, cache) -> list[dict]:
    result = []
    for session_number in sessions:
        p = d / f"bills_{session_number}.json"
        if _need_refresh(session_number, sessions, cache, p):
            try:
                bills = scrape_bills_of_session(session_number)
            except _low.DataUnimplementedError:
                # TODO: do something so we don't continually try re-scraping
                continue
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(_low.json.dumps(bills, indent=4))
        else:
            bills = json.loads(p.read_text())
        result.extend(bills)
    return result


def scrape_bills_of_session(session_number: int) -> list[dict]:
    b = _low.bills(
        queries=[
            "Subjects",
            # "Actions",  # this makes each response too large
        ],
        session=session_number,
    )
    return [{**bill, "Session": session_number} for bill in b]


# "Code": "ADE",
# "UID": 0,
# "LastName": "Anderson",
# "MiddleName": "",
# "FirstName": "Tom",
# "FormalName": "Representative Tom Anderson",
# "ShortName": "Anderson       ",
# "SessionContact": {
#     "tollFree": null,
#     "Street": null,
#     "Room": null,
#     "City": null,
#     "State": null,
#     "Zip": null,
#     "Phone": null,
#     "Fax": null,
#     "POBox": null
# },
# "InterimContact": {
#     "tollFree": null,
#     "Street": null,
#     "Room": null,
#     "City": null,
#     "State": null,
#     "Zip": null,
#     "Phone": null,
#     "Fax": null,
#     "POBox": null
# },
# "Chamber": "H",
# "District": "19",
# "Seat": " ",
# "Party": "R",
# "Phone": "4654939",
# "EMail": "Representative.Tom.Anderson@akleg.gov",
# "Building": "CAPITOL",
# "Room": "432",
# "Comment": "",
# "IsActive": true,
# "IsMajority": true,
# "Session": 23
class Member(TypedDict):
    Session: int
    Code: str
    # UID: int
    LastName: str
    MiddleName: str
    FirstName: str
    FormalName: str
    ShortName: str
    # SessionContact: dict
    # InterimContact: dict
    Chamber: Literal["H", "S"]
    District: str
    Seat: str
    Party: str
    Phone: str
    EMail: str
    Building: str
    Room: str
    Comment: str
    IsActive: bool
    IsMajority: bool


def scrape_votes(members: list[Member], d: Path, cache) -> list[dict]:
    result = []
    sessions = {m["Session"] for m in members}
    for m in members:
        session = m["Session"]
        code = m["Code"]
        p = d / f"votes_{session}_{code}.json"
        if _need_refresh(session, sessions, cache, p):
            try:
                votes = scrape_votes_of(session, code)
            except _low.DataUnimplementedError:
                # TODO: do something so we don't continually try re-scraping
                continue
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(_low.json.dumps(votes, indent=4))
        else:
            votes = json.loads(p.read_text())
        result.extend(votes)
    return result


def scrape_votes_of(
    session_number: int, member_code: str
) -> tuple[list[dict], list[dict]]:
    mems = _low.members(
        session=session_number,
        queries=[
            f"members;code={member_code}",
            "Votes",
            "Bills",
        ],
    )
    votes = []
    for m in mems:
        votes.extend(m["Votes"])
    votes = [{**v, "Session": session_number} for v in votes]
    return votes


def _is_latest(session, sessions) -> bool:
    return int(session) == max(int(s) for s in sessions)


def _need_refresh(session: int, sessions: Iterable[int], cache: Cache, path: Path):
    if not cache:
        return True
    if not path.exists():
        return True
    if cache is True:
        return False
    assert cache == "previous-sessions"
    return _is_latest(session, sessions)
