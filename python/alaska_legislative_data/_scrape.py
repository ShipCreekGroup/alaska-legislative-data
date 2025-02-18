import asyncio
import datetime
import json
import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Literal, TypeAlias, TypedDict

from alaska_legislative_data import _low

logger = logging.getLogger(__name__)

Cache: TypeAlias = bool | Literal["previous-sessions"]


def scrape(directory: str | Path, *, cache: Cache = "previous-sessions") -> None:
    """
    Scrape the Alaska Legislative API data and save it to the specified directory.

    Args:
        directory (str): The directory where the scraped data will be saved.
    """
    d = Path(directory)
    legs = scrape_legislatures(d / "legislatures.json")
    leg_numbers = [int(s["LegislatureNumber"]) for s in legs]
    members = scrape_members(
        legislature_numbers=leg_numbers, d=d / "members", cache=cache
    )
    scrape_bills(legislature_numbers=leg_numbers, d=d / "bills", cache=cache)
    scrape_votes(members=members, d=d / "votes", cache=cache)


def scrape_legislatures(
    p: Path | str,
    *,
    legislature_numbers: list[int] | None = None,
    cache: bool = False,
) -> list[dict]:
    p = Path(p)
    if cache and p.exists():
        return json.loads(p.read_text())
    if legislature_numbers is None:
        # +5 to be safe
        legislature_numbers = range(1, _estimate_max_leg_num() + 5)
    sessions = _do_scrape_legs(legislature_numbers)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(sessions, indent=4))
    return sessions


def _do_scrape_legs(leg_nums: list[int]) -> list[dict]:
    tasks = [_do_scrape_leg(n) for n in leg_nums]
    sessions = asyncio.run(asyncio.gather(*tasks))
    sessions = [s for s in sessions if s is not None]
    return sessions


async def _do_scrape_leg(leg_num: int) -> dict | None:
    try:
        s = await _low.session(
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
            session=leg_num,
        )
    except _low.DataUnimplementedError:
        return None
    s = {**s, "LegislatureNumber": int(s["Number"])}
    del s["Number"]
    return s


def _estimate_max_leg_num(current_year: int | None = None):
    if current_year is None:
        current_year = datetime.datetime.now().year
    return (current_year - 2023) // 2 + 33


assert _estimate_max_leg_num(2021) == 32, _estimate_max_leg_num(2021)
assert _estimate_max_leg_num(2022) == 32, _estimate_max_leg_num(2022)
assert _estimate_max_leg_num(2023) == 33, _estimate_max_leg_num(2023)
assert _estimate_max_leg_num(2024) == 33, _estimate_max_leg_num(2024)
assert _estimate_max_leg_num(2025) == 34, _estimate_max_leg_num(2025)
assert _estimate_max_leg_num(2026) == 34, _estimate_max_leg_num(2026)


def scrape_members(
    *, legislature_numbers: list[int], d: Path | str, cache: Cache = False
) -> list[dict]:
    d = Path(d)
    tasks = []
    cached_results = []
    for leg_num in legislature_numbers:
        p = d / f"members_{leg_num}.json"
        if _need_refresh(leg_num, legislature_numbers, cache, p):
            tasks.append(_scrape_members_of_leg(leg_num, p))
        else:
            cached_results.append(json.loads(p.read_text()))
    results = asyncio.run(asyncio.gather(*tasks))
    results = [r for r in results if r is not None]
    results.extend(cached_results)
    flattened = []
    for r in results:
        flattened.extend(r)
    return flattened


async def _scrape_members_of_leg(legislature_number: int, p: Path) -> list[dict] | None:
    try:
        m = await _low.members(session=legislature_number)
    except _low.DataUnimplementedError:
        # TODO: do something so we don't continually try re-scraping
        return None
    members = [{**member, "LegislatureNumber": legislature_number} for member in m]
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(members, indent=4))
    return members


def scrape_bills(
    *, legislature_numbers: list[int], d: Path | str, cache: Cache = False
) -> list[dict]:
    d = Path(d)
    tasks = []
    cached_results = []
    for leg_num in legislature_numbers:
        p = d / f"bills_{leg_num}.json"
        if _need_refresh(leg_num, legislature_numbers, cache, p):
            tasks.append(scrape_bills_of_legislature(leg_num, p))
        else:
            cached_results.append(json.loads(p.read_text()))
    results = asyncio.run(asyncio.gather(*tasks))
    results = [r for r in results if r is not None]
    results.extend(cached_results)
    flattened = []
    for r in results:
        flattened.extend(r)
    return flattened


async def scrape_bills_of_legislature(
    legislature_number: int, p: Path
) -> list[dict] | None:
    try:
        b = await _low.bills(
            queries=[
                "Subjects",
                # "Actions",  # this makes each response too large
            ],
            session=legislature_number,
        )
    except _low.DataUnimplementedError:
        # TODO: do something so we don't continually try re-scraping
        return None
    bills = [{**bill, "LegislatureNumber": legislature_number} for bill in b]
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(_low.json.dumps(bills, indent=4))
    return bills


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


def scrape_votes(
    *, members: list[Member], d: Path | str, cache: Cache = False
) -> list[dict]:
    d = Path(d)
    results = []
    leg_nums = {m["LegislatureNumber"] for m in members}
    for leg_num in leg_nums:
        member_codes = [m["Code"] for m in members if m["LegislatureNumber"] == leg_num]
        p = d / f"votes_{leg_num}.json"
        if _need_refresh(leg_num, leg_nums, cache, p):
            results.extend(scrape_votes_of_legislature(leg_num, member_codes, p))
        else:
            results.extend(json.loads(p.read_text()))
    return results


def scrape_votes_of_legislature(
    leg_number: int, member_codes: list[str], p: Path
) -> list[dict] | None:
    tasks = [_scrape_votes_of(leg_number, code) for code in member_codes]
    votes = asyncio.run(asyncio.gather(*tasks))
    votes = [v for v in votes if v is not None]
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(_low.json.dumps(votes, indent=4))
    return votes


async def _scrape_votes_of(
    leg_num: int, member_code: str
) -> tuple[list[dict], list[dict]] | None:
    try:
        mems = await _low.members(
            session=leg_num,
            queries=[
                f"members;code={member_code}",
                "Votes",
                "Bills",
            ],
        )
    except _low.DataUnimplementedError:
        return None
    votes = []
    for m in mems:
        votes.extend(m["Votes"])
    votes = [{**v, "LegislatureNumber": leg_num} for v in votes]
    return votes


def _is_latest(session, sessions) -> bool:
    return int(session) == max(int(s) for s in sessions)


def _need_refresh(leg_num: int, sessions: Iterable[int], cache: Cache, path: Path):
    if not cache:
        return True
    if not path.exists():
        return True
    if cache is True:
        return False
    assert cache == "previous-sessions"
    return _is_latest(leg_num, sessions)
