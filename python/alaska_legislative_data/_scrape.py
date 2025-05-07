import asyncio
import logging
from typing import TypedDict

from alaska_legislative_data import _bill_version_text, _low, _util

logger = logging.getLogger(__name__)


class BillSpec(TypedDict):
    LegislatureNumber: int
    BillNumber: str


def scrape_legislatures_and_sessions(
    legislature_numbers: list[int] | None = None,
) -> list[dict]:
    if legislature_numbers is None:
        legislature_numbers = _gen_leg_numbers()

    async def main() -> list[dict]:
        tasks = [_do_scrape_leg(n) for n in legislature_numbers]
        sessions = await asyncio.gather(*tasks)
        sessions = [s for s in sessions if s is not None]
        return sessions

    leg_and_sessions = asyncio.run(main())
    return leg_and_sessions


def _gen_leg_numbers() -> list[int]:
    return list(range(1, _util.current_leg_num_approx() + 2))  # +2 to be safe


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


def scrape_members(legislature_numbers: list[int] | None = None) -> list[dict]:
    if legislature_numbers is None:
        legislature_numbers = _gen_leg_numbers()

    async def main():
        tasks = [_scrape_members_of_leg(n) for n in legislature_numbers]
        return await asyncio.gather(*tasks)

    results = asyncio.run(main())
    results = [r for r in results if r is not None]
    flattened = []
    for r in results:
        flattened.extend(r)
    return flattened


async def _scrape_members_of_leg(legislature_number: int) -> list[dict] | None:
    try:
        m = await _low.members(session=legislature_number)
    except _low.DataUnimplementedError:
        # TODO: do something so we don't continually try re-scraping
        return None
    return [{**member, "LegislatureNumber": legislature_number} for member in m]


def scrape_bills(
    legislature_numbers: list[int] | None = None,
) -> list[dict]:
    if legislature_numbers is None:
        legislature_numbers = _gen_leg_numbers()

    async def main():
        tasks = [scrape_bills_of_legislature(n) for n in legislature_numbers]
        return await asyncio.gather(*tasks)

    results = asyncio.run(main())
    results = [r for r in results if r is not None]
    flattened = []
    for r in results:
        flattened.extend(r)
    return flattened


async def scrape_bills_of_legislature(legislature_number: int) -> list[dict] | None:
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
    return bills


async def scrape_bill_details(
    legislature_number: int, bill_number: str
) -> _low.Bill | None:
    try:
        bills = await _low.bills(
            queries=[
                f"bills;bill={bill_number}",
                "versions;fulltext=urlonly",
                "sponsors",
            ],
            session=legislature_number,
        )
        logger.debug(f"Scraped bill {bill_number} from {legislature_number}")
        return {
            **bills[0],
            "LegislatureNumber": legislature_number,
        }
    except _low.DataUnimplementedError:
        # TODO: do something so we don't continually try re-scraping
        return None


async def scrape_bill_versions(leg_num: int, bill_number: str) -> list[dict]:
    """Scrape the versions of a bill."""
    raw_bill = await scrape_bill_details(
        legislature_number=leg_num, bill_number=bill_number
    )
    if raw_bill is None:
        logger.warning(f"Failed to scrape bill {leg_num}:{bill_number}")
        return []
    versions = await _prep_bill_versions(leg_num, raw_bill)
    return versions


def scrape_votes(*, leg_num_and_member_codes: list[tuple[int, str]]) -> list[dict]:
    async def main():
        tasks = [_scrape_votes_of(*t) for t in leg_num_and_member_codes]
        results = []
        for chunk in _util.chunks(tasks, 20):
            chunk_votes = await asyncio.gather(*chunk)
            for v in chunk_votes:
                if v is None:
                    continue
                results.extend(v)
        return results

    results = asyncio.run(main())
    return results


async def _scrape_votes_of(
    leg_num: int, member_code: str
) -> tuple[list[dict], list[dict]] | None:
    try:
        mems = await _low.members(
            session=leg_num,
            queries=[
                f"members;code={member_code}",
                "Votes",
                # "Bills",  # this gets the bills they sponsored,
            ],
        )
    except _low.DataUnimplementedError:
        return None
    except _low.ServerError:
        if (leg_num, member_code) in KNOWN_FAILING_MEMBERS:
            return None
        raise
    votes = []
    for m in mems:
        votes.extend(m["Votes"])
    votes = [{**v, "LegislatureNumber": leg_num} for v in votes]
    return votes


async def _prep_bill_versions(leg_num: int, bill: _low.Bill) -> list[dict]:
    bill_id = f"""{leg_num}:{bill["BillNumber"]}"""

    async def _version(raw_version: _low.BillVersion):
        bill_version_id = f"""{bill_id}:{raw_version["VersionLetter"]}"""
        full_text = await _bill_version_text.get_bill_version_text(
            legislature_number=leg_num,
            bill_number=bill["BillNumber"],
            version_letter=raw_version["VersionLetter"],
        )
        return {
            "BillVersionId": bill_version_id,
            "BillId": bill_id,
            "BillVersionLetter": raw_version["VersionLetter"],
            "BillVersionTitle": raw_version["Title"],
            "BillVersionName": raw_version["Name"],
            "BillVersionIntroDate": raw_version["IntroDate"],
            "BillVersionPassedHouse": raw_version["PassedHouse"],
            "BillVersionPassedSenate": raw_version["PassedSenate"],
            "BillVersionWorkOrder": raw_version["WorkOrder"],
            "BillVersionPdfUrl": raw_version["Url"],
            "BillVersionFullText": full_text,
        }

    tasks = [_version(raw_version) for raw_version in bill["Versions"]]
    logger.debug(f"Scraping {len(tasks)} versions for {bill_id} in leg {leg_num}")
    result = await asyncio.gather(*tasks)
    logger.debug(
        f"Scraping {len(tasks)} versions for {bill_id} in leg {leg_num}...Done"
    )
    return result


KNOWN_FAILING_MEMBERS = [
    (18, "GRS"),
    (18, "HUD"),
    (18, "DVG"),
]
