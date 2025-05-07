import asyncio
import json
import logging
from collections.abc import Iterable
from typing import Literal, TypedDict

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://www.akleg.gov/publicservice/basis"


class DataUnimplementedError(ValueError):
    """Exception when you try to access data that is not implemented yet.

    eg you try to get the members for the 2nd legislature.
    This is back in 1961, the librarians at the Alaska Legislature
    have not yet digitized the data for this session.
    """


class ServerError(RuntimeError):
    """Exception when the server returns an error."""

    def __init__(self, response: str, url: str, headers: dict) -> None:
        super().__init__(f"Server error for {url} with headers {headers}: {response}")


class SponsoringMember(TypedDict):
    # {"Code":"BUR",
    # "UID":0,"LastName":"Burke",
    # "MiddleName":"",
    # "FirstName":"Robyn Niayuq",
    # "FormalName":"Representative Robyn Niayuq Burke",
    # "ShortName":"Burke ",
    # "Chamber":"H",
    # "District":"40",
    # "Seat":" ",
    # "Party":"D",
    # "Phone":"4653473",
    # "EMail":"Representative.Robyn.Burke@akleg.gov",
    # "Building":"CAPITOL",
    # "Room":"108",
    # "Comment":"",
    # "IsActive":true,"IsMajority":true}
    Code: str
    UID: int
    LastName: str
    MiddleName: str
    FirstName: str
    FormalName: str
    ShortName: str
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


class BillSponsor(TypedDict):
    # {"BillRoot":"HB 16",
    # "SponsoringMember":,
    # "SponsoringCommittee":null,"Chamber":"H",
    # "Requestor":"",
    # "Name":"Burke ",
    # "SponsorSeq":"07",
    # "isPrime":false},
    BillRoot: str
    SponsoringMember: SponsoringMember
    SponsoringCommittee: str | None
    Chamber: Literal["H", "S"]
    Requestor: str
    Name: str
    SponsorSeq: str
    isPrime: bool


class Enacted(TypedDict):
    # {"Url":null,"Mime":null,"Encoding":null,"Data":null}
    Url: str | None
    Mime: str | None
    Encoding: str | None
    Data: str | None


class BillVersion(TypedDict):
    # {"VersionLetter":"A",
    # "Title":"\"An Act amending campaign contribution limits for state and local office; directing the Alaska Public Offices Commission to adjust campaign contribution limits for state and local office once each decade beginning in 2031; and relating to campaign contribution reporting requirements.\" ",
    # "Name":"HB 16",
    # "IntroDate":"2025-01-22",
    # "PassedHouse":null,"PassedSenate":null,"WorkOrder":"34-LS0217",
    # "Url":"https://www.akleg.gov/PDF/34/Bills/HB0016A.PDF",
    # "Mime":null,"Encoding":null,"Data":null}
    VersionLetter: str
    Title: str
    Name: str
    IntroDate: str
    PassedHouse: str | None
    PassedSenate: str | None
    WorkOrder: str
    Url: str
    Mime: str | None
    Encoding: str | None
    Data: str | None


class Bill(TypedDict):
    # PartialVeto":false,"Vetoed":false
    # "BillNumber":"HB 16",
    # "BillName":"CSHB 16(STA)",
    # "ShortTitle":"CAMPAIGN FINANCE, CONTRIBUTION LIMITS",
    # "FullTitle":" \"An Act requiring a group supporting or opposing a candidate or ballot proposition in a state or local election to maintain an address in the state; amending campaign contribution limits for state and local office; directing the Alaska Public Offices Commission to adjust campaign contribution limits for state and local office once each decade beginning in 2031; relating to campaign contribution reporting requirements; relating to administrative complaints filed with the Alaska Public Offices Commission; relating to state election expenditures and contributions made by a foreign-influenced corporation or foreign national; and providing for an effective date.\"",
    # "StatusCode":"002",
    # "StatusText":"(S) FIN",
    # "Flag1":"S",
    # "Flag2":" ",
    # "StatusDate":"2025-04-30",
    # "StatusAndThen":[],"StatusSummaryCode":" ",
    # "OnFloor":" ",
    # "NotKnown":" ",
    # "Filler":" ",
    # "Lock":" ",
    PartialVeto: bool
    Vetoed: bool
    BillNumber: str
    BillName: str
    ShortTitle: str
    FullTitle: str
    StatusCode: str
    StatusText: str
    Flag1: str
    Flag2: str
    StatusDate: str
    StatusAndThen: list[str]
    StatusSummaryCode: str
    OnFloor: str
    NotKnown: str
    Filler: str
    Lock: str
    Sponsors: list[BillSponsor]
    Versions: list[BillVersion]
    SponsorUrl: str
    """eg "SponsorUrl":"http://www.akleg.gov/basis/get_documents.asp?session=34\u0026docid=6837"

    This goes to a PDF statement of Schrage, the bill's prime sponsor,
    explaining the reasoning behind the bill.

    The `\u0026` is a unicode escape for `&`
    """
    Enacted: Enacted


async def bills(
    queries: Iterable[str] | str | None = None,
    session: int | None = None,
    chamber: Literal["H", "S"] | None = None,
    range: slice | None = None,
) -> list[Bill]:
    return (
        await _make_request(
            "bills",
            queries=queries,
            session=session,
            chamber=chamber,
            range=range,
        )
    )["Bills"]


async def committees(
    queries: Iterable[str] | str | None = None,
    session: int | None = None,
    chamber: Literal["H", "S"] | None = None,
    range: slice | None = None,
) -> dict:
    return (
        await _make_request(
            "committees",
            queries=queries,
            session=session,
            chamber=chamber,
            range=range,
        )
    )["Committees"]


async def meetings(
    queries: Iterable[str] | str | None = None,
    session: int | None = None,
    chamber: Literal["H", "S"] | None = None,
    range: slice | tuple[int | None, int | None] | None = None,
) -> dict:
    return (
        await _make_request(
            "meetings",
            queries=queries,
            session=session,
            chamber=chamber,
            range=range,
        )
    )["Meetings"]


async def members(
    queries: Iterable[str] | str | None = None,
    session: int | None = None,
    chamber: Literal["H", "S"] | None = None,
    range: slice | tuple[int | None, int | None] | None = None,
) -> dict:
    if session <= 9:
        raise DataUnimplementedError(
            "Invalid Session Number: The data is not available for this session"
        )
    resp = await _make_request(
        "members",
        queries=queries,
        session=session,
        chamber=chamber,
        range=range,
    )
    return resp["Members"]


async def session(
    queries: Iterable[str] | str | None = None,
    session: int | None = None,
    chamber: Literal["H", "S"] | None = None,
    range: slice | tuple[int | None, int | None] | None = None,
) -> dict:
    """This doesn't follow the pattern and returns a single session."""
    result = await _make_request(
        "sessions",
        queries=queries,
        session=session,
        chamber=chamber,
        range=range,
    )
    return result["Session"]  # It is NOT "Sessions"


rate_semaphore = asyncio.Semaphore(5)


def get_client() -> httpx.AsyncClient:
    """Get a system-wide client for the Alaska Legislature API.

    By being system-wide, we can impose limits on the number of concurrent connections"""
    timeout = httpx.Timeout(30.0, pool=30.0)
    limits = httpx.Limits(max_keepalive_connections=None, max_connections=5)
    return httpx.AsyncClient(
        headers={
            "user-agent": "Mozilla/5.0",
            "X-Alaska-Legislature-Basis-Version": "1.4",
            "Accept-Encoding": "gzip;q=1.0",
        },
        timeout=timeout,
        limits=limits,
        follow_redirects=True,
    )


async def _make_request(
    endpoint: str,
    *,
    queries: Iterable[str] | str | None = None,
    session: int | None = None,
    chamber: Literal["H", "S"] | None = None,
    range: slice | tuple[int | None, int | None] | None = None,
    client: httpx.AsyncClient | None = None,
) -> dict:
    if client is None:
        client = get_client()
    url = f"https://www.akleg.gov/publicservice/basis/{endpoint}?minifyresult=false&json=true"
    if session is not None:
        url += f"&session={session}"
    if chamber is not None:
        url += f"&chamber={chamber}"

    if queries is None:
        queries = ()
    if isinstance(queries, str):
        queries = (queries,)
    headers = {}
    if queries:
        headers["X-Alaska-Legislature-Basis-Query"] = ",".join(queries)
    if range:
        headers["X-Alaska-Query-ResultRange"] = _range_str(range)
    logger.debug(f"Requesting {url} with headers {headers}")

    async with client:

        async def f():
            async with rate_semaphore:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                return _parse(url, headers, response.text)

        try:
            return await _with_retries(
                f,
                max_retries=3,
                # Sometimes the request fails with a ServerError but can succeed on retry
                exception_classes=(httpx.HTTPError, ServerError),
            )
        except DataUnimplementedError:
            raise
        except ServerError:
            raise
        except Exception as e:
            logger.error(f"Failed to get {url} with headers {headers}: {e!r}")
            raise


async def _with_retries(f, *, max_retries: int, exception_classes=(Exception,)):
    for i in range(max_retries):
        try:
            return await f()
        except exception_classes as e:
            logger.warning(f"Retrying {i + 1}/{max_retries} after error: {e!r}")
            if i == max_retries - 1:
                raise RuntimeError(f"Failed {max_retries} times") from e
            else:
                # exponential backoff
                # 0.5, 1, 2, 4, 8
                await asyncio.sleep(0.5 * 2**i)


def _parse(url: str, headers: dict, raw: str) -> dict:
    try:
        d = json.loads(raw)
    except json.JSONDecodeError as e:
        if "Invalid Session Number" in raw:
            raise DataUnimplementedError(str(e)) from e
        if "<Code>FaultException</Code>" in raw:
            raise ServerError(raw, url, headers) from e
        if "<Code>XmlSchemaValidationException</Code>" in raw:
            raise ServerError(raw, url, headers) from e
        raise ValueError(f"Invalid JSON: {raw}") from e
    return d["Basis"]


def _range_str(range: slice | tuple[int | None, int | None]) -> str:
    if not isinstance(range, (slice, tuple)):
        raise ValueError("Invalid range: {range}")
    if not isinstance(range, slice):
        range = slice(*range)
    if range.step is not None:
        raise ValueError("Step is not supported")

    if range.start is None and range.stop is not None and range.stop >= 0:
        return f"{range.stop}"
    if (
        range.start is not None
        and range.start >= 0
        and range.stop is not None
        and range.stop >= 0
    ):
        return f"{range.start}..{range.stop}"
    if range.start is not None and range.stop is None and range.start <= 0:
        return f"..{abs(range.start)}"
    raise ValueError(f"Invalid range: {range}")
