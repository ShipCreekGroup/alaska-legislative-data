import gzip
import json
import logging
from collections.abc import Iterable
from typing import Literal
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

BASE_URL = "https://www.akleg.gov/publicservice/basis"
BASE_HEADERS = {
    "user-agent": "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)",
    "X-Alaska-Legislature-Basis-Version": "1.4",
    "Accept-Encoding": "gzip;q=1.0",
}


class DataUnimplementedError(ValueError):
    """Exception when you try to access data that is not implemented yet.

    eg you try to get the members for the 2nd legislature.
    This is back in 1961, the librarians at the Alaska Legislature
    have not yet digitized the data for this session.
    """


def bills(
    queries: Iterable[str] | str | None = None,
    session: int | None = None,
    chamber: Literal["H", "S"] | None = None,
    range: slice | None = None,
) -> dict:
    return _make_request(
        "bills",
        queries=queries,
        session=session,
        chamber=chamber,
        range=range,
    )["Bills"]


def committees(
    queries: Iterable[str] | str | None = None,
    session: int | None = None,
    chamber: Literal["H", "S"] | None = None,
    range: slice | None = None,
) -> dict:
    return _make_request(
        "committees",
        queries=queries,
        session=session,
        chamber=chamber,
        range=range,
    )["Committees"]


def meetings(
    queries: Iterable[str] | str | None = None,
    session: int | None = None,
    chamber: Literal["H", "S"] | None = None,
    range: slice | tuple[int | None, int | None] | None = None,
) -> dict:
    return _make_request(
        "meetings",
        queries=queries,
        session=session,
        chamber=chamber,
        range=range,
    )["Meetings"]


def members(
    queries: Iterable[str] | str | None = None,
    session: int | None = None,
    chamber: Literal["H", "S"] | None = None,
    range: slice | tuple[int | None, int | None] | None = None,
) -> dict:
    return _make_request(
        "members",
        queries=queries,
        session=session,
        chamber=chamber,
        range=range,
    )["Members"]


def sessions(
    queries: Iterable[str] | str | None = None,
    session: int | None = None,
    chamber: Literal["H", "S"] | None = None,
    range: slice | tuple[int | None, int | None] | None = None,
) -> dict:
    return _make_request(
        "sessions",
        queries=queries,
        session=session,
        chamber=chamber,
        range=range,
    )["Session"]  # It is NOT "Sessions"


def _make_request(
    endpoint: str,
    *,
    queries: Iterable[str] | str | None = None,
    session: int | None = None,
    chamber: Literal["H", "S"] | None = None,
    range: slice | tuple[int | None, int | None] | None = None,
) -> dict:
    url = f"https://www.akleg.gov/publicservice/basis/{endpoint}?minifyresult=false&json=true"
    if session is not None:
        url += f"&session={session}"
    if chamber is not None:
        url += f"&chamber={chamber}"

    if queries is None:
        queries = ()
    if isinstance(queries, str):
        queries = (queries,)
    headers = {**BASE_HEADERS}
    if queries:
        headers["X-Alaska-Legislature-Basis-Query"] = ",".join(queries)
    if range:
        headers["X-Alaska-Query-ResultRange"] = _range_str(range)
    logger.debug(f"Requesting {url} with headers {headers}")
    req = Request(url=url, headers=headers)
    with urlopen(req) as response:
        if response.info().get("Content-Encoding") == "gzip":
            with gzip.GzipFile(fileobj=response) as uncompressed:
                return _parse(uncompressed.read().decode("utf-8"))
        else:
            return _parse(response.read().decode("utf-8"))


def _parse(raw: str) -> dict:
    try:
        d = json.loads(raw)
    except json.JSONDecodeError as e:
        if "<Code>FaultException</Code>" in raw:
            raise DataUnimplementedError(raw) from e
        raise ValueError(f"Invalid JSON: {raw}") from e
    return d["Basis"]


def _range_str(range: slice | tuple[int | None, int | None]) -> str:
    #  *  - '10' to get the first 10 results
    # *  - '10..20' to get results 10 through 20
    # *  - '..30' to get the last 30 results
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
