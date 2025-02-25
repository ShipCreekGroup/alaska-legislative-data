import asyncio
import json
import logging
from collections.abc import Iterable
from typing import Literal

import httpx

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


class ServerError(RuntimeError):
    """Exception when the server returns an error."""

    def __init__(self, response: str, url: str, headers: dict) -> None:
        super().__init__(f"Server error for {url} with headers {headers}: {response}")


async def bills(
    queries: Iterable[str] | str | None = None,
    session: int | None = None,
    chamber: Literal["H", "S"] | None = None,
    range: slice | None = None,
) -> dict:
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


def make_client() -> httpx.AsyncClient:
    timeout = httpx.Timeout(30.0, pool=30.0)
    limits = httpx.Limits(max_keepalive_connections=None, max_connections=1000)
    return httpx.AsyncClient(timeout=timeout, limits=limits, follow_redirects=True)


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
        client = make_client()
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
    logger.info(f"Requesting {url} with headers {headers}")

    async with client:

        async def f():
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            return _parse(url, headers, response.text)

        try:
            return await _with_retries(
                f, max_retries=3, exception_classes=(httpx.HTTPError,)
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
        if "Invalid Session Number" in str(e):
            raise DataUnimplementedError(str(e)) from e
        if "<Code>FaultException</Code>" in raw:
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
