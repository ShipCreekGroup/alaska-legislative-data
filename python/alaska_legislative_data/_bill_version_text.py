import logging
import os
import re

import httpx
import ibis
from ibis import _
from ibis.backends.sql import BaseBackend as SQLBackend

from alaska_legislative_data import _low

logger = logging.getLogger(__name__)


def get_db(url: str | SQLBackend | None = None) -> SQLBackend:
    if isinstance(url, SQLBackend):
        return url
    if url is None:
        url = os.environ.get("DATABASE_URL")
    return ibis.connect(url)


def add_missing_bill_text(backend: str | SQLBackend | None = None) -> None:
    backend = get_db(backend)
    bills = backend.table("bills", database=("neondb", "vote_tracker"))
    logger.info(bills.get_name(), bills.schema())
    needs_text = bills.filter(
        _.BillLatestVersionText.isnull() | (_.BillLatestVersionText == ""),
        _.BillShortTitle != "NOT INTRODUCED",
        # early bills don't have text
        _.LegislatureNumber > 25,
    )
    # needs_text = needs_text.head(2)
    df = needs_text.select("BillId", "LegislatureNumber", "BillNumber").execute()
    records = df.to_dict(orient="records")
    logger.info("Updating %d bills with missing text", len(records))
    for record in records:
        text = get_bill_version_text(record["LegislatureNumber"], record["BillNumber"])
        backend.con.execute(
            """
                UPDATE vote_tracker.bills SET
                    "BillLatestVersionText" = %s
                WHERE
                    "vote_tracker"."bills"."BillId" = %s;
            """,
            (text, record["BillId"]),
        )
        backend.con.commit()


# https://www.akleg.gov/basis/Bill/Plaintext/25?Hsid=HB0087A
async def get_bill_version_text(
    *, legislature_number: int, bill_number: str, version_letter: str
) -> str:
    formatted_bill_number = format_bill_number(bill_number)
    url = f"https://www.akleg.gov/basis/Bill/Plaintext/{legislature_number}?Hsid={formatted_bill_number}{version_letter}"
    logger.info(
        "fetching text for %s %s %s", legislature_number, bill_number, version_letter
    )

    raw_text = await _fetch_safe(url)
    parsed = _parse_raw_text(raw_text)
    logger.info(
        "fetching text for %s %s %s...Done",
        legislature_number,
        bill_number,
        version_letter,
    )
    return parsed


def format_bill_number(bill_number_raw: str) -> str:
    # incoming looks like
    # HB 169 or HB3169 or HJR 42 or SJR2345
    # We need it to look like "HB0169"
    bill_type = re.search(r"[A-Z]{2,3}", bill_number_raw).group(0)
    bill_number = re.search(r"\d+", bill_number_raw).group(0)
    return f"{bill_type}{int(bill_number):04d}"


async def _fetch_safe(url: str) -> str:
    i = 0
    while True:
        i += 1
        exception = None
        try:
            return await _fetch(url)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise
            exception = e
        except Exception as e:
            exception = e
        logger.warning("Error fetching %s: %s", url, exception)
        if i > 5:
            raise


async def _fetch(url: str) -> str:
    async with httpx.AsyncClient(headers={"user-agent": "Mozilla/5.0"}) as client:
        async with _low.rate_semaphore:
            # some versions, like https://www.akleg.gov/basis/Bill/Plaintext/27?Hsid=SB0160C,
            # are huge and take a long time to download
            response = await client.get(url, timeout=60)
            response.raise_for_status()
            return response.text


def _parse_raw_text(raw_text: str) -> str:
    """Remove row numbers and control characters from the raw text

    Consider https://www.akleg.gov/PDF/34/Bills/HB0200A.PDF.
    At the foot of each page, it says "New Text Underlined [DELETED TEXT BRACKETED]".
    The raw text at https://www.akleg.gov/basis/Bill/Plaintext/34?Hsid=HB0200A shows:

    00                             HOUSE BILL NO. 200
    01 "An Act relating to the elimination or modification of state agency publications that are
    02 outdated, duplicative, or excessive."
    03 BE IT ENACTED BY THE LEGISLATURE OF THE STATE OF ALASKA:
    04    * Section 1.  AS 03.40.090 is amended to read:
    05 Sec. 03.40.090. Publication of record. The commissioner shall publish, in
    06 book form, a list of all brands and marks on record at the time of the publication. The
    07 lists may be supplemented from time to time. The publication must contain a facsimile
    08 of all recorded brands and marks, together with the owner's name and mailing address.
    09 The records shall be arranged in convenient form for reference. The publication must 
    10 be made available for download on the Internet website of the division of the 
    11 department with responsibility for agriculture [THE BOOKS AND
    12 SUPPLEMENTS MAY BE SOLD TO THE GENERAL PUBLIC AT AN AMOUNT
    13       NOT TO EXCEED $2 A COPY]. 
    14    * Sec. 2.  AS 14.42.035(a) is amended to read:
    01 (a)  The commission may require the institutions of public and private higher
    02 education and other institutions of postsecondary education in the state to submit data
    03 on costs, selection, and retention of students, enrollments, education outcomes, plant
    04 capacities and use, and other matters pertinent to effective planning and coordination
    05 [, AND SHALL FURNISH INFORMATION CONCERNING THESE MATTERS
    06 TO THE GOVERNOR, TO THE LEGISLATURE, AND TO OTHER STATE AND
    07       FEDERAL AGENCIES AS REQUESTED BY THEM]. 

    So, as far as I can tell:
    - The  (\x16) character marks the start of a section,
      and  (\x17) character marks the end of a section
    - the "" ("\x16\x10") character sequence marks the start,
       and the " " ("\x11 \x17") character sequence marks the end of
       a section of text that is added to law. In the PDF this is underlined.
    - IDK exactly what the ` ` sequence means

    In this function, we just remove the row numbers.
    The control characters are kept so that we can do further processing
    later if we want to.
    They shouldn't affect full text search in the database or
    how the text is displayed in a web app.
    """
    lines = raw_text.splitlines()
    lines = [line[3:] for line in lines if line]
    rejoined = "\n".join(lines)
    # rejoined = rejoined.replace("", "")
    # rejoined = rejoined.replace("", "")
    return rejoined
