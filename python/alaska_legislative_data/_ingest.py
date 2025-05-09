import asyncio
import logging
import os
from urllib.parse import urlparse

import ibis
from ibis import _
from ibis.backends.duckdb import Backend as DuckDBBackend

from alaska_legislative_data import (
    _curated,
    _db,
    _parse,
    _scrape,
    _split_choices,
    _util,
)

logger = logging.getLogger(__name__)


def ingest_all(
    db: str | _db.Backend | None = None,
    *,
    legislatures: ibis.Table | None = None,
    sessions: ibis.Table | None = None,
    people: ibis.Table | None = None,
    members: ibis.Table | None = None,
    bills: ibis.Table | None = None,
    votes: ibis.Table | None = None,
    choices: ibis.Table | None = None,
):
    db = get_db(db)
    # ingest_legislatures_and_sessions(db, legislatures=legislatures, sessions=sessions)
    ingest_people(db, people=people)
    ingest_members(db, members=members)
    ingest_bills(db, new_bills=bills)
    ingest_votes_and_choices(db, votes=votes, choices=choices)


def ingest_legislatures_and_sessions(
    db: str | _db.Backend,
    legislatures: ibis.Table | None = None,
    sessions: ibis.Table | None = None,
):
    db = get_db(db)
    if legislatures is None or sessions is None:
        leg, sess = _scrape_missing_legislatures_and_sessions(db)
        if legislatures is None:
            legislatures = leg
        if sessions is None:
            sessions = sess

    # avoid https://github.com/ibis-project/ibis/issues/10942
    legislatures = ibis.memtable(
        legislatures.to_pyarrow(), schema=legislatures.schema()
    )
    sessions = ibis.memtable(sessions.to_pyarrow(), schema=sessions.schema())

    logger.info(f"Ingesting {legislatures.count().execute()} legislatures")
    logger.info(f"Ingesting {sessions.count().execute()} sessions")

    n_existing_legs = (
        legislatures.semi_join(db.Legislature, "LegislatureNumber").count().execute()
    )
    new_legs = legislatures.anti_join(db.Legislature, "LegislatureNumber")
    n_new_legs = new_legs.count().execute()
    logger.info(f"Found {n_existing_legs} existing legislatures")
    logger.info(f"Found {n_new_legs} new legislatures")
    if n_new_legs > 0:
        logger.info(f"Adding {n_new_legs} new legislatures")
        db.insert("legislatures", new_legs)

    n_existing_sess = (
        sessions.semi_join(db.LegislatureSession, "LegislatureSessionId")
        .count()
        .execute()
    )
    new_sess = sessions.anti_join(db.LegislatureSession, "LegislatureSessionId")
    n_new_sess = new_sess.count().execute()
    logger.info(f"Found {n_existing_sess} existing sessions")
    logger.info(f"Found {n_new_sess} new sessions")
    if n_new_sess > 0:
        logger.info(f"Adding {n_new_sess} new sessions")
        db.insert("legislature_sessions", new_sess)


def ingest_people(
    db: str | _db.Backend,
    people: ibis.Table | None = None,
) -> None:
    """Ingest the curated people data."""
    db = get_db(db)
    if people is None:
        people = _curated.read_people(backend=db)

    # avoid https://github.com/ibis-project/ibis/issues/10942
    people = ibis.memtable(people.to_pyarrow(), schema=people.schema())
    logger.info(f"Ingesting {people.count().execute()} people")

    only_new = db.Person.anti_join(people, "PersonId").to_pandas()
    if not only_new.empty:
        raise ValueError(
            f"Some people are in the database but not in the curated list: {only_new}"
        )

    n_existing_people = people.semi_join(db.Person, "PersonId").count().execute()
    new_people = people.anti_join(db.Person, "PersonId")
    n_new_people = new_people.count().execute()
    logger.info(f"Found {n_existing_people} existing people")
    logger.info(f"Found {n_new_people} new people")
    if n_new_people > 0:
        logger.info(f"Adding {n_new_people} new people")
        db.insert("people", new_people)


def ingest_members(
    db: str | _db.Backend,
    members: ibis.Table | None = None,
):
    """Ingest the curated members data."""
    db = get_db(db)
    if members is None:
        members = _curated.read_members(backend=db)

    # avoid https://github.com/ibis-project/ibis/issues/10942
    members = ibis.memtable(members.to_pyarrow(), schema=members.schema())
    logger.info(f"Ingesting {members.count().execute()} members")

    only_new = db.Member.anti_join(members, "MemberId").to_pandas()
    if not only_new.empty:
        raise ValueError(
            f"Some members are in the database but not in the new list: {only_new}"
        )

    missing_person = members.anti_join(db.Person, "PersonId").to_pandas()
    if not missing_person.empty:
        raise ValueError(
            f"Some new members don't have an existing person: {missing_person}"
        )

    dupe_member_id = members.MemberId.topk(10, name="n").filter(_.n > 1).to_pandas()
    if not dupe_member_id.empty:
        raise ValueError(f"Some new members have duplicate MemberId: {dupe_member_id}")

    n_existing_members = members.semi_join(db.Member, "MemberId").count().execute()
    new_members = members.anti_join(db.Member, "MemberId")
    n_new_members = new_members.count().execute()
    logger.info(f"Found {n_existing_members} existing members")
    logger.info(f"Found {n_new_members} new members")
    if n_new_members > 0:
        logger.info(f"Adding {n_new_members} new members")
        db.insert("members", new_members)


def ingest_bills(
    db: str | _db.Backend,
    new_bills: ibis.Table | None = None,
):
    db = get_db(db)
    if new_bills is None:
        new_bills = _scrape_missing_bills(existing_bills=db.Bill)

    # avoid https://github.com/ibis-project/ibis/issues/10942
    new_bills = ibis.memtable(new_bills.to_pyarrow(), schema=new_bills.schema())
    logger.info(f"Ingesting {new_bills.count().execute()} bills")

    n_existing_bills = new_bills.semi_join(db.Bill, "BillId").count().execute()
    new_bills = new_bills.anti_join(db.Bill, "BillId")
    n_new_bills = new_bills.count().execute()
    logger.info(f"Found {n_existing_bills} existing bills")
    logger.info(f"Found {n_new_bills} new bills")
    if n_new_bills > 0:
        logger.info(f"Adding {n_new_bills} new bills")
        db.insert("bills", new_bills)


def ingest_votes_and_choices(
    db: str | _db.Backend,
    *,
    votes: ibis.Table | None = None,
    choices: ibis.Table | None = None,
):
    db = get_db(db)
    if votes is None or choices is None:
        v, c = _scrape_missing_votes_and_choices(db)
        if votes is None:
            votes = v
        if choices is None:
            choices = c

    # avoid https://github.com/ibis-project/ibis/issues/10942
    votes = ibis.memtable(votes.to_pyarrow(), schema=votes.schema())
    choices = ibis.memtable(choices.to_pyarrow(), schema=choices.schema())

    logger.info(f"Ingesting {votes.count().execute()} votes")
    logger.info(f"Ingesting {choices.count().execute()} choices")

    n_existing_votes = votes.semi_join(db.Vote, "VoteId").count().execute()
    new_votes = votes.anti_join(db.Vote, "VoteId")
    n_new_votes = new_votes.count().execute()
    logger.info(f"Found {n_existing_votes} existing votes")
    logger.info(f"Found {n_new_votes} new votes")
    if n_new_votes > 0:
        logger.info(f"Adding {n_new_votes} new votes")
        db.insert("votes", new_votes)

    n_existing_choices = choices.semi_join(db.Choice, "ChoiceId").count().execute()
    new_choices = choices.anti_join(db.Choice, "ChoiceId")
    n_new_choices = new_choices.count().execute()
    logger.info(f"Found {n_existing_choices} existing choices")
    logger.info(f"Found {n_new_choices} new choices")
    if n_new_choices > 0:
        logger.info(f"Adding {n_new_choices} new choices")
        db.insert("choices", new_choices)


def bills_needing_version_updates(backend: _db.Backend) -> list[_scrape.BillSpec]:
    latest_leg_num = backend.Bill.LegislatureNumber.max().execute()
    t = backend.Bill.filter(
        backend.Bill.LegislatureNumber > 25,
        ibis.or_(
            backend.Bill.LegislatureNumber >= latest_leg_num,
            backend.Bill.BillId.notin(backend.BillVersion.BillId),
        ),
    )
    return (
        t.select(
            backend.Bill.LegislatureNumber,
            backend.Bill.BillNumber,
        )
        .order_by(
            backend.Bill.LegislatureNumber.asc(),
        )
        .to_pandas()
        .to_dict(orient="records")
    )


def scrape_and_insert_bill_versions(
    *,
    db: _db.Backend | str | None = None,
    bills: list[_scrape.BillSpec] | None = None,
):
    """Scrape the bill versions and insert them into the database."""
    db = get_db(db)
    if bills is None:
        bills = bills_needing_version_updates(db)
    logger.info(f"Scraping bill versions for {len(bills)} bills")
    bills = list(bills)
    # Do in chunks so that if we error, we still have made some progress
    MAX_BILLS = 50
    if len(bills) > MAX_BILLS:
        results = []
        for chunk in _util.chunks(bills, MAX_BILLS):
            results.extend(scrape_and_insert_bill_versions(db=db, bills=chunk))
        return results

    tasks = [
        _scrape.scrape_bill_versions(
            leg_num=spec["LegislatureNumber"], bill_number=spec["BillNumber"]
        )
        for spec in bills
    ]

    async def main():
        return await asyncio.gather(*tasks)

    task_results = asyncio.run(main())
    bill_versions = []
    for task_result in task_results:
        bill_versions.extend(task_result)
    inserted = _insert_bill_versions(db, bill_versions)
    return inserted


def _insert_bill_versions(
    db: _db.Backend,
    versions: list[dict],
) -> list[dict]:
    """Insert the bill versions into the database."""
    new = ibis.memtable(versions, schema=db.BillVersion.schema())
    logger.info(f"Ingesting {new.count().execute()} bill versions")

    new = new.anti_join(db.BillVersion, "BillVersionId")
    new = new.cache()
    n_new = new.count().execute()
    # logger.info(f"Found {n_existing} existing bill versions")
    logger.info(f"Found {n_new} new bill versions")
    if n_new > 0:
        logger.info(f"Adding {n_new} new bill versions")
        db.insert("billVersions", new)
    return new.execute().to_dict(orient="records")


def _scrape_missing_legislatures_and_sessions(
    db: _db.Backend,
) -> tuple[ibis.Table, ibis.Table]:
    existing_legs = (
        ibis.union(
            db.Legislature.LegislatureNumber.as_table(),
            db.LegislatureSession.LegislatureNumber.as_table(),
        )
        .distinct()
        .LegislatureNumber
    )
    missing_leg_nums = _missing_leg_nums(
        min_leg_num=12, existing_leg_nums=existing_legs
    )
    scraped_dicts = _scrape.scrape_legislatures_and_sessions(missing_leg_nums)
    if not scraped_dicts:
        # workaround for https://github.com/ibis-project/ibis/issues/10940
        leg = db.Legislature.limit(0)
        sess = db.LegislatureSession.limit(0)
    else:
        scraped_raw = ibis.memtable(scraped_dicts)
        leg, sess = _parse.clean_and_split_legislatures_into_sessions(scraped_raw)
    return leg, sess


def _scrape_missing_bills(*, existing_bills: _db.BillTable) -> ibis.Table:
    """Scrape the bills for the legislatures that are missing bills."""
    missing_leg_nums = _missing_leg_nums(
        # the API doesn't have any data from the 11th legislature and before
        min_leg_num=12,
        existing_leg_nums=existing_bills.LegislatureNumber,
    )
    logger.info(f"Scraping missing bills for {missing_leg_nums}")
    bill_dicts = _scrape.scrape_bills(legislature_numbers=missing_leg_nums)
    if not bill_dicts:
        # workaround for https://github.com/ibis-project/ibis/issues/10940
        bills = existing_bills.limit(0)
    else:
        bills = ibis.memtable(bill_dicts)
        bills = _parse.clean_bills(bills)
    return bills


def _votes_to_scrape(db: _db.Backend) -> list[tuple[int, str]]:
    """Determine which LegNum, MemberCode pairs might be missing from the database."""
    missing_leg_nums = _missing_leg_nums(
        # the API doesn't have any data from the 18th legislature and before,
        min_leg_num=19,
        existing_leg_nums=db.Vote.LegislatureNumber,
    )
    results = []
    for leg_num in missing_leg_nums:
        member_codes = db.Member.filter(
            db.Member.LegislatureNumber == leg_num
        ).MemberCode
        results.extend((leg_num, code) for code in member_codes.execute())
    return results


def _scrape_missing_votes_and_choices(
    db: _db.Backend, *, votes_to_scrape: list[tuple[int, str]] | None = None
) -> ibis.Table:
    if votes_to_scrape is None:
        votes_to_scrape = _votes_to_scrape(db)
    logger.info(f"Scraping missing votes for {votes_to_scrape}")
    dicts = _scrape.scrape_votes(leg_num_and_member_codes=votes_to_scrape)
    choices = ibis.memtable(dicts)
    choices = _parse.clean_choices(choices)
    votes, choices = _split_choices.split_choices(
        choices_raw=choices, bills=db.Bill, members=db.Member
    )
    return votes, choices


def _missing_leg_nums(
    *,
    min_leg_num: int,
    existing_leg_nums: ibis.ir.IntegerColumn,
) -> list[int]:
    """Determine which legislature numbers are missing from the database."""
    desired_nums = range(min_leg_num, _util.current_leg_num_approx() + 1)
    existing_nums = existing_leg_nums.name("num").as_table().distinct().num.execute()
    logger.info(f"Desired leg_nums: {sorted(desired_nums)}")
    logger.info(f"Existing leg_nums: {sorted(existing_nums)}")
    missing_nums = set(desired_nums) - set(existing_nums)
    # Always assume that we have a stale view of the latest legislature
    if len(existing_nums):
        latest = max(existing_nums)
        missing_nums.add(latest)
    missing_nums_list = sorted(missing_nums)
    logger.info(f"Missing leg_nums: {missing_nums_list}")
    return missing_nums_list


def get_db(
    url: str | _db.Backend | None = None,
) -> _db.Backend:
    """Get a database connection."""
    if isinstance(url, _db.Backend):
        return url
    if url is None:
        url = os.environ.get("DATABASE_URL")
    backend: DuckDBBackend = ibis.duckdb.connect()
    attach_postgres(backend, url, name="postgres", schema="vote_tracker")
    backend.raw_sql("USE postgres")
    # backend.raw_sql("SET search_path TO vote_tracker;")
    return _db.Backend(backend, check_structure=False)


def attach_postgres(
    ddb_backend: DuckDBBackend,
    url: str,
    *,
    name: str | None = None,
    skip_if_exists: bool = False,
    schema: str | None = None,
) -> str:
    """Attach a PostgreSQL instance to a duckdb connection.

    This is useful for importing tables from PostgreSQL into duckdb.

    Parameters
    ----------
    ddb_backend:
        The duckdb backend to attach to.
    url:
        The connection string to the PostgreSQL instance.
    name:
        The name to attach as (the catalog name).
        If not provided, a unique one will be generated.
    skip_if_exists:
        Whether to skip the attachment if it already exists.
    schema:
        The schema to attach to in the PostgreSQL instance.

    Returns
    -------
    str
        The name of the attached catalog.
    """
    spec = urlparse(url)
    if name is None:
        name = ibis.util.gen_name(spec.hostname)
    ine = "IF NOT EXISTS" if skip_if_exists else ""
    options = ["TYPE postgres"]
    if schema is not None:
        options.append(f"SCHEMA '{schema}'")
    options_str = ", ".join(options)
    ddb_backend.raw_sql(f"""ATTACH {ine} '{url}' AS "{name}" ({options_str});""")
    return name
