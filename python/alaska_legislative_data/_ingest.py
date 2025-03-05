import logging

import ibis

from alaska_legislative_data import _curated, _db, _parse, _scrape, _util

logger = logging.getLogger(__name__)


def ingest_all(
    db: str | _db.Backend = _util.DEFAULT_DB_PATH,
    *,
    people: ibis.Table | None = None,
    members: ibis.Table | None = None,
    bills: ibis.Table | None = None,
):
    ingest_legislatures_and_sessions(db)
    ingest_people(db, people=people)
    ingest_members(db, members=members)
    ingest_bills(db, new_bills=bills)


def ingest_legislatures_and_sessions(
    db: str | _db.Backend = _util.DEFAULT_DB_PATH,
    legislatures: ibis.Table | None = None,
    sessions: ibis.Table | None = None,
):
    db = _db.Backend(db)
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
    db: str | _db.Backend = _util.DEFAULT_DB_PATH,
    people: ibis.Table | None = None,
) -> None:
    """Ingest the curated people data."""
    db = _db.Backend(db)
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
    db: str | _db.Backend = _util.DEFAULT_DB_PATH,
    members: ibis.Table | None = None,
):
    """Ingest the curated members data."""
    db = _db.Backend(db)
    if members is None:
        members = _curated.read_members(backend=db)

    # avoid https://github.com/ibis-project/ibis/issues/10942
    members = ibis.memtable(members.to_pyarrow(), schema=members.schema())
    logger.info(f"Ingesting {members.count().execute()} members")
    if "MemberId" not in members.columns:
        members = members.mutate(
            MemberId=members.LegislatureNumber.cast(str) + ":" + members.PersonId
        )

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

    n_existing_members = members.semi_join(db.Member, "MemberId").count().execute()
    new_members = members.anti_join(db.Member, "MemberId")
    n_new_members = new_members.count().execute()
    logger.info(f"Found {n_existing_members} existing members")
    logger.info(f"Found {n_new_members} new members")
    if n_new_members > 0:
        logger.info(f"Adding {n_new_members} new members")
        db.insert("members", new_members)


def ingest_bills(
    db: str | _db.Backend = _util.DEFAULT_DB_PATH,
    new_bills: ibis.Table | None = None,
):
    db = _db.Backend(db)
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


def _missing_leg_nums(
    *, min_leg_num: int, existing_leg_nums: ibis.ir.IntegerColumn
) -> list[int]:
    """Determine which legislature numbers are missing from the database."""
    desired_nums = range(min_leg_num, _util.current_leg_num_approx() + 1)
    existing_nums = existing_leg_nums.name("num").as_table().distinct().num.execute()
    logger.info(f"Desired leg_nums: {sorted(desired_nums)}")
    logger.info(f"Existing leg_nums: {sorted(existing_nums)}")
    missing_nums = sorted(set(desired_nums) - set(existing_nums))
    logger.info(f"Missing leg_nums: {missing_nums}")
    return missing_nums
