import shutil
from pathlib import Path

from alaska_legislative_data import _db


def export(
    *,
    db: str | Path | _db.Backend | None = None,
    directory: str | Path = "export/",
):
    db = _db.get_db(db)
    directory = Path(directory)
    if directory.exists():
        shutil.rmtree(directory)
    directory.mkdir(exist_ok=True)
    to_csvs(db, directory)
    to_duckdb(db, directory / "ak_leg.duckdb")


def to_csvs(db: _db.Backend, dir: str | Path):
    dir = Path(dir)
    dir.mkdir(exist_ok=True)
    # db.Legislature.to_csv(dir / "legislatures.csv")
    # db.LegislatureSession.to_csv(dir / "sessions.csv")
    db.Person.to_csv(dir / "people.csv")
    db.Member.to_csv(dir / "members.csv")
    db.Bill.to_csv(dir / "bills.csv")
    db.Vote.to_csv(dir / "votes.csv")
    db.Choice.to_csv(dir / "choices.csv")
    db.BillVersion.to_csv(dir / "bill_versions.csv")


def to_duckdb(db: _db.Backend, path: str | Path):
    """Export the entire database to a new .duckdb file."""
    path = Path(path)
    path.unlink(missing_ok=True)
    path.parent.mkdir(exist_ok=True)
    db.raw_sql(f"ATTACH '{path}' AS export;")
    db.create_table("people", db.Person, database=("export", "main"))
    db.create_table("members", db.Member, database=("export", "main"))
    db.create_table("bills", db.Bill, database=("export", "main"))
    db.create_table("votes", db.Vote, database=("export", "main"))
    db.create_table("choices", db.Choice, database=("export", "main"))
    db.create_table("billVersions", db.BillVersion, database=("export", "main"))
