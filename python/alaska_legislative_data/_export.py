import shutil
import tempfile
from pathlib import Path

import ibis

from alaska_legislative_data import _db, _util


def export(
    *,
    db: str | Path | _db.Backend = _util.DEFAULT_DB_PATH,
    directory: str | Path = "export/",
):
    db = _db.Backend(db)
    directory = Path(directory)
    if directory.exists():
        shutil.rmtree(directory)
    to_csvs(db, directory)
    to_duckdb(db, directory / "ak_leg.duckdb")


def to_csvs(db: _db.Backend, dir: str | Path):
    dir = Path(dir)
    dir.mkdir(exist_ok=True)
    db.Legislature.to_csv(dir / "legislatures.csv")
    db.LegislatureSession.to_csv(dir / "sessions.csv")
    db.Person.to_csv(dir / "people.csv")
    db.Member.to_csv(dir / "members.csv")
    db.Bill.to_csv(dir / "bills.csv")
    db.Vote.to_csv(dir / "votes.csv")
    db.Choice.to_csv(dir / "choices.csv")


def to_duckdb(db: _db.Backend, path: str | Path):
    """Export the entire database to a new .duckdb file."""
    with tempfile.TemporaryDirectory() as tempdir:
        db.raw_sql(f"EXPORT DATABASE '{tempdir}' (FORMAT parquet);")
        new_db = ibis.duckdb.connect(path)
        new_db.raw_sql(f"IMPORT DATABASE '{tempdir}'")
