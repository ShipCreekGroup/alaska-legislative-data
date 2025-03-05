import shutil
from pathlib import Path

from alaska_legislative_data import _db, _git, _util


def push(
    dir: str | Path,
    *,
    remote: str | None = None,
    branch: str = "data_v1_latest",
    commit_message: str = "Update data",
):
    _git.push_directory_to_github_branch(
        dir, branch, remote=remote, commit_message=commit_message
    )


def export(
    *,
    db: str | Path | _db.Backend = _util.DEFAULT_DB_PATH,
    directory: str | Path,
):
    db = _db.Backend(db)
    directory = Path(directory)
    if directory.exists():
        shutil.rmtree(directory)
    to_csvs(db, directory)


def to_csvs(db: _db.Backend, dir: str | Path):
    dir = Path(dir)
    dir.mkdir(exist_ok=True)
    # db.Legislature.to_csv(dir / "legislatures.csv")
    # db.Session.to_csv(dir / "sessions.csv")
    db.Person.to_csv(dir / "people.csv")
    db.Member.to_csv(dir / "members.csv")
    db.Bill.to_csv(dir / "bills.csv")
    db.Vote.to_csv(dir / "votes.csv")
    db.Choice.to_csv(dir / "choices.csv")
