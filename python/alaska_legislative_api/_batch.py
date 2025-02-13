import sys
from pathlib import Path

import ibis
from ibis import BaseBackend

from alaska_legislative_api import _augment, _git, _parse, _scrape


def scrape(
    *,
    working_directory: str | Path = "./ak-leg-data",
) -> _augment.AugmentedTables:
    """Scrape the API and augment the data with hand-curated data."""
    working_directory = Path(working_directory)
    dir_raw = working_directory / "raw"
    dir_parsed = working_directory / "parsed"
    dir_aug = working_directory / "augmented"
    if not dir_raw.exists():
        _scrape.scrape(dir_raw)
    if not dir_parsed.exists():
        parsed = _parse.parse_scraped(dir_raw)
        parsed.to_parquets(dir_parsed)
    if not dir_aug.exists():
        parsed = _parse.ParsedTables.from_parquets(dir_parsed)
        augmented = _augment.augment_parsed(parsed)
        augmented.to_parquets(dir_aug)
    return _augment.AugmentedTables.from_parquets(dir_aug)


def export(aug: _augment.AugmentedTables, directory: str | Path):
    directory = Path(directory)
    augmented_to_csvs(aug, directory)
    augmented_to_duckdb(aug, directory / "db.duckdb")


def scrape_and_push(
    branch: str,
    *,
    working_directory: str | Path = "./ak-leg-data",
    remote: str | None = None,
    commit_message: str = "Update data",
    tmp_git_dir: str | Path = None,
):
    working_directory = Path(working_directory)
    aug = scrape(working_directory=working_directory)
    export_dir = working_directory / "export"
    export(aug, export_dir)
    _git.push_directory_to_github_branch(
        export_dir,
        branch,
        remote=remote,
        commit_message=commit_message,
        tmp_git_dir=tmp_git_dir,
    )


def augmented_to_csvs(aug: _augment.AugmentedTables, dir: str | Path):
    dir = Path(dir)
    dir.mkdir(exist_ok=True)
    aug.sessions.to_csv(dir / "sessions.csv")
    aug.people.to_csv(dir / "people.csv")
    aug.members.to_csv(dir / "memberships.csv")
    aug.bills.to_csv(dir / "bills.csv")
    aug.votes.to_csv(dir / "votes.csv")


def augmented_to_duckdb(
    aug: _augment.AugmentedTables, conn_or_path: str | Path | BaseBackend
):
    if isinstance(conn_or_path, str) or isinstance(conn_or_path, Path):
        conn = ibis.duckdb.connect(conn_or_path)
    else:
        conn = conn_or_path
    conn.create_table("sessions", aug.sessions.to_pyarrow())
    conn.create_table("people", aug.people.to_pyarrow())
    conn.create_table("memberships", aug.members.to_pyarrow())
    conn.create_table("bills", aug.bills.to_pyarrow())
    conn.create_table("votes", aug.votes.to_pyarrow())


if __name__ == "__main__":
    branch = sys.argv[1] or "scrape_and_push"
    scrape_and_push(branch)
