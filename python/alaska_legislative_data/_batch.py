import shutil
from pathlib import Path

import ibis
from ibis import BaseBackend

from alaska_legislative_data import _augment, _git, _parse, _scrape


def process_batch(batch_dir: str | Path) -> _augment.AugmentedTables:
    """Scrape the API and augment the data with hand-curated data."""
    batch_dir = Path(batch_dir)
    dir_raw = batch_dir / "raw"
    dir_parsed = batch_dir / "parsed"
    dir_aug = batch_dir / "augmented"
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
    if directory.exists():
        shutil.rmtree(directory)
    augmented_to_csvs(aug, directory)
    # augmented_to_duckdb(aug, directory / "db.duckdb")


def scrape_and_push(
    branch: str,
    *,
    batch_dir: str | Path = "./.ak-leg-data",
    remote: str | None = None,
    commit_message: str = "Update data",
    tmp_git_dir: str | Path = None,
):
    batch_dir = Path(batch_dir)
    aug = process_batch(batch_dir=batch_dir)
    export_dir = batch_dir / "export"
    export(aug, export_dir)
    _git.push_directory_to_github_branch(
        export_dir,
        branch,
        remote=remote,
        commit_message=commit_message,
        tmp_git_dir=tmp_git_dir or batch_dir / "git",
    )


def augmented_to_csvs(aug: _augment.AugmentedTables, dir: str | Path):
    dir = Path(dir)
    dir.mkdir(exist_ok=True)
    aug.legislatures.to_csv(dir / "legislatures.csv")
    aug.people.to_csv(dir / "people.csv")
    aug.members.to_csv(dir / "members.csv")
    aug.bills.to_csv(dir / "bills.csv")
    aug.votes.to_csv(dir / "votes.csv")
    aug.choices.to_csv(dir / "choices.csv")


def augmented_to_duckdb(
    aug: _augment.AugmentedTables, conn_or_path: str | Path | BaseBackend
):
    if isinstance(conn_or_path, str) or isinstance(conn_or_path, Path):
        conn = ibis.duckdb.connect(conn_or_path)
    else:
        conn = conn_or_path
    conn.create_table("legislatures", aug.legislatures.to_pyarrow(), overwrite=True)
    conn.create_table("people", aug.people.to_pyarrow(), overwrite=True)
    conn.create_table("memberships", aug.members.to_pyarrow(), overwrite=True)
    conn.create_table("bills", aug.bills.to_pyarrow(), overwrite=True)
    conn.create_table("votes", aug.votes.to_pyarrow(), overwrite=True)
    conn.create_table("choices", aug.choices.to_pyarrow(), overwrite=True)
