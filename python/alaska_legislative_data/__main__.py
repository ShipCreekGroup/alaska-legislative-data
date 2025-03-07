import logging
import urllib.request
from pathlib import Path

import fire

from alaska_legislative_data import _export, _ingest

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(level=logging.INFO)
    fire.Fire(
        {
            "fetch": fetch,
            "ingest": _ingest.ingest_all,
            "export": _export.export,
        }
    )


def fetch(
    url: str = "https://github.com/ShipCreekGroup/alaska-legislative-data/raw/refs/heads/data_v1_latest/ak_leg.duckdb",
    dest: str = "ak_leg.duckdb",
):
    """Get the .duckdb file from the data_v1_latest branch."""
    p = Path(dest).absolute()
    p.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Downloading {url} to {p}")
    urllib.request.urlretrieve(url, p)


if __name__ == "__main__":
    main()
