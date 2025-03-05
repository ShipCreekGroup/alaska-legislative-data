import logging

import fire

from alaska_legislative_data import _export, _ingest


def main():
    logging.basicConfig(level=logging.INFO)
    fire.Fire(
        {
            "ingest": _ingest.ingest_all,
            "export": _export.export,
            "push": _export.push,
        }
    )


if __name__ == "__main__":
    main()
