import logging

import dotenv
import fire

from alaska_legislative_data import _export, _ingest

logger = logging.getLogger(__name__)


def main():
    dotenv.load_dotenv(dotenv.find_dotenv(".env"))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    # Set log level for other libraries to a higher level to suppress their logs
    for logger_name in logging.root.manager.loggerDict:
        if not logger_name.startswith("alaska_legislative_data"):
            logging.getLogger(logger_name).setLevel(logging.WARNING)
    fire.Fire(
        {
            "ingest": _ingest.ingest_all,
            "export": _export.export,
        }
    )


if __name__ == "__main__":
    main()
