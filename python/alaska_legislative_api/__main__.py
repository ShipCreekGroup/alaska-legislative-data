import logging
import sys

from alaska_legislative_api import _batch


def main():
    if len(sys.argv) != 2:
        print("Usage: python -m alaska_legislative_api BRANCH")
        sys.exit(1)
    logging.basicConfig(level=logging.DEBUG)
    _batch.scrape_and_push(sys.argv[1])


if __name__ == "__main__":
    main()
