import sys

from alaska_legislative_api import _batch

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python -m alaska_legislative_api._batch BRANCH")
        sys.exit(1)
    _batch.scrape_and_push(sys.argv[1])
