# Alaska Legislative Data - Python Library

Python library/scripts for bulk scraping the API.

## to develop:

Get access to motherduck, where we house the data.
Talk to Nick.
Store this in a `.env` file in the root of this repo, as `motherduck_token=...`.
python notebooks should pick this up automatically,
but python scripts probably will need to load this using the `dotenv` package.
See `__main__.py` for an example.

From inside this dir: `uv sync` to install all the deps.