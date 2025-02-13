# Alaska Legislative Data

Python and javascript code for scraping and reading the Alaska Legislature's API
for sessions, bills, members, votes, documents, etc.

This is composed of two subprojects:
1. A javascript/typescript library for fetching from the API with individual HTTP requests.
   This is in the [js](./js) folder.
2. A python library for bulk scraping, parsing, and cleaning the API.
   This is in the [python](./python/) folder.

We scrape and clean the data daily, and publish the results to the [`data_*` branches](https://github.com/ShipCreekGroup/alaska-legislative-data/branches/all?query=data&lastTab=overview).
For example, you can download the csv's directly from
`https://github.com/ShipCreekGroup/alaska-legislative-data/raw/refs/heads/data_v1_latest/votes.csv`.
The `v1` means it is v1 of the schema.
In the future, we may make changes to the format of the data, and we will increment
the version number for this.
The `latest` means it is the latest version of that schema.
In google sheets, you can use `IMPORTDATA('https://github.com/ShipCreekGroup/alaska-legislative-data/raw/refs/heads/data_v1_latest/votes.csv')` to get a daily-updated view of all the votes in the
alaska legislature.