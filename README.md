# Alaska Legislative Data

Python and javascript code for scraping and reading the Alaska Legislature's API
for sessions, bills, members, votes, documents, etc.

This is composed of two subprojects:
1. A javascript/typescript library for fetching from the API with individual HTTP requests.
   This is in the [js](./js) folder.
2. A python library for bulk scraping, parsing, and cleaning the API.
   This is in the [python](./python/) folder.

We scrape and clean the data daily, and publish the results to the
[releases page](https://github.com/ShipCreekGroup/alaska-legislative-data/releases).

While SCG provides this data as a courtesy to the community, we also have some paid services
that may be of interest to you.
For example, we use this data to build a proprietary vote tracking service for the Alaska Legislature.
If you are a lobbyist, nonprofit, or someone who wants to be able to keep close tabs on
key votes, the voting history of representatives, and the progress of bills through
the legislature, please get in touch at https://shipcreekgroup.com.

If you have questions about this code or data, please get in touch with
n.crews@shipcreekgroup.com, or file an issue in the tab above.