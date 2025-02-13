# Alaska Legislative API

A typed javascript library for interacting with the Alaska Legislative API.
SCG doesn't use this actively, use at your own risk.
This was our first attempt at accessing the API.
Now, we rely more on the python library to scrape the API in bulk, and then
we access the cleaned data directly.

Will Muldoon's playground for working with this API: https://akleg.willmuldoon.com/

This isn't documented at all, and the API might change between releases.
But, this is pretty much typed correctly, so your IDE should give you
hints if you are using the API incorrectly.

You can copy-paste the `index.js` if that suits your needs, or install in a bundled
environment with `pnpm add alaska-legislative-api`

See [examples](examples/) for (messy, incomplete) examples.