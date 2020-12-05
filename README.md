# govinfo-tools

This repo contains several useful tools to fetch data from www.govinfo.gov.

## ginfo

`ginfo` is a topic-wise search-and-download crawler for any court opinion document available under advanced search feature of www.govinfo.gov. You can choose court opinions with a *nature of suit* and the resulting search will
only yield orders/opinions within the chosen scope.

The most important advantage of this crawler is that it is indeed a backdoor method to get data and therefore is not restricted by govinfo's API limitations. For example, API calls to govinfo's server has to
be limited to 1000 per hour whereas you can make as many as 10000 calls per minute using this crawler.

A future version will include a generic search capability that is not limited by the nature of suit.
