# ScrapeGoogleFinance

This script scrapes Google Finance (google.com/finance/getprices) for NASDAQ price for minute-by-minute quotes. The data is then organized by stock and by day.

Since minute-by-minute data can only be collected for 15 business days, you need to run the script about every 20 days if looking to create a database of historical minute-by-minute data.

A couple of notes: Google blocks you from its servers after about 1000 requests, so for the list of 3000(ish) ticker prices, you must run the script 3 times incrementing the variable "start" by 1000 every time.
