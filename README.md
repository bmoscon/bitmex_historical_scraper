# BITMEX Historical Data Scraper

Bitmex no longer offers historical trade data on their REST API. They do have the data in a public AWS bucket, which this scrapes and converts to CSV files (by year).


### Installation
1. Clone/download repository
2. Install requirements: `pip install -r requirements.txt`


### Usage
`python scrape.py` or `python scrape.py YYYYMMDD` to specify a start date, where YYYY is the year, MM is the month and DD is the day. If no date is supplied, will scrape all available data. 
