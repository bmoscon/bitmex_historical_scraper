# BITMEX Historical Data Scraper

Bitmex no longer offers historical trade data on their REST API. They do have the data in a public AWS bucket, which this scrapes and converts to CSV files (by year).


### Installation
1. Clone/download repository
2. Install requirements: `pip install -r requirements.txt`


### Usage
* `python scrape.py` - Scrape all available data
* `python scrape.py --start YYYYMMDD` - Scrape data from start date through yesterday
* `python scrape.py --start YYYYMMDD --end YYYYMMDD` - Scrape data from start date through end date (inclusive)
* `python scrape.py --end YYYYMMDD` - Scrape data from start of data through end date
