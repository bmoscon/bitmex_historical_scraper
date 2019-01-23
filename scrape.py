from datetime import datetime as dt
from datetime import timedelta
import gzip
import glob
import os

import requests
import pandas as pd


# https://public.bitmex.com/?prefix=data/trade/
endpoint = 'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/{}.csv.gz'


def scrape(year):
    if year == 2014:
        # First date available
        date = dt(2014, 11, 22)
    else:
        date = dt(year, 1, 1)
    
    end_date = min(dt(year, 12, 31), dt.today() - timedelta(days=1))
    

    while date <= end_date:
        date_str = date.strftime('%Y%m%d')
        print("Processing {}...".format(date))
        r = requests.get(endpoint.format(date_str))
        r.raise_for_status()
        with open(date_str, 'wb') as fp:
            fp.write(r.content)

        with gzip.open(date_str, 'rb') as fp:
            data = fp.read()

        with open(date_str, 'wb') as fp:
            fp.write(data)

        date += timedelta(days=1)


def merge(year):
    print("Generating CSV for {}".format(year))
    files = glob.glob("{}*".format(year))
    combined_csv = pd.concat([pd.read_csv(f) for f in files])
    combined_csv.to_csv("{}.csv".format(year), index=False)
    for f in files:
        os.unlink(f)

if __name__ == '__main__':
    years = [2014, 2015, 2016, 2017, 2018, 2019]
    for year in years:
        scrape(year)
        merge(year)