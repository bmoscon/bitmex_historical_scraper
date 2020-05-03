from datetime import datetime as dt
from datetime import timedelta
import argparse
import gzip
import glob
import os
import shutil
import time

import requests


# https://public.bitmex.com/?prefix=data/trade/
endpoint = 'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/{}.csv.gz'


def scrape(year, date, end):
    end_date = min(dt(year, 12, 31), dt.today() - timedelta(days=1))

    while date <= end_date and date <= end:
        date_str = date.strftime('%Y%m%d')
        print("Processing {}...".format(date))
        count = 0
        while True:
            r = requests.get(endpoint.format(date_str))
            if r.status_code == 200:
                break
            else:
                count += 1
                if count == 10:
                    r.raise_for_status()
                print("Error processing {} - {}, trying again".format(date, r.status_code))
                time.sleep(10)


        with open(date_str, 'wb') as fp:
            fp.write(r.content)

        with gzip.open(date_str, 'rb') as fp:
            data = fp.read()

        with open(date_str, 'wb') as fp:
            fp.write(data)

        date += timedelta(days=1)


def merge(year):
    print("Generating CSV for {}".format(year))
    files = sorted(glob.glob("{}*".format(year)))
    first = True
    with open("{}.csv".format(year), 'wb') as out:
        for f in files:
            with open(f, 'rb') as fp:
                if first is False:
                    fp.readline()
                first = False
                shutil.copyfileobj(fp, out)
    for f in files:
        os.unlink(f)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='BitMex historical data scraper. Scrapes files into single year CSVs')
    parser.add_argument('--start', default="20141122", help='start date, in YYYYMMDD format. Default is 2014-11-22, the earliest data date for BitMex')
    parser.add_argument('--end', default=None, help='end date, in YYYYMMDD format. Default is yesterday')
    args = parser.parse_args()

    start = dt.strptime(args.start, '%Y%m%d')
    end = dt.strptime(args.end, '%Y%m%d') if args.end else dt.utcnow()

    years = list(range(start.year, end.year + 1))

    starts = [dt(year, 1, 1) for year in years]
    starts[0] = start

    for year, start in zip(years, starts):
        scrape(year, start, end)
        merge(year)
