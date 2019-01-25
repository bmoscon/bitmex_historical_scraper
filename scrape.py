from datetime import datetime as dt
from datetime import timedelta
import gzip
import glob
import os
import shutil
import sys
import time

import requests


# https://public.bitmex.com/?prefix=data/trade/
endpoint = 'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/{}.csv.gz'


def scrape(year, date):
    end_date = min(dt(year, 12, 31), dt.today() - timedelta(days=1))

    while date <= end_date:
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
    with open("{}.csv".format(year), 'wb') as out:
        first = True
        for f in files:
            with open(f, 'rb') as fp:
                if first:
                    fp.readline()
                    first = False
                shutil.copyfileobj(fp, out)
    for f in files:
        os.unlink(f)


if __name__ == '__main__':
    if len(sys.argv) == 2:
        # if arg is supplied must be in format YYYYMMDD
        # will attempt to remove that file, if exists
        # in case data is incomplete
        year = int(sys.argv[1][:4])
        month = int(sys.argv[1][4:6])
        day = int(sys.argv[1][6:])
        start = dt(year, month, day)
        years = list(range(year, dt.now().year + 1))

        try:
            os.unlink(sys.argv[1])
        except FileNotFoundError:
            pass

    else:
        # 2014-11-12 is the first day of data
        start = dt(2014, 11, 22)
        years = list(range(2014, dt.now().year + 1))

    starts = [dt(year, 1, 1) for year in years]
    starts[0] = start

    for year, start in zip(years, starts):
        scrape(year, start)
        merge(year)
