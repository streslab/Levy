import csv
from datetime import datetime, timedelta
import time
import sys
import requests
import time


class Trade:
    def __init__(self, data):
        self.type = data[0]
        self.timestamp = data[1]
        self.amount = data[2]
        self.currency = data[4]
        self.usdvalue = 0


if __name__=="__main__":
    csvfile = input("CSV Path: ")

    with open(csvfile) as data:
        reader = csv.reader(data)
        next(reader)
        for line in reader:
            transaction = Trade(line)

            reqparams = {
                "start" : transaction.timestamp,
                "end" : (datetime.strptime(transaction.timestamp, "%Y-%m-%dT%H:%M:%S.%fZ") + timedelta(minutes=1)).isoformat(),
                "granularity" : "60",
            }

            time.sleep(.33)

            response = requests.get("https://api.gdax.com/products/%s-USD/candles" % transaction.currency, reqparams)
            if response:
                print(response.json()[0])