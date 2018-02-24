import csv
from datetime import datetime, timedelta
import time
import sys
import requests
import time

GDAX_RATE_DELAY = .33


class Trade:
    def __init__(self, data):
        self.type = data[0]
        self.timestamp = data[1]
        self.amount = float(data[2])
        self.currency = data[4]
        self.usdvalue = 0.0

    def calcUSDValue(self, apidata):
        median = (apidata[3] - apidata[4])/2 + apidata[4]
        self.usdvalue = median * float(self.amount)

    def printTrade(self):
        print("{0:<24s}, {1:<5s}, {2: 2.8f}, {3:<3s}, {4:#04.2f}, USD".format(self.timestamp, self.type, self.amount, self.currency, self.usdvalue))


if __name__=="__main__":
    csvfile = input("CSV Path: ")
    csvtype = input("CSV Type: ")

    tradearray = []

    with open(csvfile) as data:
        reader = csv.reader(data)
        next(reader)
        for line in reader:
            if(csvtype == "GDAX"):
                transaction = Trade(line)
            else:

                transaction = Trade(["mine", datetime.utcfromtimestamp(float(line[2])).isoformat() + ".000Z", line[7], "","ETH"])

            if len(tradearray) > 0:
                prevtransaction = tradearray[len(tradearray) - 1]
                if transaction.timestamp == prevtransaction.timestamp:
                        prevtransaction.amount += transaction.amount
                        continue
                prevtransaction.printTrade()

            reqparams = {
                "start" : transaction.timestamp,
                "end" : (datetime.strptime(transaction.timestamp, "%Y-%m-%dT%H:%M:%S.%fZ") + timedelta(minutes=1)).isoformat(),
                "granularity" : "60",
            }

            time.sleep(GDAX_RATE_DELAY)

            response = requests.get("https://api.gdax.com/products/%s-USD/candles" % transaction.currency, reqparams)
            if response:
                transaction.calcUSDValue(response.json()[0])
            tradearray.append(transaction)

        for item in tradearray:
            item.printTrade()