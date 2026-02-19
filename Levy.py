import argparse
import csv
from datetime import datetime, timedelta
import time
import sys
import time
import sqlite3

TRADING_DB = 'trades.db'

def init_db():
    conn = sqlite3.connect(TRADING_DB)
    cursor = conn.cursor()

    # Tracks buy and sell transactions
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY,
        symbol TEXT,
        side TEXT,
        quantity REAL,
        price REAL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        );""")
    
    # Tracks the 'lots' from each buy. As portions of the lot are
    # sold, the remaining_quantity decrements so that cost basis
    # can be accurately tracked.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS lots (
        id INTEGER PRIMARY KEY,
        transaction_id INTEGER,
        symbol TEXT,
        original_quantity REAL,
        remaining_quantity REAL,
        buy_price REAL,
        buy_timestamp DATETIME,
        FOREIGN KEY(transaction_id) REFERENCES transactions(id)
    );""")

    conn.commit()
    conn.close()


def import_coinbase_transactions(filename):
    with open(filename,'r') as csvfile:
        reader = None
        while True:
            last_pos = csvfile.tell()
            line = csvfile.readline()
            if not line:
                break # EOF
            if "Price at Transaction" in line:
                csvfile.seek(last_pos)
                reader = csv.DictReader(csvfile)
                break

        print(reader.fieldnames)
        for row in reader:
            print(row)
            break

def import_kraken_transactions(filename):
    pass

def import_etherscan_transactions(filename):
    pass


if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", help="Import transactions", action="store_true")
    parser.add_argument("-t", "--type", dest = "file_type", help="File format: (C)oinbase, (K)raken, (E)therscan)")
    parser.add_argument("-p", "--path", help="File path")
    
    args = parser.parse_args()

    if args.i:
        file_type = args.file_type
        if file_type.upper() == "C":
            import_coinbase_transactions(args.path)

