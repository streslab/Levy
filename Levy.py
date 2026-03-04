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
        id TEXT PRIMARY KEY,
        account TEXT NOT NULL,
        symbol TEXT NOT NULL,
        side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL', 'SEND', 'RECEIVE')),
        quantity REAL NOT NULL,
        price REAL NOT NULL,
        timestamp DATETIME NOT NULL,
        disposition TEXT DEFAULT NULL,      -- NULL = pending review for SEND/RECEIVE
        disposition_notes TEXT DEFAULT NULL,
        transfer_id TEXT DEFAULT NULL       -- shared between matched SEND/RECEIVE pairs
        );""")
    
    # Tracks the 'lots' from each buy. As portions of the lot are
    # sold, the remaining_quantity decrements so that cost basis
    # can be accurately tracked.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS lots (
        id TEXT PRIMARY KEY,
        transaction_id TEXT NOT NULL,
        original_quantity REAL NOT NULL,
        remaining_quantity REAL NOT NULL,
        buy_price REAL NOT NULL,
        buy_timestamp DATETIME NOT NULL,
        FOREIGN KEY(transaction_id) REFERENCES transactions(id)
    );""")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS lot_disposals (
        id TEXT PRIMARY KEY,
        lot_id TEXT NOT NULL,
        sell_transaction_id TEXT NOT NULL,
        quantity_disposed REAL NOT NULL,
        sell_price REAL NOT NULL,
        sell_timestamp DATETIME NOT NULL,
        gain_loss REAL,
        is_internal_transfer INTEGER DEFAULT 0,
        FOREIGN KEY(lot_id) REFERENCES lots(id),
        FOREIGN KEY(sell_transaction_id) REFERENCES transactions(id)
    );""")

    conn.commit()
    conn.close()

SIDE_MAP = {
    "buy": "BUY",
    "advanced trade buy": "BUY",
    "sell": "SELL",
    "advanced trade sell": "SELL",
    "send": "SEND",
    "receive": "RECEIVE",
    "staking income": "RECEIVE",
    "pro withdrawal": "RECEIVE",
    "pro deposit": "SEND",
    "exchange deposit": "SEND",
    "deposit": "RECEIVE",
    "withdrawal": "SEND",
    "exchange withdrawal": "RECEIVE"
}
def normalize_side(raw_side: str) -> str:
    key = raw_side.strip().lower()
    if key not in SIDE_MAP:
        raise ValueError(f"Unrecognized transaction side: '{raw_side}'")
    return SIDE_MAP[key]


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

        account = "Coinbase"
        conn = sqlite3.connect(TRADING_DB)
        cursor = conn.cursor()

        for row in reader:
            print(row)
            side = normalize_side(row['Transaction Type'])
            id = row['ID']
            symbol = row['Asset']
            quantity = float(row['Quantity Transacted'])
            price = float(row['Price at Transaction'].replace('$', '').replace(',', ''))
            timestamp = datetime.strptime(row['Timestamp'],"%Y-%m-%d %H:%M:%S UTC")
            notes = " - ".join(filter(None, [row['Transaction Type'], row['Notes']]))

            cursor.execute("""
                INSERT OR IGNORE INTO transactions (id, account, symbol, side, quantity, price, timestamp, disposition_notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (id, account, symbol, side, quantity, price, timestamp, notes))
        conn.commit()
        conn.close()


def import_coinbasepro_transfers(filename):
    with open(filename,'r') as csvfile:
        reader = None
        while True:
            last_pos = csvfile.tell()
            line = csvfile.readline()
            if not line:
                break # EOF
            if "trade id" in line:
                csvfile.seek(last_pos)
                reader = csv.DictReader(csvfile)
                break

        account = "Coinbase Pro"
        conn = sqlite3.connect(TRADING_DB)
        cursor = conn.cursor()

        for row in reader:
            if row['type'] == "withdrawal" or row['type'] == "deposit":
                print(row)
                side = normalize_side(row['type'])
                id = row['transfer id']
                symbol = row['amount/balance unit']
                quantity = float(row['amount'])
                price = 0
                timestamp = datetime.strptime(row['time'].split('.')[0],"%Y-%m-%dT%H:%M:%S")
                transfer_id = row['transfer id']
                cursor.execute("""
                    INSERT OR IGNORE INTO transactions (id, account, symbol, side, quantity, price, timestamp, transfer_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (id, account, symbol, side, quantity, price, timestamp, transfer_id))
        conn.commit()
        conn.close()


def import_coinbasepro_transactions(filename):
    with open(filename,'r') as csvfile:
        reader = None
        while True:
            last_pos = csvfile.tell()
            line = csvfile.readline()
            if not line:
                break # EOF
            if "trade id" in line:
                csvfile.seek(last_pos)
                reader = csv.DictReader(csvfile)
                break

        account = "Coinbase Pro"
        conn = sqlite3.connect(TRADING_DB)
        cursor = conn.cursor()

        for row in reader:
            print(row)
            side = normalize_side(row['side'])
            id = row['trade id']
            symbol = row['size unit']
            quantity = float(row['size'])
            price = float(row['price'].replace('$', '').replace(',', ''))
            timestamp = datetime.strptime(row['created at'].split('.')[0],"%Y-%m-%dT%H:%M:%S")

            cursor.execute("""
                INSERT OR IGNORE INTO transactions (id, account, symbol, side, quantity, price, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (id, account, symbol, side, quantity, price, timestamp))
        conn.commit()
        conn.close()


def import_kraken_transactions(filename):
    with open(filename,'r') as csvfile:
        reader = None
        while True:
            last_pos = csvfile.tell()
            line = csvfile.readline()
            if not line:
                break # EOF
            if "amountusd" in line:
                csvfile.seek(last_pos)
                reader = csv.DictReader(csvfile)
                break

        account = "Kraken"
        conn = sqlite3.connect(TRADING_DB)
        cursor = conn.cursor()

        for row in reader:
            print(row)
            
            id = row['txid']
            quantity = float(row['amount'])
            if row['type'] == "trade" and quantity > 0:
                side = normalize_side("BUY")
            elif row['type'] == "trade" and quantity < 0:
                side = normalize_side("SELL")
            else:
                side = normalize_side(row['type']) 
            symbol = row['asset']
            
            price = float(row['amountusd'].replace('$', '').replace(',', '')) / quantity
            timestamp = datetime.strptime(row['time'],"%Y-%m-%d %H:%M:%S")

            cursor.execute("""
                INSERT OR IGNORE INTO transactions (id, account, symbol, side, quantity, price, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (id, account, symbol, side, quantity, price, timestamp))
        conn.commit()
        conn.close()


def import_etherscan_transactions(filename):
    pass


if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", help="Import transactions", action="store_true")
    parser.add_argument("-t", "--type", dest = "file_type", help="File format: (C)oinbase, (K)raken, (E)therscan)")
    parser.add_argument("-p", "--path", help="File path")
    
    args = parser.parse_args()
    init_db()
    if args.i:
        file_type = args.file_type
        if file_type.upper() == "C":
            import_coinbase_transactions(args.path)
        if file_type.upper() == "CP":
            import_coinbasepro_transactions(args.path)
        if file_type.upper() == "CPT":
            import_coinbasepro_transfers(args.path)
        if file_type.upper() == "K":
            import_kraken_transactions(args.path)

