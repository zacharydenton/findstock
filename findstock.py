#!/usr/bin/env python
import csv
import glob
import sqlalchemy as sa


engine = sa.create_engine("sqlite:///findstock.db", echo=True)
metadata = sa.MetaData()
symbols = sa.Table("symbols", metadata,
                   sa.Column("id", sa.Integer, primary_key=True),
                   sa.Column("symbol", sa.String, unique=True, index=True),
                   sa.Column("name", sa.String))
metadata.create_all(engine)


def parse_stock_lists():
    lists = glob.glob("data/*list*.txt")
    for filename in lists:
        with open(filename) as f:
            reader = csv.reader(f, delimiter='|')
            for row in reader:
                yield row[:2]


def main():
    conn = engine.connect()
    stocks = dict(parse_stock_lists())
    inserts = []
    for symbol, name in stocks.items():
        inserts.append({"symbol": symbol, "name": name})
    conn.execute(symbols.insert(), inserts)

if __name__ == "__main__":
    main()
