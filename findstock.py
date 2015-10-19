#!/usr/bin/env python
import csv
import glob
import decimal
import datetime
import pytz
import yahoo_finance
import sqlalchemy as sa

EST = pytz.timezone("America/New_York")

engine = sa.create_engine("postgresql+psycopg2://findstock_dev@localhost/findstock_dev", echo=True)
metadata = sa.MetaData()
symbols = sa.Table("symbols", metadata,
                   sa.Column("id", sa.Integer, primary_key=True),
                   sa.Column("symbol", sa.String, unique=True, index=True, nullable=False),
                   sa.Column("name", sa.String))
prices = sa.Table("prices", metadata,
                  sa.Column("id", sa.Integer, primary_key=True),
                  sa.Column("symbol_id", None, sa.ForeignKey("symbols.id"), index=True, nullable=False),
                  sa.Column("time", sa.DateTime(timezone=True), index=True, nullable=False),
                  sa.Column("price", sa.Numeric(asdecimal=True), nullable=False))
metadata.create_all(engine)


def parse_stock_lists():
    lists = glob.glob("data/*list*.txt")
    for filename in lists:
        with open(filename) as f:
            reader = csv.reader(f, delimiter='|')
            for row in reader:
                yield row[:2]


def insert_all_symbols(conn):
    query = sa.select([sa.func.count(symbols.c.id)])
    count = conn.execute(query).fetchone()[0]
    if count > 0:
        return

    stocks = dict(parse_stock_lists())
    inserts = []
    for symbol, name in stocks.items():
        inserts.append({"symbol": symbol, "name": name})
    conn.execute(symbols.insert(), inserts)


def download_all_historical_data(conn):
    query = sa.select([symbols.c.id, symbols.c.symbol])
    for row in conn.execute(query):
        download_historical_data(conn, row[0], row[1])


def download_historical_data(conn, symbol_id, symbol):
    query = sa.select([sa.func.count(prices.c.id)]).where(prices.c.symbol_id == symbol_id)
    count = conn.execute(query).fetchone()[0]
    if count > 0:
        return

    try:
        stock = yahoo_finance.Share(symbol)
    except:
        return

    info = stock.get_info()
    if 'start' not in info:
        print(info)
        return

    try:
        history = stock.get_historical(info['start'], info['end'])
    except ValueError:
        start_date = datetime.date(int(info['start'].split('-')[0]), 1, 1).strftime("%Y-%m-%d")
        end_date = datetime.date.today().strftime("%Y-%m-%d")
        history = stock.get_historical(start_date, end_date)

    inserts = []
    for point in history:
        date = datetime.datetime.strptime(point["Date"], "%Y-%m-%d").date()
        open_time = datetime.datetime.combine(date, datetime.time(9, 30, tzinfo=EST))
        close_time = datetime.datetime.combine(date, datetime.time(16, tzinfo=EST))
        open_price = decimal.Decimal(point["Open"])
        close_price = decimal.Decimal(point["Close"])
        inserts.append({
            "symbol_id": symbol_id,
            "time": open_time,
            "price": open_price
        })
        inserts.append({
            "symbol_id": symbol_id,
            "time": close_time,
            "price": close_price
        })
    conn.execute(prices.insert(), inserts)


def main():
    conn = engine.connect()
    insert_all_symbols(conn)
    download_all_historical_data(conn)

if __name__ == "__main__":
    main()
