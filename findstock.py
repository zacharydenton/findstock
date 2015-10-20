#!/usr/bin/env python
import csv
import glob
import decimal
import datetime
import pytz
import yahoo_finance
import sqlalchemy as sa

EST = pytz.timezone("America/New_York")

engine = sa.create_engine("postgresql+psycopg2cffi://findstock_dev@localhost/findstock_dev", echo=True)
metadata = sa.MetaData()
symbols = sa.Table("symbols", metadata,
                   sa.Column("id", sa.Integer, primary_key=True),
                   sa.Column("symbol", sa.String, unique=True, index=True, nullable=False),
                   sa.Column("name", sa.String))
prices = sa.Table("prices", metadata,
                  sa.Column("id", sa.Integer, primary_key=True),
                  sa.Column("symbol_id", None, sa.ForeignKey("symbols.id"), index=True, nullable=False),
                  sa.Column("time", sa.DateTime(), nullable=False),
                  sa.Column("price", sa.Numeric(asdecimal=True), nullable=False),
                  sa.Index("ix_prices_symbol_time", "symbol_id", "time", unique=True),
                  sa.Index("ix_prices_symbol_price", "symbol_id", "price"),
                  )
volumes = sa.Table("volumes", metadata,
                   sa.Column("id", sa.Integer, primary_key=True),
                   sa.Column("symbol_id", None, sa.ForeignKey("symbols.id"), index=True, nullable=False),
                   sa.Column("time", sa.DateTime(), nullable=False),
                   sa.Column("volume", sa.Integer, nullable=False),
                   sa.Index("ix_volumes_symbol_time", "symbol_id", "time", unique=True),
                   sa.Index("ix_volumes_symbol_volume", "symbol_id", "volume"),
                   )
metadata.create_all(engine)


def est_to_utc(datetime):
    return EST.localize(datetime, is_dst=None).astimezone(pytz.utc).replace(tzinfo=None)


def open_time(date):
    return est_to_utc(datetime.datetime.combine(date, datetime.time(9, 30)))


def close_time(date):
    return est_to_utc(datetime.datetime.combine(date, datetime.time(16)))


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


def extract_all_history_data(conn):
    # http://cs.brown.edu/~pavlo/stocks/history.tar.gz
    query = sa.select([symbols.c.id, symbols.c.symbol])
    for row in conn.execute(query):
        try:
            extract_history_data(conn, row[0], row[1])
        except IOError:
            pass


def extract_history_data(conn, symbol_id, symbol):
    query = sa.select([sa.func.count(volumes.c.id)]).where(volumes.c.symbol_id == symbol_id)
    count = conn.execute(query).fetchone()[0]
    if count > 0:
        return

    with open("data/history/{}.csv".format(symbol)) as f:
        # Skip comments
        for line in f:
            if not line.startswith("#"):
                break
        reader = csv.reader(f)
        price_inserts = []
        volume_inserts = []
        for row in reader:
            date = datetime.datetime.strptime(row[0], "%Y-%m-%d").date()
            close_price = decimal.Decimal(row[1])
            volume = int(row[2])
            price_inserts.append({
                "symbol_id": symbol_id,
                "time": close_time(date),
                "price": close_price
            })
            volume_inserts.append({
                "symbol_id": symbol_id,
                "time": close_time(date),
                "volume": volume
            })
        conn.execute(prices.insert(), price_inserts)
        conn.execute(volumes.insert(), volume_inserts)


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
        open_price = decimal.Decimal(point["Open"])
        close_price = decimal.Decimal(point["Close"])
        inserts.append({
            "symbol_id": symbol_id,
            "time": open_time(date),
            "price": open_price
        })
        inserts.append({
            "symbol_id": symbol_id,
            "time": close_time(date),
            "price": close_price
        })
    conn.execute(prices.insert(), inserts)


def main():
    conn = engine.connect()
    insert_all_symbols(conn)
    extract_all_history_data(conn)
    # download_all_historical_data(conn)

if __name__ == "__main__":
    main()
