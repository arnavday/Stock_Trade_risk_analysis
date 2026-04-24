"""
db/seed.py
----------
Generates and inserts 10,000+ realistic equity trades into the trades table.
Run once to populate the database before running the risk engine.

Usage:
    python -m db.seed
"""

import random
import uuid
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import text
from db.connection import engine, init_db

# ── Config ──
SYMBOLS  = ["RELIANCE", "HDFCBANK", "INFY", "TCS", "ICICIBANK",
            "WIPRO", "AXISBANK", "SBIN", "ITC", "BAJFINANCE"]
TRADERS  = [f"TR{str(i).zfill(3)}" for i in range(1, 21)]   # 20 traders
N_TRADES = 10_500
START_DT = datetime(2024, 1, 1, 9, 15)
END_DT   = datetime(2024, 12, 31, 15, 30)

# Approximate price ranges per symbol
PRICE_MAP = {
    "RELIANCE": (2200, 3100), "HDFCBANK": (1400, 1800), "INFY": (1300, 1900),
    "TCS":      (3500, 4400), "ICICIBANK": (900, 1300),  "WIPRO": (400, 600),
    "AXISBANK": (950, 1250),  "SBIN": (550, 850),        "ITC":   (380, 500),
    "BAJFINANCE": (6500, 8000),
}

random.seed(42)


def random_timestamp(start: datetime, end: datetime) -> datetime:
    delta = end - start
    secs  = random.randint(0, int(delta.total_seconds()))
    ts    = start + timedelta(seconds=secs)
    # Keep within market hours (9:15 – 15:30)
    ts    = ts.replace(hour=random.randint(9, 15),
                       minute=random.randint(0, 59) if ts.hour < 15 else random.randint(0, 30))
    return ts


def generate_trades(n: int) -> pd.DataFrame:
    rows = []
    for _ in range(n):
        symbol     = random.choice(SYMBOLS)
        lo, hi     = PRICE_MAP[symbol]
        price      = round(random.uniform(lo, hi), 2)
        quantity   = random.choice([50, 100, 200, 500, 1000, 2000])
        trade_type = random.choice(["BUY", "SELL"])
        rows.append({
            "trade_id"   : f"TRD-{uuid.uuid4().hex[:10].upper()}",
            "symbol"     : symbol,
            "trader_id"  : random.choice(TRADERS),
            "trade_type" : trade_type,
            "quantity"   : quantity,
            "price"      : price,
            "trade_value": round(price * quantity, 2),
            "timestamp"  : random_timestamp(START_DT, END_DT),
        })
    return pd.DataFrame(rows)


def seed():
    print("Initialising database...")
    init_db()

    print(f"Generating {N_TRADES:,} trades...")
    df = generate_trades(N_TRADES)

    print("Inserting trades into SQL...")
    df.to_sql("trades", engine, if_exists="append", index=False, method="multi", chunksize=500)
    print(f"  Done. {len(df):,} trades inserted.")


if __name__ == "__main__":
    seed()
