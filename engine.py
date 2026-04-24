"""
engine.py
---------
Core rule engine. Queries trades from SQL, applies three pre-trade risk
frameworks, and writes all breaches back to the breach_log table.

Rules enforced:
  1. Position limit    — max net quantity per symbol per trader (default: 5,000)
  2. Exposure cap      — max trade value per single trade (default: Rs 50,00,000)
  3. Trade frequency   — max trades per trader per hour (default: 10)

Usage:
    python engine.py
    python engine.py --date 2024-06-15
"""

import argparse
from datetime import datetime, date

import pandas as pd
from sqlalchemy import text

from db.connection import get_session, engine

# ─────────────────────────────────────────────
# RISK THRESHOLDS
# ─────────────────────────────────────────────

POSITION_LIMIT    = 5_000      # max net units per symbol per trader
EXPOSURE_CAP      = 5_000_000  # max single trade value (Rs)
FREQUENCY_LIMIT   = 10         # max trades per trader per hour


# ─────────────────────────────────────────────
# FETCH TRADES FROM SQL
# ─────────────────────────────────────────────

def fetch_trades(trade_date: date) -> pd.DataFrame:
    """Query all trades for a given date from the SQL trades table."""
    query = text("""
        SELECT trade_id, symbol, trader_id, trade_type,
               quantity, price, trade_value, timestamp
        FROM   trades
        WHERE  DATE(timestamp) = :trade_date
        ORDER  BY timestamp ASC
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"trade_date": str(trade_date)})
    print(f"  Fetched {len(df):,} trades for {trade_date} from SQL.")
    return df


# ─────────────────────────────────────────────
# RULE 1 — POSITION LIMIT
# ─────────────────────────────────────────────

def check_position_limits(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flag traders whose net position in any symbol exceeds POSITION_LIMIT.
    Net position = total BUY quantity - total SELL quantity.
    """
    df = df.copy()
    df["signed_qty"] = df.apply(
        lambda r: r["quantity"] if r["trade_type"] == "BUY" else -r["quantity"], axis=1
    )
    net = (
        df.groupby(["trader_id", "symbol"])["signed_qty"]
        .sum()
        .reset_index()
        .rename(columns={"signed_qty": "net_position"})
    )
    breaches = net[net["net_position"].abs() > POSITION_LIMIT].copy()
    breaches["breach_type"]  = "position_limit"
    breaches["breach_value"] = breaches["net_position"].abs()
    breaches["threshold"]    = POSITION_LIMIT

    # Map back to latest trade_id per trader+symbol
    latest = df.sort_values("timestamp").groupby(["trader_id", "symbol"])["trade_id"].last()
    breaches = breaches.join(latest, on=["trader_id", "symbol"])
    return breaches[["trade_id", "symbol", "trader_id", "breach_type", "breach_value", "threshold"]]


# ─────────────────────────────────────────────
# RULE 2 — EXPOSURE CAP
# ─────────────────────────────────────────────

def check_exposure_caps(df: pd.DataFrame) -> pd.DataFrame:
    """Flag individual trades whose trade_value exceeds EXPOSURE_CAP."""
    breaches = df[df["trade_value"] > EXPOSURE_CAP].copy()
    breaches["breach_type"]  = "exposure_cap"
    breaches["breach_value"] = breaches["trade_value"]
    breaches["threshold"]    = EXPOSURE_CAP
    return breaches[["trade_id", "symbol", "trader_id", "breach_type", "breach_value", "threshold"]]


# ─────────────────────────────────────────────
# RULE 3 — TRADE FREQUENCY
# ─────────────────────────────────────────────

def check_trade_frequency(df: pd.DataFrame) -> pd.DataFrame:
    """Flag traders who exceed FREQUENCY_LIMIT trades within any rolling 60-min window."""
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.sort_values("timestamp", inplace=True)

    breach_rows = []
    for trader, group in df.groupby("trader_id"):
        group = group.set_index("timestamp").sort_index()
        # Rolling count over 60-minute window
        rolling_counts = group["trade_id"].rolling("60min").count()
        over_limit     = rolling_counts[rolling_counts > FREQUENCY_LIMIT]
        if not over_limit.empty:
            worst_ts  = over_limit.idxmax()
            trade_row = group.loc[worst_ts]
            breach_rows.append({
                "trade_id"   : trade_row["trade_id"],
                "symbol"     : trade_row["symbol"],
                "trader_id"  : trader,
                "breach_type": "trade_frequency",
                "breach_value": over_limit.max(),
                "threshold"  : FREQUENCY_LIMIT,
            })
    return pd.DataFrame(breach_rows) if breach_rows else pd.DataFrame()


# ─────────────────────────────────────────────
# WRITE BREACHES TO SQL
# ─────────────────────────────────────────────

def write_breaches(breaches: pd.DataFrame):
    """Insert all flagged breaches into the breach_log SQL table."""
    if breaches.empty:
        print("  No breaches detected.")
        return
    breaches.to_sql("breach_log", engine, if_exists="append", index=False, method="multi")
    print(f"  {len(breaches):,} breaches written to SQL breach_log.")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def run(trade_date: date):
    print(f"\n{'='*50}")
    print(f"  TRADE RISK ENGINE  |  {trade_date}")
    print(f"{'='*50}")

    df = fetch_trades(trade_date)
    if df.empty:
        print("  No trades found for this date. Exiting.")
        return

    print("\nRunning risk frameworks...")
    pos_breaches  = check_position_limits(df)
    exp_breaches  = check_exposure_caps(df)
    freq_breaches = check_trade_frequency(df)

    all_breaches = pd.concat(
        [b for b in [pos_breaches, exp_breaches, freq_breaches] if not b.empty],
        ignore_index=True
    )

    print(f"  Position limit breaches  : {len(pos_breaches):,}")
    print(f"  Exposure cap breaches    : {len(exp_breaches):,}")
    print(f"  Trade frequency breaches : {len(freq_breaches):,}")
    print(f"  Total breaches flagged   : {len(all_breaches):,}")

    write_breaches(all_breaches)
    print("\nRisk engine complete.\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=str(date.today()),
                        help="Trade date to process (YYYY-MM-DD)")
    args = parser.parse_args()
    run(date.fromisoformat(args.date))
