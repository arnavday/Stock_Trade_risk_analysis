"""
reports/pnl_report.py
---------------------
Queries trades and breach_log from SQL, computes daily PnL per trader
per symbol, joins breach counts, and writes results to the pnl_report table.
Also exports a formatted CSV summary to reports/.

Usage:
    python -m reports.pnl_report
    python -m reports.pnl_report --date 2024-06-15
"""

import argparse
import os
from datetime import date

import pandas as pd
from sqlalchemy import text

from db.connection import engine

OUTPUT_DIR = "reports"
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ─────────────────────────────────────────────
# QUERY HELPERS
# ─────────────────────────────────────────────

def fetch_trades_for_pnl(trade_date: date) -> pd.DataFrame:
    query = text("""
        SELECT trader_id, symbol, trade_type, quantity, price, trade_value
        FROM   trades
        WHERE  DATE(timestamp) = :trade_date
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={"trade_date": str(trade_date)})


def fetch_breach_counts(trade_date: date) -> pd.DataFrame:
    """Query breach_log to get breach count per trader per symbol for the day."""
    query = text("""
        SELECT  b.trader_id,
                b.symbol,
                COUNT(*) AS breach_count
        FROM    breach_log b
        JOIN    trades t ON b.trade_id = t.trade_id
        WHERE   DATE(t.timestamp) = :trade_date
        GROUP   BY b.trader_id, b.symbol
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={"trade_date": str(trade_date)})


# ─────────────────────────────────────────────
# PnL COMPUTATION
# ─────────────────────────────────────────────

def compute_pnl(df: pd.DataFrame) -> pd.DataFrame:
    """
    Net PnL = total value of SELL trades - total value of BUY trades.
    Positive = net gain, Negative = net cost (open long position).
    """
    buys  = df[df["trade_type"] == "BUY"].groupby(["trader_id", "symbol"])["trade_value"].sum().rename("total_bought")
    sells = df[df["trade_type"] == "SELL"].groupby(["trader_id", "symbol"])["trade_value"].sum().rename("total_sold")
    count = df.groupby(["trader_id", "symbol"])["trade_value"].count().rename("trade_count")

    pnl = pd.concat([buys, sells, count], axis=1).fillna(0).reset_index()
    pnl["net_pnl"] = (pnl["total_sold"] - pnl["total_bought"]).round(2)
    return pnl


# ─────────────────────────────────────────────
# WRITE REPORT TO SQL + CSV
# ─────────────────────────────────────────────

def write_report(pnl: pd.DataFrame, trade_date: date):
    pnl["report_date"] = trade_date

    # Write to SQL pnl_report table
    pnl.to_sql("pnl_report", engine, if_exists="append", index=False, method="multi")
    print(f"  PnL report written to SQL for {trade_date} ({len(pnl):,} rows).")

    # Export CSV
    csv_path = os.path.join(OUTPUT_DIR, f"pnl_{trade_date}.csv")
    pnl.to_csv(csv_path, index=False)
    print(f"  CSV saved → {csv_path}")

    # Print summary
    print(f"\n  {'Trader':<10} {'Symbol':<12} {'Net PnL (Rs)':>15} {'Trades':>8} {'Breaches':>10}")
    print("  " + "-" * 60)
    for _, row in pnl.sort_values("net_pnl", ascending=False).head(15).iterrows():
        breaches = int(row.get("breach_count", 0))
        print(f"  {row['trader_id']:<10} {row['symbol']:<12} {row['net_pnl']:>15,.2f} "
              f"{int(row['trade_count']):>8} {breaches:>10}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def run(trade_date: date):
    print(f"\n{'='*50}")
    print(f"  PnL REPORT GENERATOR  |  {trade_date}")
    print(f"{'='*50}\n")

    trades   = fetch_trades_for_pnl(trade_date)
    breaches = fetch_breach_counts(trade_date)

    if trades.empty:
        print("  No trades found. Exiting.")
        return

    pnl = compute_pnl(trades)
    pnl = pnl.merge(breaches, on=["trader_id", "symbol"], how="left")
    pnl["breach_count"] = pnl["breach_count"].fillna(0).astype(int)

    write_report(pnl, trade_date)
    print("\nReport complete.\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=str(date.today()))
    args = parser.parse_args()
    run(date.fromisoformat(args.date))
