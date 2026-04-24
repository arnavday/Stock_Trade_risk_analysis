# Trade Risk Engine

Python-based rule engine that queries equity trades from a SQL database, enforces pre-trade risk frameworks across position limits, exposure caps, and trade frequency thresholds, and generates automated PnL reports.

---

## Project Structure

```
trade_risk_engine/
├── db/
│   ├── schema.sql          # SQL table definitions (trades, breach_log, pnl_report)
│   ├── connection.py       # SQLAlchemy engine and session factory
│   └── seed.py             # Generates and inserts 10,000+ sample trades
├── reports/
│   └── pnl_report.py       # Queries SQL to compute and export daily PnL reports
├── engine.py               # Core rule engine — runs all three risk frameworks
├── requirements.txt
├── .env.example
└── README.md
```

---

## Setup

### 1. Database

Requires PostgreSQL. Create a database:

```sql
CREATE DATABASE trade_risk;
```

### 2. Environment

```bash
cp .env.example .env
# Edit .env with your database credentials
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Initialise and seed

```bash
python -m db.seed
```

---

## Usage

**Run the risk engine for today:**
```bash
python engine.py
```

**Run for a specific date:**
```bash
python engine.py --date 2024-06-15
```

**Generate PnL report:**
```bash
python -m reports.pnl_report --date 2024-06-15
```

---

## Risk Frameworks

| Rule | Threshold | Breach type |
|---|---|---|
| Position limit | 5,000 net units per symbol per trader | `position_limit` |
| Exposure cap | Rs 50,00,000 per single trade | `exposure_cap` |
| Trade frequency | 10 trades per trader per 60-minute window | `trade_frequency` |

All thresholds are configurable in `engine.py`.

---

## SQL Tables

- `trades` — raw equity trade records
- `breach_log` — flagged risk breaches written by the engine
- `pnl_report` — daily PnL per trader per symbol with breach counts

---

## Stack

Python · Pandas · NumPy · SQLAlchemy · PostgreSQL · psycopg2
