"""
db/connection.py
----------------
Centralised SQLAlchemy engine and session factory.
All other modules import from here — never create engines elsewhere.
"""

import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# ── Read connection string from environment (set in .env) ──
DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:password@localhost:5432/trade_risk"
)

engine       = create_engine(DB_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)


def get_session():
    """Return a new database session. Caller is responsible for closing."""
    return SessionLocal()


def init_db():
    """Run schema.sql to create tables if they don't exist."""
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path, "r") as f:
        sql = f.read()
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    print("  Database initialised.")
