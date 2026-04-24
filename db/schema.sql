-- ─────────────────────────────────────────────
-- Trade Risk Engine — Database Schema
-- ─────────────────────────────────────────────

-- Raw trades ingested from source
CREATE TABLE IF NOT EXISTS trades (
    trade_id        VARCHAR(20)    PRIMARY KEY,
    symbol          VARCHAR(20)    NOT NULL,
    trader_id       VARCHAR(20)    NOT NULL,
    trade_type      VARCHAR(4)     NOT NULL CHECK (trade_type IN ('BUY', 'SELL')),
    quantity        INTEGER        NOT NULL,
    price           NUMERIC(12,2)  NOT NULL,
    trade_value     NUMERIC(16,2)  NOT NULL,
    timestamp       TIMESTAMP      NOT NULL,
    created_at      TIMESTAMP      DEFAULT CURRENT_TIMESTAMP
);

-- Breach records written by the rule engine
CREATE TABLE IF NOT EXISTS breach_log (
    breach_id       SERIAL         PRIMARY KEY,
    trade_id        VARCHAR(20)    REFERENCES trades(trade_id),
    symbol          VARCHAR(20)    NOT NULL,
    trader_id       VARCHAR(20)    NOT NULL,
    breach_type     VARCHAR(30)    NOT NULL,   -- position_limit | exposure_cap | trade_frequency
    breach_value    NUMERIC(16,2),             -- the value that triggered the breach
    threshold       NUMERIC(16,2),             -- the limit that was exceeded
    flagged_at      TIMESTAMP      DEFAULT CURRENT_TIMESTAMP
);

-- Daily PnL per trader, written by the report generator
CREATE TABLE IF NOT EXISTS pnl_report (
    report_id       SERIAL         PRIMARY KEY,
    report_date     DATE           NOT NULL,
    trader_id       VARCHAR(20)    NOT NULL,
    symbol          VARCHAR(20)    NOT NULL,
    total_bought    NUMERIC(16,2),
    total_sold      NUMERIC(16,2),
    net_pnl         NUMERIC(16,2),
    trade_count     INTEGER,
    breach_count    INTEGER,
    generated_at    TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (report_date, trader_id, symbol)
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_trades_symbol    ON trades(symbol);
CREATE INDEX IF NOT EXISTS idx_trades_trader    ON trades(trader_id);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
CREATE INDEX IF NOT EXISTS idx_breach_trader    ON breach_log(trader_id);
CREATE INDEX IF NOT EXISTS idx_breach_type      ON breach_log(breach_type);
