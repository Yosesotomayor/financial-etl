CREATE SCHEMA IF NOT EXISTS raw;

SET search_path TO raw;

DROP TABLE IF EXISTS daily_prices;
DROP TABLE IF EXISTS assets;

CREATE TABLE assets (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) UNIQUE NOT NULL,
    name VARCHAR(255),
    asset_type VARCHAR(50) DEFAULT 'stock',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE daily_prices (
    id SERIAL PRIMARY KEY,
    asset_id INTEGER REFERENCES assets(id),
    price_date DATE NOT NULL,
    open_price DECIMAL(18, 6),
    high_price DECIMAL(18, 6),
    low_price DECIMAL(18, 6),
    close_price DECIMAL(18, 6),
    adj_close_price DECIMAL(18, 6),
    volume BIGINT,
    UNIQUE(asset_id, price_date)
);

CREATE INDEX idx_prices_date ON daily_prices(price_date);
CREATE INDEX idx_assets_ticker ON assets(ticker);
