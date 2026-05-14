CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS results;

SET search_path TO raw;

DROP TABLE IF EXISTS assets;

CREATE TABLE assets (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) UNIQUE NOT NULL,
    name VARCHAR(255),
    asset_type VARCHAR(50) DEFAULT 'stock',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_assets_ticker ON assets(ticker);
