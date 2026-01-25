CREATE TABLE IF NOT EXISTS stg_currency_rates (
  snapshot_ts      timestamptz NOT NULL,
  coin_id          text        NOT NULL,
  symbol           text,
  name             text,
  raw              jsonb       NOT NULL,
  inserted_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (coin_id, snapshot_ts)
);

CREATE TABLE IF NOT EXISTS fct_currency_rates (
  snapshot_ts      timestamptz NOT NULL,
  coin_id          text        NOT NULL,
  symbol           text        NOT NULL,
  name             text        NOT NULL,

  price            numeric(20,8) NOT NULL,
  market_cap       numeric(28,2),
  total_volume     numeric(28,2),

  high_24h         numeric(20,8),
  low_24h          numeric(20,8),
  price_change_24h numeric(20,8),
  price_change_pct_24h numeric(10,4),

  source           text NOT NULL DEFAULT 'coingecko',
  inserted_at      timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (coin_id, snapshot_ts)
);

CREATE INDEX IF NOT EXISTS idx_fct_rates_ts ON fct_currency_rates (snapshot_ts);

CREATE OR REPLACE VIEW v_daily_crypto_stats AS
WITH last24 AS (
  SELECT *
  FROM fct_currency_rates
  WHERE snapshot_ts >= now() - interval '24 hours'
),
w AS (
  SELECT
    coin_id,
    symbol,
    name,
    snapshot_ts,
    price,

    AVG(price) OVER (PARTITION BY coin_id) AS avg_price_24h,
    MIN(price) OVER (PARTITION BY coin_id) AS min_price_24h,
    MAX(price) OVER (PARTITION BY coin_id) AS max_price_24h,

    FIRST_VALUE(price) OVER (PARTITION BY coin_id ORDER BY snapshot_ts DESC) AS current_price,
    FIRST_VALUE(snapshot_ts) OVER (PARTITION BY coin_id ORDER BY snapshot_ts DESC) AS current_ts
  FROM last24
)
SELECT DISTINCT ON (coin_id)
  coin_id,
  symbol,
  name,
  current_ts,
  current_price,
  avg_price_24h,
  min_price_24h,
  max_price_24h,
  CASE
    WHEN avg_price_24h = 0 THEN NULL
    ELSE ROUND(((current_price - avg_price_24h) / avg_price_24h) * 100, 4)
  END AS deviation_pct_from_avg_24h
FROM w
ORDER BY coin_id, current_ts DESC;



