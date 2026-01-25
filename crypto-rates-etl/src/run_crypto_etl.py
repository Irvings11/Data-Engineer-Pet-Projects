import os
import time
import json
import logging
from datetime import datetime, timezone
import requests
import pandas as pd
from sqlalchemy import text
from src.db_etl import get_engine


def setup_logger() -> logging.Logger:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    return logging.getLogger("crypto-etl")


def floor_snapshot_time() -> datetime:
    now = datetime.now(timezone.utc)
    return now.replace(second=0, microsecond=0)


def fetch_market_data(logger: logging.Logger) -> list[dict]:
    base_url = os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")
    vs_currency = os.getenv("VS_CURRENCY", "usd")
    top_n = int(os.getenv("TOP_N", "10"))

    timeout_sec = int(os.getenv("REQUEST_TIMEOUT_SEC", "15"))
    retry_count = int(os.getenv("RETRY_COUNT", "5"))
    backoff_base = int(os.getenv("RETRY_BACKOFF_SEC", "2"))

    url = f"{base_url}/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": top_n,
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "24h",
    }

    last_err: Exception | None = None

    for attempt in range(1, retry_count + 1):
        try:
            logger.info("Sending request to %s [%d/%d]", url, attempt, retry_count)
            r = requests.get(url, params=params, timeout=timeout_sec)

            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"Temporary API error: HTTP {r.status_code} | {r.text[:120]}")

            r.raise_for_status()
            data = r.json()
            if not isinstance(data, list):
                raise ValueError("Market data response is not a list")

            logger.info("Extract: received %d market records", len(data))
            return data

        except Exception as e:
            last_err = e
            sleep_sec = backoff_base ** attempt
            logger.warning("Request failed (%r). Retrying in %d seconds", e, sleep_sec)
            time.sleep(sleep_sec)

    raise RuntimeError(f"API request failed after multiple attempts: {last_err!r}")


def load_to_staging(logger: logging.Logger, snapshot_ts: datetime, rows: list[dict]) -> None:
    logger.info("Saving %d records to staging table", len(rows))
    engine = get_engine()

    payload = []
    for item in rows:
        payload.append({
            "snapshot_ts": snapshot_ts,
            "coin_id": item.get("id"),
            "symbol": item.get("symbol"),
            "name": item.get("name"),
            "raw": json.dumps(item, ensure_ascii=False),
        })

    upsert_sql = text("""
        INSERT INTO stg_currency_rates (snapshot_ts, coin_id, symbol, name, raw)
        VALUES (:snapshot_ts, :coin_id, :symbol, :name, CAST(:raw AS jsonb))
        ON CONFLICT (coin_id, snapshot_ts)
        DO UPDATE SET
          symbol = EXCLUDED.symbol,
          name = EXCLUDED.name,
          raw = EXCLUDED.raw;
    """)

    with engine.begin() as conn:
        conn.execute(upsert_sql, payload)

    logger.info("Staging table successfully updated")


def transform_and_load_fct(logger: logging.Logger, snapshot_ts: datetime) -> int:
    engine = get_engine()
    logger.info("Transforming staging data for snapshot %s", snapshot_ts.isoformat())

    with engine.begin() as conn:
        stg_rows = conn.execute(
            text("""
                SELECT coin_id, symbol, name, raw
                FROM stg_currency_rates
                WHERE snapshot_ts = :snapshot_ts
            """),
            {"snapshot_ts": snapshot_ts},
        ).mappings().all()

    if not stg_rows:
        logger.warning("No staging data found for current snapshot")
        return 0

    records = []
    for r in stg_rows:
        raw = r["raw"]
        if isinstance(raw, str):
            raw = json.loads(raw)

        records.append({
            "snapshot_ts": snapshot_ts,
            "coin_id": r["coin_id"],
            "symbol": (r["symbol"] or "").lower(),
            "name": r["name"] or "",

            "price": raw.get("current_price"),
            "market_cap": raw.get("market_cap"),
            "total_volume": raw.get("total_volume"),
            "high_24h": raw.get("high_24h"),
            "low_24h": raw.get("low_24h"),
            "price_change_24h": raw.get("price_change_24h"),
            "price_change_pct_24h": raw.get("price_change_percentage_24h"),
        })

    df = pd.DataFrame(records)

    num_cols = [
        "price", "market_cap", "total_volume",
        "high_24h", "low_24h",
        "price_change_24h", "price_change_pct_24h"
    ]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df[df["price"].notna() & (df["price"] > 0)]
    df = df[df["coin_id"].notna() & (df["coin_id"].astype(str).str.len() > 0)]
    df = df[df["symbol"].notna() & (df["symbol"].astype(str).str.len() > 0)]
    df = df[df["name"].notna() & (df["name"].astype(str).str.len() > 0)]

    if df.empty:
        logger.warning("All records were excluded during validation step")
        return 0

    payload = df.to_dict(orient="records")

    logger.info("Writing %d records to fact table", len(payload))

    upsert_sql = text("""
        INSERT INTO fct_currency_rates (
          snapshot_ts, coin_id, symbol, name,
          price, market_cap, total_volume,
          high_24h, low_24h, price_change_24h, price_change_pct_24h,
          source
        )
        VALUES (
          :snapshot_ts, :coin_id, :symbol, :name,
          :price, :market_cap, :total_volume,
          :high_24h, :low_24h, :price_change_24h, :price_change_pct_24h,
          'coingecko'
        )
        ON CONFLICT (coin_id, snapshot_ts)
        DO UPDATE SET
          symbol = EXCLUDED.symbol,
          name = EXCLUDED.name,
          price = EXCLUDED.price,
          market_cap = EXCLUDED.market_cap,
          total_volume = EXCLUDED.total_volume,
          high_24h = EXCLUDED.high_24h,
          low_24h = EXCLUDED.low_24h,
          price_change_24h = EXCLUDED.price_change_24h,
          price_change_pct_24h = EXCLUDED.price_change_pct_24h;
    """)

    with engine.begin() as conn:
        conn.execute(upsert_sql, payload)

    logger.info("Fact table successfully updated")
    return len(payload)


def main() -> None:
    logger = setup_logger()
    logger.info("Starting ETL pipeline crypto process")

    snapshot_ts = floor_snapshot_time()
    logger.info("snapshot_ts=%s", snapshot_ts.isoformat())

    data = fetch_market_data(logger)
    load_to_staging(logger, snapshot_ts, data)
    cnt = transform_and_load_fct(logger, snapshot_ts)

    logger.info("ETL crypto pipeline finished successfully. fct_upserted=%d", cnt)


if __name__ == "__main__":
    main()
