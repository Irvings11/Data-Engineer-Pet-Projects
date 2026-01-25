import os
from sqlalchemy import create_engine


def make_pg_url() -> str:
    host = os.getenv("POSTGRES_HOST", "db")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "crypto")
    user = os.getenv("POSTGRES_USER", "crypto_user")
    pwd = os.getenv("POSTGRES_PASSWORD", "crypto_pass")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


def get_engine():
    return create_engine(make_pg_url(), pool_pre_ping=True)
