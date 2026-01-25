FROM apache/superset:latest

USER root

# 1) Включаем pip в venv (если его нет)
RUN /app/.venv/bin/python -m ensurepip --upgrade || true \
 && /app/.venv/bin/python -m pip install --no-cache-dir --upgrade pip setuptools wheel

# 2) Ставим драйверы (совместимо с SQLAlchemy 1.4)
RUN /app/.venv/bin/python -m pip install --no-cache-dir \
    psycopg2-binary \
    "sqlalchemy<2.0" \
    clickhouse-driver \
    clickhouse-sqlalchemy==0.2.4

USER superset
