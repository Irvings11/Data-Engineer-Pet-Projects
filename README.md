# Superset + PostgreSQL + ClickHouse (Docker)

Учебный data engineering проект с визуализацией данных в Apache Superset.

## Стек
- Apache Superset
- PostgreSQL (2 базы)
- ClickHouse
- Docker / Docker Network / Volumes
- Python (psycopg2)

## Архитектура
- postgres_1 — metadata / test_app
- postgres_2 — application DB с тестовыми данными
- clickhouse — аналитическое хранилище
- superset — BI слой

Все сервисы работают в одной docker-сети `app_net`.

## Запуск

```bash
docker build -t superset-fixed -f docker/Dockerfile.superset .
bash docker/command.sh
```
# Superset будет доступен:
```bash
http://localhost
```
# Инициализация Superset
```bash
docker exec -it superset superset fab create-admin
docker exec -it superset superset db upgrade
docker exec -it superset superset init
```
# Наполнение PostgreSQL тестовыми данными
```bash
python scripts/pg_sql.py
```

# Подключение БД в Superset
- PostgreSQL:
```bash
postgresql+psycopg2://postgres_someuser:postgres_somepassword@postgres_2:5432/app_db
```
# ClickHouse:
```bash
clickhouse+native://default:@clickhouse:9000/default
```