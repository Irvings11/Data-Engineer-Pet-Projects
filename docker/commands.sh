#!/bin/bash

# volumes
docker volume create postgres_vol_1 || true
docker volume create postgres_vol_2 || true
docker volume create clickhouse_vol || true
docker volume create superset_home || true


# network
docker network create app_net 2>/dev/null || true

#Postgres_1
docker run  -d \
    --name postgres_1 \
    -e POSTGRES_PASSWORD=postgres_admin \
    -e POSTGRES_USER=postgres_admin \
    -e POSTGRES_DB=test_app \
    -v postgres_vol_1:/var/lib/postgresql/data \
    --network app_net \
    postgres:15



#Clickhouse
docker run  -d \
  --name clickhouse \
  --network app_net \
  -v clickhouse_vol:/var/lib/clickhouse \
  yandex/clickhouse-server


#Superset
docker run -d  \
  --name superset \
  --network app_net \
  -p 80:8088 \
  -e SUPERSET_SECRET_KEY=MY_FIXED_SUPERSET_SECRET_KEY_1234567890 \
  -v superset_home:/app/superset_home \
  superset-fixed


#Postgres_2
docker run  -d \
    --name postgres_2 \
    -e POSTGRES_PASSWORD=postgres_somepassword \
    -e POSTGRES_USER=postgres_someuser \
    -e POSTGRES_DB=app_db \
    -v postgres_vol_2:/var/lib/postgresql/data \
    --network app_net \
    -p 55433:5432 \
    postgres:15

