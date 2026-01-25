# Crypto Rates ETL (CoinGecko -> PostgreSQL)

## Что делает проект
1. Забирает топ-10 криптовалют по капитализации (BTC, ETH, SOL и т.д)
2. Сохраняет сырые данные в staging-таблицу
3. Преобразует и очищает данные 
4. Загружает очищенные данные в fact-таблицу
5. Строит витрину с агрегатами за последние 24 часа

- ETL запускается один раз при старте контейнера
- Если порт занят — поменяйте левую часть
- Port: 55432:5432



## Запуск проекта

1. Создать файл с переменными окружения:
```bash
cp .env.example .env
```
2. Запустить проект:
```bash
docker compose up --build
```
## Схема данных
### stg_currency_rates
Staging-таблица для сырых данных из API.
- `snapshot_ts timestamptz` - время снимка данных
- `coin_id text` - идентификатор криптовалюты
- `symbol text`
- `name text`
- `raw jsonb` - полный json-ответ API  
- **Primary Key**: `(coin_id, snapshot_ts)`

### fct_currency_rates
Fact-таблица с очищенными и типизированными данными.
- `snapshot_ts timestamptz`
- `coin_id text`, `symbol text`, `name text`
- `price numeric(20,8)` (записи с `price <= 0` отбрасываются)
- `market_cap numeric`, `total_volume numeric`
- `high_24h numeric`, `low_24h numeric`
- `price_change_24h numeric`, `price_change_pct_24h numeric`
- **Primary Key**: `(coin_id, snapshot_ts)`
- Идемпотентность реализована через  
  `ON CONFLICT (coin_id, snapshot_ts) DO UPDATE`

### v_daily_crypto_stats
SQL витрина с агрегатами за последние 24 часа (оконные функции).
- средняя, минимальная и максимальная цена
- текущее отклонение цены от среднего значения (в %)

## Проверка
Просмотр логов ETL:
```bash
docker logs --tail 100 crypto_etl
```







