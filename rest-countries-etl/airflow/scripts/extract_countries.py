import requests
import psycopg2

def extract_countries():
    url = "https://restcountries.com/v3.1/all"
    params = {
        "fields": "name,cca2,cca3,region,subregion,population"
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    conn = psycopg2.connect(
        host="postgres",
        database="etl",
        user="postgres",
        password="postgres"
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS countries (
            name TEXT,
            cca2 TEXT,
            cca3 TEXT,
            region TEXT,
            subregion TEXT,
            population BIGINT
        )
    """)

    for c in data:
        cur.execute(
            """
            INSERT INTO countries (name, cca2, cca3, region, subregion, population)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                c["name"]["common"],
                c.get("cca2"),
                c.get("cca3"),
                c.get("region"),
                c.get("subregion"),
                c.get("population")
            )
        )

    conn.commit()
    cur.close()
    conn.close()
