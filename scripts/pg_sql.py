import os
import string
from random import choice
from pprint import pprint

import psycopg2


def generate_login(uid):
    name = choice(string.ascii_lowercase)
    surname = "".join(choice(string.ascii_lowercase) for _ in range(5))
    return f"{name}_{uid}_{surname}"


def generate_name():
    return "".join(choice(string.ascii_lowercase) for _ in range(6)).title()


if __name__ == "__main__":
    conn = psycopg2.connect(
    host="localhost",
    port="55433",          # тот порт, что ты пробросил -p 55433:5432
    database="app_db",
    user="postgres_someuser",
    password="postgres_somepassword",
)

    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS app_table (
            uid   INT NOT NULL,
            login TEXT NOT NULL,
            name  TEXT NOT NULL
        );
    """)

    for i in range(1, 101):
        cur.execute(
            "INSERT INTO app_table (uid, login, name) VALUES (%s, %s, %s);",
            (i, generate_login(i), generate_name()),
        )

    cur.execute("SELECT * FROM app_table LIMIT 5;")
    pprint(cur.fetchall())

    conn.commit()
    cur.close()
    conn.close()
