import numpy as np
import pandas as pd
import psycopg
import io
from datetime import datetime, timedelta
import glob

num_days = 30
subject_ids = [
    2022484408,
    2026352035,
    2347167796,
    4020332650,
    4558609924,
    5553957443,
    5577150313,
    6117666160,
    6391747486,
    6775888955,
    6962181067,
    7007744171,
    8792009665,
    8877689391,
]


db_params = {
    "dbname": "examples",
    "user": "examples",
    "password": "examples",
    "host": "db",
    "port": 5432,
}

try:
    with psycopg.connect(**db_params) as conn:
        with conn.cursor() as cursor:
            conn.autocommit = True

            cursor.execute("DROP TABLE IF EXISTS readings")

            create_table_query = """
            CREATE TABLE IF NOT EXISTS readings (
                id SERIAL PRIMARY KEY,
                subject_id BIGINT,
                data_type INT,
                datetime TIMESTAMP,
                reading DOUBLE PRECISION
            );
            """

            cursor.execute(create_table_query)

            for csvfile in glob.glob("csv/*.csv"):
                with open(csvfile, "r") as f:
                    with cursor.copy(
                        "COPY readings (subject_id, datetime, reading, data_type) FROM STDIN WITH (FORMAT CSV)"
                    ) as copy:
                        while data := f.read(512):
                            copy.write(data)

except Exception as e:
    print(f"An error occurred: {e}")
