import numpy as np
import pandas as pd
import psycopg
import io
from datetime import datetime, timedelta
import glob


db_params = {
    "dbname": "examples",
    "user": "examples",
    "password": "examples",
    "host": "172.23.208.124",
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
