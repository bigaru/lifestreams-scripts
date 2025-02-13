import numpy as np
import pandas as pd
import psycopg2
import io


mean = 0  # Mean (μ)
std_dev = 1  # Standard deviation (σ)
sample_size = 10000

data = np.random.normal(loc=mean, scale=std_dev, size=sample_size)

time = pd.date_range(start="2025-01-01", periods=sample_size, freq="D")
time = time.strftime("%Y-%m-%d %H:%M:%S")

df = pd.DataFrame({"subject_id": 1, "data_type": 1, "datetime": time, "reading": data})

print(df.head(10))


db_params = {
    "dbname": "examples",
    "user": "examples",
    "password": "examples",
    "host": "db",
    "port": 5432,
}

cursor = None
conn = None

try:
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    conn.autocommit = True

    create_table_query = """
    CREATE TABLE IF NOT EXISTS readings (
        id SERIAL PRIMARY KEY,
        subject_id INT,
        data_type INT,
        datetime TIMESTAMP,
        reading DOUBLE PRECISION
    );
    """

    cursor.execute(create_table_query)

    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    cursor.copy_from(
        file=buffer,
        table="readings",
        columns=("subject_id", "data_type", "datetime", "reading"),
        sep=",",
        null="",
    )


except Exception as e:
    print(f"An error occurred: {e}")

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
