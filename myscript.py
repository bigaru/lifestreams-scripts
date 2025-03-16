import numpy as np
import pandas as pd
import psycopg
import io
from datetime import datetime, timedelta


def transform_date(date_str):
    date_obj = datetime.strptime(date_str, "%m/%d/%Y %I:%M:%S %p")

    # Shift to '1/04/2025 12:00:00'
    new_date = date_obj + timedelta(days=3222)
    return new_date.timestamp()


# mean = 0  # Mean (μ)
# std_dev = 1  # Standard deviation (σ)
# sample_size = 10000

# data = np.random.normal(loc=mean, scale=std_dev, size=sample_size)

# time = pd.date_range(start="2025-01-01", periods=sample_size, freq="D")
# time = time.strftime("%Y-%m-%d %H:%M:%S")

# df = pd.DataFrame({"subject_id": 1, "data_type": 1, "datetime": time, "reading": data})
# print(df.head(10))

df_steps = pd.read_csv("./fitabase/data/hourlySteps_merged.csv")
df_steps = df_steps.rename(
    columns={"Id": "subject_id", "ActivityHour": "datetime", "StepTotal": "reading"}
)
df_steps["data_type"] = 2
df_steps["datetime"] = df_steps["datetime"].map(transform_date)

print(df_steps.head(10))

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
                datetime DOUBLE PRECISION,
                reading DOUBLE PRECISION
            );
            """

            cursor.execute(create_table_query)

            buffer = io.StringIO()
            df_steps.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            with cursor.copy(
                "COPY readings (subject_id, datetime, reading, data_type) FROM STDIN WITH CSV"
            ) as copy:
                while data := buffer.read(512):
                    copy.write(data)

except Exception as e:
    print(f"An error occurred: {e}")
