import numpy as np
import pandas as pd
import psycopg
import io
from datetime import datetime, timedelta

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


def transform_date(date_str):
    date_obj = datetime.strptime(date_str, "%m/%d/%Y %I:%M:%S %p")

    # Shift to '1/04/2025 12:00:00'
    new_date = date_obj + timedelta(days=3220)
    return new_date


def create_synthetic_steps(userId: int):
    df = pd.read_csv("./fitabase/data/hourlySteps_merged.csv")
    df["pyTime"] = pd.to_datetime(df["ActivityHour"], format="%m/%d/%Y %I:%M:%S %p")

    user_df = df[df["Id"] == userId]

    starting_date = pd.to_datetime("2025-02-05")
    date_range = pd.date_range(start=starting_date, periods=24 * num_days, freq="h")

    df_synthetic = pd.DataFrame(date_range, columns=["datetime"])
    df_synthetic["reading"] = 0

    synth_by_hour = {i: [] for i in range(24)}

    for hour in range(0, 24):
        df_hour = user_df[user_df["pyTime"].dt.hour == hour]

        synthetic_data = np.random.normal(
            df_hour["StepTotal"].mean(), df_hour["StepTotal"].std(), num_days
        )
        synthetic_data = np.nan_to_num(synthetic_data)
        synthetic_data = np.clip(synthetic_data, a_min=0, a_max=None)
        synthetic_data = synthetic_data.astype(int)

        synth_by_hour[hour] = synthetic_data

    for date in date_range:
        no_day = (date - starting_date).days
        df_synthetic.loc[df_synthetic["datetime"] == date, "reading"] = synth_by_hour[
            date.hour
        ][no_day]

    df_synthetic["subject_id"] = userId
    return df_synthetic


def create_synthetic_sleep(userId: int):
    df = pd.read_csv("./fitabase/data/minuteSleep_merged.csv")
    df["pyTime"] = pd.to_datetime(df["date"], format="%m/%d/%Y %I:%M:%S %p")

    user_df = df[df["Id"] == userId]

    user_df = (
        user_df.groupby(pd.Grouper(key="pyTime", freq="h"))
        .agg(
            {
                "Id": "first",
                "date": "first",
                "value": "sum",
                "logId": "first",
            }
        )
        .reset_index()
    )

    user_df["Id"] = user_df["Id"].fillna(0).astype(int)
    user_df["logId"] = user_df["logId"].fillna(0).astype(int)

    starting_date = pd.to_datetime("2025-02-05")
    date_range = pd.date_range(start=starting_date, periods=24 * num_days, freq="h")

    df_synthetic = pd.DataFrame(date_range, columns=["datetime"])
    df_synthetic["reading"] = 0

    synth_by_hour = {i: [] for i in range(24)}

    for hour in range(0, 24):
        df_hour = user_df[user_df["pyTime"].dt.hour == hour]

        synthetic_data = np.random.normal(
            df_hour["value"].mean(), df_hour["value"].std(), num_days
        )
        synthetic_data = np.nan_to_num(synthetic_data)
        synthetic_data = np.clip(synthetic_data, a_min=0, a_max=None)
        synthetic_data = synthetic_data.astype(int)

        synth_by_hour[hour] = synthetic_data

    for date in date_range:
        no_day = (date - starting_date).days
        df_synthetic.loc[df_synthetic["datetime"] == date, "reading"] = synth_by_hour[
            date.hour
        ][no_day]

    df_synthetic["subject_id"] = userId
    return df_synthetic


#
# Raw step data
#
df_raw_steps = pd.read_csv("./fitabase/data/hourlySteps_merged.csv")
df_raw_steps = df_raw_steps.rename(
    columns={"Id": "subject_id", "ActivityHour": "datetime", "StepTotal": "reading"}
)
df_raw_steps["data_type"] = 2
df_raw_steps["datetime"] = df_raw_steps["datetime"].map(transform_date)

df_raw_steps = df_raw_steps[df_raw_steps["subject_id"].isin(subject_ids)]

#
# Synthetic step data
#
syn_steps_frames = [create_synthetic_steps(subject) for subject in subject_ids]
df_syn_steps = pd.concat(syn_steps_frames, axis=0, ignore_index=True)
df_syn_steps["data_type"] = 2
df_syn_steps = df_syn_steps[["subject_id", "datetime", "reading", "data_type"]]

#
# Raw heartrate data
#
df_raw_hr = pd.read_csv("./fitabase/data/heartrate_seconds_merged.csv")
df_raw_hr = df_raw_hr.rename(
    columns={"Id": "subject_id", "Time": "datetime", "Value": "reading"}
)
df_raw_hr["data_type"] = 3
df_raw_hr["datetime"] = df_raw_hr["datetime"].map(transform_date)

#
# Raw sleep data
#
df_raw_sleep = pd.read_csv("./fitabase/data/minuteSleep_merged.csv")
df_raw_sleep = df_raw_sleep.drop("logId", axis=1)

df_raw_sleep = df_raw_sleep.rename(
    columns={"Id": "subject_id", "date": "datetime", "value": "reading"}
)
df_raw_sleep["data_type"] = 4
df_raw_sleep["datetime"] = df_raw_sleep["datetime"].map(transform_date)

df_raw_sleep = df_raw_sleep[df_raw_sleep["subject_id"].isin(subject_ids)]

#
# Synthetic sleep data
#
syn_sleep_frames = [create_synthetic_sleep(subject) for subject in subject_ids]
df_syn_sleep = pd.concat(syn_sleep_frames, axis=0, ignore_index=True)
df_syn_sleep["data_type"] = 4
df_syn_sleep = df_syn_sleep[["subject_id", "datetime", "reading", "data_type"]]


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

            buffer_raw_steps = io.StringIO()
            df_raw_steps.to_csv(buffer_raw_steps, index=False, header=False)
            buffer_raw_steps.seek(0)

            buffer_syn_steps = io.StringIO()
            df_syn_steps.to_csv(buffer_syn_steps, index=False, header=False)
            buffer_syn_steps.seek(0)

            buffer_raw_hr = io.StringIO()
            df_raw_hr.to_csv(buffer_raw_hr, index=False, header=False)
            buffer_raw_hr.seek(0)

            buffer_raw_sleep = io.StringIO()
            df_raw_sleep.to_csv(buffer_raw_sleep, index=False, header=False)
            buffer_raw_sleep.seek(0)

            buffer_syn_sleep = io.StringIO()
            df_syn_sleep.to_csv(buffer_syn_sleep, index=False, header=False)
            buffer_syn_sleep.seek(0)

            with cursor.copy(
                "COPY readings (subject_id, datetime, reading, data_type) FROM STDIN WITH CSV"
            ) as copy:
                while data := buffer_raw_steps.read(512):
                    copy.write(data)

                while data := buffer_syn_steps.read(512):
                    copy.write(data)

                while data := buffer_raw_hr.read(512):
                    copy.write(data)

                while data := buffer_raw_sleep.read(512):
                    copy.write(data)

                while data := buffer_syn_sleep.read(512):
                    copy.write(data)

except Exception as e:
    print(f"An error occurred: {e}")
