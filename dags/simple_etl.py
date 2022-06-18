""" module doc string goes here"""
import os
from datetime import datetime, timezone
from typing import OrderedDict, Any
from functools import wraps
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from dotenv import dotenv_values
from sqlalchemy import create_engine, inspect
import sqlalchemy


DATASET_URL:str = 'https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv'


CONFIG:OrderedDict[str,str] = dotenv_values("../.env")
if not CONFIG:
    CONFIG = os.environ

def logger(func):
    """doc string"""
    @wraps(func)
    def inner(*args, **kwargs):
        called_at = datetime.now(timezone.utc)
        print(f">>> Running {func.__name__!r} function. Logged at {called_at}")
        to_execute = func(*args, **kwargs)
        print(f">>> Function: {func.__name__!r} executed. Logged at {called_at}")
        return to_execute

    return inner

@logger
def connect_db() -> sqlalchemy.engine:
    """doc string"""
    print("Connecting to DB")
    connection_uri:str = f"""postgresql+psycopg2://{CONFIG["POSTGRES_USER"]}:{CONFIG["POSTGRES_PASSWORD"]}@{CONFIG["POSTGRES_HOST"]}:{CONFIG["POSTGRES_PORT"]}"""
    engine = create_engine(connection_uri, pool_pre_ping=True)
    engine.connect()
    return engine


@logger
def extract(dataset_url:str) -> pd.DataFrame:
    """doc string"""
    print(f"Reading dataset from {dataset_url}")
    df = pd.read_csv(dataset_url)
    return df


@logger
def transform(df:Any) -> pd.DataFrame:
    """doc string"""
    # transformation
    print("Transforming data")
    df_transform = df.copy()
    winecolor_encoded = pd.get_dummies(df_transform["winecolor"], prefix="winecolor")
    df_transform[winecolor_encoded.columns.to_list()] = winecolor_encoded
    df_transform.drop("winecolor", axis=1, inplace=True)

    for column in df_transform.columns:
        df_transform[column] = (
            df_transform[column] - df_transform[column].mean()
        ) / df_transform[column].std()
    return df


@logger
def check_table_exists(table_name:str, engine:SQLAlchemy.engine) -> None:
    """doc string"""
    if table_name in inspect(engine).get_table_names():
        print(f"{table_name!r} exists in the DB!")
    else:
        print(f"{table_name} does not exist in the DB!")


@logger
def load_to_db(df:pd.DataFrame, table_name:str, engine:SQLAlchemy.engine) -> str:
    """doc string"""
    print(f"Loading dataframe to DB on table: {table_name}")
    df.to_sql(table_name, engine, if_exists="replace")


@logger
def etl()-> None:
    """doc string"""
    db_engine = connect_db()

    raw_df = extract(DATASET_URL)
    raw_table_name = "raw_wine_quality_dataset"

    clean_df = transform(raw_df)
    clean_table_name = "clean_wine_quality_dataset"

    load_to_db(raw_df, raw_table_name, db_engine)
    load_to_db(clean_df, clean_table_name, db_engine)


@logger
def tables_exists()->None:
    """doc string"""
    db_engine = connect_db()
    print("Checking if tables exists")
    check_table_exists("raw_wine_quality_dataset", db_engine)
    check_table_exists("clean_wine_quality_dataset", db_engine)

    db_engine.dispose()



default_args = {
    "owner": "Airflow",
    'depends_on_past': False,
    "start_date": days_ago(1)
}

with DAG(
    dag_id="simple_etl_dag",
    default_args=default_args,
    description='a simple etl dag for testing purpose',
    schedule_interval=None,
    catchup=False
) as dag:
    run_etl_task = PythonOperator(
        task_id="run_etl_task",
        python_callable=etl
    )
    run_tables_exists_task = PythonOperator(
        task_id="run_tables_exists_task",
        python_callable=tables_exists
    )
    run_etl_task >> run_tables_exists_task
