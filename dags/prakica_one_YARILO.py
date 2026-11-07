import ast
import codecs
import csv
from datetime import datetime
from io import BytesIO

import boto3
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from botocore.client import Config
from psycopg2.extras import execute_values
from psycopg2.sql import SQL, Identifier

RAW_TABLE = 'Yarilo_table'
AGG_TABLE = 'Yarilo_agg_table'

DEFAULT_ARGS = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2024, 11, 12),
}

API_URL = "https://b2b.itresume.ru/api/statistics"

RAW_COLUMNS = (
    'lti_user_id', 'is_correct', 'attempt_type', 'created_at',
    'oauth_consumer_key', 'lis_result_sourcedid', 'lis_outcome_service_url',
)


def load_from_api(**context):
    start = context['data_interval_start']
    end = context['data_interval_end']

    INSERT_RAW = SQL('INSERT INTO {} ({}) VALUES %s').format(
        Identifier(RAW_TABLE), SQL(', '.join(RAW_COLUMNS)),
    )
    DELETE_RAW = SQL(
        'DELETE FROM {} WHERE created_at >= %s AND created_at < %s'
    ).format(Identifier(RAW_TABLE))

    payload = {
        'client': 'Skillfactory',
        'client_key': 'M2MGWS',
        'start': start.to_date_string(),
        'end': end.to_date_string(),
    }
    response = requests.get(API_URL, params=payload, timeout=60)
    response.raise_for_status()
    data = response.json()

    rows = []
    for el in data:
        passback_params = ast.literal_eval(el.get('passback_params') or '{}')
        rows.append((
            el.get('lti_user_id'),
            el.get('is_correct') == 1,
            el.get('attempt_type'),
            el.get('created_at'),
            passback_params.get('oauth_consumer_key'),
            passback_params.get('lis_result_sourcedid'),
            passback_params.get('lis_outcome_service_url'),
        ))

    hook = PostgresHook(postgres_conn_id='conn_pg')
    conn = hook.get_conn()
    try:
        with conn:  # транзакция
            with conn.cursor() as cur:
                cur.execute(DELETE_RAW, (start, end))
                if rows:
                    execute_values(
                        cur, INSERT_RAW.as_string(conn), rows, page_size=1000
                    )
    finally:
        conn.close()


def agg_data(**context):
    start = context['data_interval_start']
    end = context['data_interval_end']

    delete_sql = SQL(
        'DELETE FROM {} WHERE date >= %(start)s AND date < %(end)s'
    ).format(Identifier(AGG_TABLE))

    insert_sql = SQL("""
        INSERT INTO {agg} (
            lti_user_id, attempt_type, attempt_count,
            attempt_failed_count, date
        )
        SELECT lti_user_id,
               attempt_type,
               COUNT(*),
               COUNT(*) FILTER (WHERE is_correct IS NOT TRUE),
               %(start)s::date
          FROM {raw}
         WHERE created_at >= %(start)s
           AND created_at <  %(end)s
         GROUP BY lti_user_id, attempt_type
    """).format(agg=Identifier(AGG_TABLE), raw=Identifier(RAW_TABLE))

    params = {'start': start, 'end': end}

    hook = PostgresHook(postgres_conn_id='conn_pg')
    conn = hook.get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(delete_sql, params)
                cur.execute(insert_sql, params)
    finally:
        conn.close()


def upload_data(table_name, file_name, date_column, **context):
    start = context['data_interval_start']
    end = context['data_interval_end']

    sql_query = SQL(
        'SELECT * FROM {} WHERE {} >= %s AND {} < %s'
    ).format(
        Identifier(table_name),
        Identifier(date_column),
        Identifier(date_column),
    )

    hook = PostgresHook(postgres_conn_id='conn_pg')
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql_query, (start, end))
            data = cur.fetchall()
    finally:
        conn.close()

    file = BytesIO()
    writer = csv.writer(
        codecs.getwriter('utf-8')(file),
        delimiter='\t',
        lineterminator='\n',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL,
    )
    writer.writerows(data)
    file.seek(0)

    connection = BaseHook.get_connection('conn_s3')
    s3_client = boto3.client(
        's3',
        endpoint_url=connection.host,
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        config=Config(signature_version="s3v4"),
    )
    s3_client.put_object(
        Body=file,
        Bucket='default-storage',
        Key=f"{file_name}.csv",
    )


def upload_raw(**context):
    upload_data(RAW_TABLE, f"RAW_{context['ds']}.csv", 'created_at', **context)


def upload_agg(**context):
    upload_data(AGG_TABLE, f"AGG_{context['ds']}.csv", 'date', **context)


with DAG(
        dag_id="iakotov_pz1",
        tags=['4', 'admin'],
        schedule='@daily',
        default_args=DEFAULT_ARGS,
        catchup=False,
        max_active_runs=1,
        max_active_tasks=1,
) as dag:
    start_dag = EmptyOperator(task_id='start')
    end_dag = EmptyOperator(task_id='end')

    load_api = PythonOperator(task_id='load_from_api', python_callable=load_from_api)
    aggregate = PythonOperator(task_id='agg', python_callable=agg_data)
    to_s3_raw = PythonOperator(task_id='upload_raw_to_s3', python_callable=upload_raw)
    to_s3_agg = PythonOperator(task_id='upload_agg_to_s3', python_callable=upload_agg)

    start_dag >> load_api >> aggregate >> [to_s3_raw, to_s3_agg] >> end_dag