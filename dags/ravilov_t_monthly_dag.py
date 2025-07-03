from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from calendar import monthrange

from datetime import datetime, timedelta

DEFAULT_ARGS = {
    'owner': 'Timur',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2025,6,20)
}

API_URL = 'https://b2b.itresume.ru/api/statistics'


class WeekTemplates:
    @staticmethod
    def current_week_start(date) -> str:
        logical_dt = datetime.strptime(date, "%Y-%m-%d")

        current_week_start = logical_dt - timedelta(days=logical_dt.weekday())

        return current_week_start.strftime("%Y-%m-%d")

    @staticmethod
    def current_week_end(date) -> str:
        logical_dt = datetime.strptime(date, "%Y-%m-%d")

        current_week_end = logical_dt + timedelta(days=6 - logical_dt.weekday())

        return current_week_end.strftime("%Y-%m-%d")

class MonthTemplates:  

    @staticmethod
    def current_month_start(date_str):
        """Возвращает первый день месяца для заданной даты."""
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        first_day = dt.replace(day=1)
        return first_day.strftime("%Y-%m-%d")

    @staticmethod
    def current_month_end(date_str):
        """Возвращает последний день месяца для заданной даты."""
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        _, last_day_num = monthrange(dt.year, dt.month)
        last_day = dt.replace(day=last_day_num)
        return last_day.strftime("%Y-%m-%d")

def load_from_api(month_start: str, month_end: str, **context):
    import requests
    import pendulum
    import psycopg2 as pg 
    import ast 
    import json

    # определяем подключения и парсинг данных
    payload = {
        'client': 'Skillfactory',
        'client_key': 'M2MGWS',
        'start': month_start,
        'end': month_end,
    }

    response = requests.get(API_URL, params=payload)
    data = response.json()

    # инициализация хука для подключения
    connection = BaseHook.get_connection('conn_pg')

    with pg.connect(
        dbname='etl',
        sslmode = 'disable',
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port,
        connect_timeout=600,
        keepalives_idle=600,
        tcp_user_timeout=600
    ) as conn:
        cursor = conn.cursor()

        for el in data:
            row = []
            passback_params = ast.literal_eval(el.get('passback_params','{}'))
            row.append(el.get('lti_user_id'))
            row.append(True if el.get('is_correct') == 1 else False)
            row.append(el.get('attempt_type'))
            row.append(el.get('created_at'))
            row.append(passback_params.get('oauth_consumer_key'))
            row.append(passback_params.get('lis_result_sourcedid'))
            row.append(passback_params.get('lis_outcome_service_url'))

            cursor.execute("INSERT INTO ravilov_t_month_admin_table VALUES(%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", row)

        cursor.execute("INSERT INTO ravilov_t_raw_month_admin_table(raw_data) VALUES(%s) ON CONFLICT DO NOTHING", (json.dumps(data),))

        conn.commit()

def upload_data(month_start: str, month_end: str, **context):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs
    import pandas as pd

    sql_query = f"""
        SELECT * FROM ravilov_t_month_admin_table
        WHERE date >= '{month_start}'::timestamp 
              AND date < '{month_end}'::timestamp + INTERVAL '1 days';
    """

    connection = BaseHook.get_connection('conn_pg')

    with pg.connect(
        dbname='postgres',
        sslmode='disable',
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port,
        connect_timeout=600,
        keepalives_idle=600,
        tcp_user_timeout=600
    ) as conn:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        data = cursor.fetchall()

        file = BytesIO()

        writer_wrapper = codecs.getwriter('utf-8')

        writer = csv.writer(
            writer_wrapper(file),
            delimiter='\t',
            lineterminator='\n',
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL
        )   

        writer.writerows(data)
        file.seek(0)

        connection = BaseHook.get_connection('conn_s3')

        s3_client = s3.client(
            's3',
            endpoint_url=connection.host,
            aws_access_key_id=connection.login,
            aws_secret_access_key=connection.password,
            config=Config(signature_version="s3v4"),
        )

        s3_client.put_object(
            Body=file,
            Bucket='default-storage',
            Key=f"admin_{month_start}_{context['ds']}.csv"
        )

def combine_data(month_start: str, month_end: str, **context):
    import psycopg2 as pg

    sql_query = f"""
        INSERT INTO ravilov_t_month_admin_table
        SELECT lti_user_id,
               attempt_type,
               COUNT(1),
               COUNT(CASE WHEN is_correct THEN NULL ELSE 1 END) AS attempt_failed_count,
               '{month_start}'::timestamp
          FROM admin_table
         WHERE created_at >= '{month_start}'::timestamp 
               AND created_at < '{month_end}'::timestamp + INTERVAL '1 days'
          GROUP BY lti_user_id, attempt_type;
    """

    connection = BaseHook.get_connection('conn_pg')

    with pg.connect(
        dbname='etl',
        sslmode='disable',
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port,
        connect_timeout=600,
        keepalives_idle=600,
        tcp_user_timeout=600
    ) as conn:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        conn.commit()


with DAG(
    dag_id="itresume_combine_api_data_weekly",
    tags=['itresume'],
    schedule="@daily",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1,
    user_defined_macros={
        "current_month_start": MonthTemplates.current_month_start,
        "current_month_end": MonthTemplates.current_month_end
    },
    render_template_as_native_obj=True
) as dag:
    
    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    load_from_api = PythonOperator(
        task_id='load_from_api',
        python_callable=load_from_api,
        op_kwargs={
            'month_start': '{{ current_month_start(ds) }}',
            'month_end': '{{ current_month_end(ds) }}',
        }
    )

    combine_data = PythonOperator(
        task_id='combine_data',
        python_callable=combine_data,
        op_kwargs={
            'month_start': '{{ current_month_start(ds) }}',
            'month_end': '{{ current_month_end(ds) }}',
        }
    )

    upload_data = PythonOperator(
        task_id='upload_data',
        python_callable=upload_data,
        op_kwargs={
            'month_start': '{{ current_month_start(ds) }}',
            'month_end': '{{ current_month_end(ds) }}',
        }
    )
    
    dag_start >> load_from_api >> combine_data >> upload_data >> dag_end