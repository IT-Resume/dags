from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook


def api_comb(**context):
    import psycopg2
    import ast
    import psycopg2
    from requests import get
    import pendulum
    conn = BaseHook.get_connection(conn_id='conn_pg')

    connection = psycopg2.connect(
        dbname='etl',
        user=conn.login,
        password=conn.password,
        host=conn.host,
        port=conn.port
    )
    cur = connection.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS UserSkilF (
            lti_user_id TEXT,
            oauth_consumer_key TEXT,
            lis_result_sourcedid TEXT,
            lis_outcome_service_url TEXT,
            is_correct BOOLEAN,  
            attempt_type TEXT,
            created_at TIMESTAMP
        )
    ''')
    EndDate = pendulum.parse(context['ds']).subtract(days=1).to_date_string()
    StartDate = pendulum.parse(endDate).subtract(days=7).to_date_string()
    URL = 'https://b2b.itresume.ru/api/statistics'
    params = {'client': 'Skillfactory',
              'client_key': 'M2MGWS',
              'start': StartDate,
              'end': EndDate
              }
    result = get(url=URL, params=params)
    data = result.json()
    for el in data:
        insert_data = {
            'lti_user_id': el.get('lti_user_id'),
            'oauth_consumer_key': ast.literal_eval(el.get('passback_params')).get('oauth_consumer_key'),
            'lis_result_sourcedid': ast.literal_eval(el.get('passback_params')).get('lis_result_sourcedid'),
            'lis_outcome_service_url': el.get('passback_params').get('lis_outcome_service_url'),
            'is_correct': el.get('is_correct'),  # Может быть None/True/False
            'attempt_type': el.get('attempt_type'),
            'created_at': el.get('created_at')
        }

        # Выполняем INSERT
        cur.execute('''
                            INSERT INTO UserSkilF (
                                lti_user_id,
                                oauth_consumer_key,
                                lis_result_sourcedid,
                                lis_outcome_service_url,
                                is_correct,
                                attempt_type,
                                created_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ''', (
            insert_data['lti_user_id'],
            insert_data['oauth_consumer_key'],
            insert_data['lis_result_sourcedid'],
            insert_data['lis_outcome_service_url'],
            insert_data['is_correct'],
            insert_data['attempt_type'],
            insert_data['created_at']
        ))
        connection.commit()

DEFAULT_ARGS = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2025, 12, 12),
}
with DAG(
    dag_id="combineApiData",
    tags=['admin', '4'],
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')
    ExtractApi =PythonOperator(task_id='dag_extract',python_callable=api_comb)
dag_start >> ExtractApi >> dag_end
